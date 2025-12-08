"""
Transpile SQL queries from PostgreSQL to Databricks Spark SQL using sqlglot.
"""

import sqlglot
from sqlglot import exp
from pathlib import Path


def _create_datediff(left: exp.Expression, right: exp.Expression) -> exp.Expression:
    """Create a DATEDIFF(left, right) expression."""
    return exp.Anonymous(
        this="DATEDIFF",
        expressions=[left, right]
    )


def _unwrap_timestamp_cast(expr: exp.Expression) -> exp.Expression:
    """Remove CAST(... AS TIMESTAMP) wrapper if present, returning the inner expression."""
    if isinstance(expr, exp.Cast):
        to_type = expr.to
        if isinstance(to_type, exp.DataType) and to_type.this == exp.DataType.Type.TIMESTAMP:
            return expr.this
    return expr


def _is_year_extract(expr: exp.Expression) -> bool:
    """
    Check if expression is EXTRACT(YEAR FROM ...) which returns an integer, not a date.
    """
    if isinstance(expr, exp.Extract):
        # Check if extracting YEAR (returns integer)
        if str(expr.this).upper() == "YEAR":
            return True
    return False


def _extract_date_operands_from_sub(sub_expr: exp.Expression):
    """
    Extract the date operands from a subtraction expression.
    Handles both simple (date - date) and CAST(... AS TIMESTAMP) patterns.
    Also handles function calls like GREATEST() and LEAST().
    Returns (left, right) operands with TIMESTAMP casts removed.

    Returns None if the operands are integers (like EXTRACT(YEAR ...)) rather than dates.
    """
    if isinstance(sub_expr, exp.Sub):
        left = _unwrap_timestamp_cast(sub_expr.left)
        right = _unwrap_timestamp_cast(sub_expr.right)

        # Don't convert if we're subtracting integers (e.g., EXTRACT(YEAR ...) - year_of_birth)
        if _is_year_extract(left) or _is_year_extract(right):
            return None, None

        # Accept any expression (columns, functions, etc.) as valid date operands
        # DATEDIFF can handle GREATEST/LEAST and other date functions
        return left, right
    return None, None


def _is_epoch_days_pattern(node: exp.Expression):
    """
    Check if node matches: CAST(EXTRACT(epoch FROM ...) / 86400 AS BIGINT)
    This is a PostgreSQL pattern for calculating day differences.
    Returns the subtraction expression if found, None otherwise.
    """
    # Pattern: CAST(... / 86400 AS BIGINT)
    if not isinstance(node, exp.Cast):
        return None

    to_type = node.to
    if not (isinstance(to_type, exp.DataType) and to_type.this == exp.DataType.Type.BIGINT):
        return None

    inner = node.this
    # Pattern: ... / 86400
    if not isinstance(inner, exp.Div):
        return None

    divisor = inner.right
    if not (isinstance(divisor, exp.Literal) and divisor.is_number):
        return None

    # Check if divisor is 86400 (seconds in a day)
    try:
        if int(divisor.this) != 86400:
            return None
    except (ValueError, TypeError):
        return None

    extract_expr = inner.left
    # Pattern: EXTRACT(epoch FROM ...)
    if not isinstance(extract_expr, exp.Extract):
        return None

    if str(extract_expr.this).upper() != "EPOCH":
        return None

    # The expression being extracted from should be a timestamp subtraction
    return extract_expr.expression


def _is_abs_with_subtraction(node: exp.Expression):
    """
    Check if node is ABS(date1 - date2) pattern.
    Returns the subtraction expression if found, None otherwise.
    """
    if isinstance(node, exp.Abs):
        inner = node.this
        if isinstance(inner, exp.Sub):
            return inner
        # Handle ABS((date1 - date2)) with extra parens
        if isinstance(inner, exp.Paren) and isinstance(inner.this, exp.Sub):
            return inner.this
    return None


def _is_numeric_value(node: exp.Expression) -> bool:
    """
    Check if a node represents a numeric value.
    Handles literal numbers, CAST(number AS INT/INTEGER/BIGINT) patterns,
    and query parameters/placeholders (e.g., :days, $1, ?).
    """
    # Direct literal number
    if isinstance(node, exp.Literal) and node.is_number:
        return True

    # Query parameters/placeholders (e.g., :days, $1, ?)
    # These are assumed to be numeric in the context of date comparisons
    if isinstance(node, (exp.Placeholder, exp.Parameter)):
        return True

    # CAST(number AS numeric_type) - e.g., 1000::int
    if isinstance(node, exp.Cast):
        inner = node.this
        if isinstance(inner, exp.Literal) and inner.is_number:
            return True
        # Also accept CAST(placeholder AS numeric_type)
        if isinstance(inner, (exp.Placeholder, exp.Parameter)):
            return True

    return False


def _is_daterange_call(node: exp.Expression):
    """
    Check if node is a DATERANGE() function call.
    Returns (start_date, end_date, bounds) if found, None otherwise.
    """
    if isinstance(node, exp.Anonymous) and node.this.upper() == "DATERANGE":
        expressions = node.expressions
        if len(expressions) >= 2:
            start_date = expressions[0]
            end_date = expressions[1]
            bounds = expressions[2] if len(expressions) > 2 else None
            return start_date, end_date, bounds
    return None


def _create_struct_for_range(start_date: exp.Expression, end_date: exp.Expression) -> exp.Expression:
    """
    Create a STRUCT(start_date AS start, end_date AS end) for Databricks range representation.
    """
    return exp.Struct(
        expressions=[
            exp.PropertyEQ(this=exp.Identifier(this="start"), expression=start_date),
            exp.PropertyEQ(this=exp.Identifier(this="end"), expression=end_date)
        ]
    )


def _is_range_overlap_operator(node: exp.Expression):
    """
    Check if node uses the && operator (PostgreSQL range overlap).
    sqlglot parses this as ArrayOverlaps.
    Returns (left, right) if found, None otherwise.
    """
    # The && operator is parsed as ArrayOverlaps in PostgreSQL dialect
    if isinstance(node, exp.ArrayOverlaps):
        return node.this, node.expression
    return None


def _is_range_intersection_operator(node: exp.Expression):
    """
    Check if node uses the * operator for range intersection (PostgreSQL).
    Returns (left, right) if found, None otherwise.

    Heuristic: If both operands are column references (not field accesses) or nested Mul operations
    or Struct expressions (indicating chained range intersections), treat as range intersection.
    We exclude Dot expressions that access .start or .end fields.
    """
    if isinstance(node, exp.Mul):
        left = node.this
        right = node.expression

        # Check if both operands look like range references
        def looks_like_range(expr):
            # Unwrap parentheses
            if isinstance(expr, exp.Paren):
                expr = expr.this

            # Mul and Struct are definitely range operations
            if isinstance(expr, (exp.Mul, exp.Struct)):
                return True

            # Dot expressions that access .start or .end are field accesses, not ranges
            if isinstance(expr, exp.Dot):
                field_name = expr.expression
                if isinstance(field_name, exp.Identifier) and field_name.this in ("start", "end"):
                    return False
                return True

            # Column references - check if they're field accesses
            if isinstance(expr, exp.Column):
                # Check if this is a 3-level identifier like d2.dr.start
                # which gets parsed as Column(this="start", table="dr", db="d2")
                this = expr.this
                if isinstance(this, exp.Identifier) and this.this in ("start", "end"):
                    # This is a field access, not a range
                    return False

                # Check if we have a table identifier
                table = expr.args.get("table")
                if table and isinstance(table, exp.Identifier):
                    # Check if there's a db identifier too (3-level: db.table.column)
                    db = expr.args.get("db")
                    if db and isinstance(db, exp.Identifier):
                        # This is a 3-level identifier - check if it's a field access
                        if this and isinstance(this, exp.Identifier) and this.this in ("start", "end"):
                            return False
                    # 2-level identifier (table.column) is a range reference
                    return True

                # Single identifier - could be a range
                return True

            return False

        if looks_like_range(left) and looks_like_range(right):
            return left, right
    return None


def _unwrap_paren(expr: exp.Expression) -> exp.Expression:
    """Remove Paren wrapper if present."""
    if isinstance(expr, exp.Paren):
        return expr.this
    return expr


def _create_range_overlap_condition(left_range: exp.Expression, right_range: exp.Expression) -> exp.Expression:
    """
    Create overlap condition for two ranges in Databricks:
    left_range.start <= right_range.end AND right_range.start <= left_range.end

    Note: left_range and right_range may be Paren-wrapped expressions.
    """
    # Unwrap parentheses if present
    left_range = _unwrap_paren(left_range)
    right_range = _unwrap_paren(right_range)

    left_start = exp.Dot(this=left_range, expression=exp.Identifier(this="start"))
    left_end = exp.Dot(this=left_range, expression=exp.Identifier(this="end"))
    right_start = exp.Dot(this=right_range, expression=exp.Identifier(this="start"))
    right_end = exp.Dot(this=right_range, expression=exp.Identifier(this="end"))

    return exp.And(
        this=exp.LTE(this=left_start, expression=right_end),
        expression=exp.LTE(this=right_start, expression=left_end)
    )


def _create_range_intersection(left_range: exp.Expression, right_range: exp.Expression) -> exp.Expression:
    """
    Create range intersection for two ranges in Databricks:
    STRUCT(GREATEST(left.start, right.start) AS start, LEAST(left.end, right.end) AS end)

    Note: left_range and right_range may be Paren-wrapped expressions.
    """
    # Unwrap parentheses if present
    left_range = _unwrap_paren(left_range)
    right_range = _unwrap_paren(right_range)

    left_start = exp.Dot(this=left_range, expression=exp.Identifier(this="start"))
    left_end = exp.Dot(this=left_range, expression=exp.Identifier(this="end"))
    right_start = exp.Dot(this=right_range, expression=exp.Identifier(this="start"))
    right_end = exp.Dot(this=right_range, expression=exp.Identifier(this="end"))

    greatest_start = exp.Anonymous(this="GREATEST", expressions=[left_start, right_start])
    least_end = exp.Anonymous(this="LEAST", expressions=[left_end, right_end])

    return exp.Struct(
        expressions=[
            exp.PropertyEQ(this=exp.Identifier(this="start"), expression=greatest_start),
            exp.PropertyEQ(this=exp.Identifier(this="end"), expression=least_end)
        ]
    )


def _transform_window_spec(node: exp.Expression) -> exp.Expression:
    """
    Transform PostgreSQL window specifications to Databricks.

    Handles: RANGE BETWEEN CURRENT ROW AND '30 days'::interval FOLLOWING
    Converts to: RANGE BETWEEN CURRENT ROW AND INTERVAL 30 DAYS FOLLOWING

    PostgreSQL syntax: '30 days'::interval FOLLOWING
    sqlglot parses as: Cast(this=Literal('30 days'), to=DataType(this=Interval(unit=Var('FOLLOWING'))))
    """
    if isinstance(node, exp.WindowSpec):
        # Check if this is a RANGE frame with INTERVAL casting
        kind = node.args.get("kind")
        if kind == "RANGE":
            # Look at the end bound (FOLLOWING)
            end = node.args.get("end")
            if end and isinstance(end, exp.Cast):
                # Check if this is Cast to INTERVAL with FOLLOWING
                to_type = end.to
                if isinstance(to_type, exp.DataType):
                    # Check if DataType.this is an Interval with unit=FOLLOWING
                    if isinstance(to_type.this, exp.Interval):
                        interval_datatype = to_type.this
                        if isinstance(interval_datatype.unit, exp.Var) and interval_datatype.unit.this == "FOLLOWING":
                            # Extract the interval string from Cast.this
                            interval_str = end.this
                            if isinstance(interval_str, exp.Literal):
                                # Parse "30 days" or similar
                                value_str = interval_str.this
                                # Create proper INTERVAL expression for Databricks
                                # In Databricks: INTERVAL 30 DAYS
                                parts = value_str.split()
                                if len(parts) == 2:
                                    number = parts[0]
                                    unit = parts[1].upper()
                                    # Create Interval node with correct structure
                                    interval_node = exp.Interval(
                                        this=exp.Literal.number(number),
                                        unit=exp.var(unit)
                                    )
                                    # Replace the Cast with Interval
                                    node.set("end", interval_node)
    return node


def _transform_date_operations(tree: exp.Expression) -> exp.Expression:
    """
    Walk the AST and convert PostgreSQL date operations to Databricks equivalents.

    Handles:
    1. Simple date subtraction: (date1 - date2) <= N
    2. Epoch extraction: CAST(EXTRACT(epoch FROM ts1 - ts2) / 86400 AS BIGINT) <= N
    3. ABS pattern: ABS(date1 - date2) <= N
    4. DATERANGE() function: convert to STRUCT(start, end)
    5. Range overlap operator (&&): convert to overlap condition
    6. Range intersection operator (*): convert to STRUCT with GREATEST/LEAST
    7. Window INTERVAL syntax: CAST('N days' AS INTERVAL) -> INTERVAL N DAYS

    Note: We use iterative transformation to ensure nested expressions are properly handled.
    """
    def transformer(node):
        # Handle window specifications with INTERVAL casting
        node = _transform_window_spec(node)

        # Handle date + integer arithmetic (PostgreSQL allows date + int for days)
        # Convert to DATE_ADD(date, int) for Databricks
        if isinstance(node, exp.Add):
            left = node.this
            right = node.expression

            # Check if this looks like date arithmetic (date + number)
            # We only transform if the right side is a numeric literal
            if isinstance(right, exp.Literal) and right.is_number:
                # Create DATE_ADD(left, right) function
                date_add = exp.Anonymous(
                    this="DATE_ADD",
                    expressions=[left, right]
                )
                return date_add

        # Handle DATERANGE() function
        daterange_parts = _is_daterange_call(node)
        if daterange_parts:
            start_date, end_date, bounds = daterange_parts
            return _create_struct_for_range(start_date, end_date)

        # Handle range overlap operator (&&)
        overlap_parts = _is_range_overlap_operator(node)
        if overlap_parts:
            left_range, right_range = overlap_parts
            return _create_range_overlap_condition(left_range, right_range)

        # Handle range intersection operator (*)
        intersection_parts = _is_range_intersection_operator(node)
        if intersection_parts:
            left_range, right_range = intersection_parts
            return _create_range_intersection(left_range, right_range)

        # Handle "IS NOT EMPTY" for ranges (PostgreSQL)
        # Sqlglot parses "IS NOT EMPTY" as NOT(IS(expr, EMPTY))
        # In PostgreSQL: range IS NOT EMPTY checks if range has any dates
        # In Databricks with STRUCT: check if start <= end
        if isinstance(node, exp.Not):
            inner = node.this
            if isinstance(inner, exp.Is):
                is_empty_check = inner.expression
                # Check if this is "IS EMPTY" (expression should be Column with name EMPTY)
                if isinstance(is_empty_check, exp.Column):
                    identifier = is_empty_check.this
                    if isinstance(identifier, exp.Identifier) and identifier.this.upper() == "EMPTY":
                        # Transform: NOT (range IS EMPTY) -> range.start <= range.end
                        range_expr = inner.this
                        range_expr = _unwrap_paren(range_expr)
                        start = exp.Dot(this=range_expr, expression=exp.Identifier(this="start"))
                        end = exp.Dot(this=range_expr, expression=exp.Identifier(this="end"))
                        return exp.LTE(this=start, expression=end)

        # Handle "IS EMPTY" for ranges (PostgreSQL)
        if isinstance(node, exp.Is):
            is_empty_check = node.expression
            # Check if this is "IS EMPTY" (expression should be Column with name EMPTY)
            if isinstance(is_empty_check, exp.Column):
                identifier = is_empty_check.this
                if isinstance(identifier, exp.Identifier) and identifier.this.upper() == "EMPTY":
                    # Transform: range IS EMPTY -> range.start > range.end
                    range_expr = node.this
                    range_expr = _unwrap_paren(range_expr)
                    start = exp.Dot(this=range_expr, expression=exp.Identifier(this="start"))
                    end = exp.Dot(this=range_expr, expression=exp.Identifier(this="end"))
                    return exp.GT(this=start, expression=end)

        # Look for comparisons
        if isinstance(node, (exp.LTE, exp.LT, exp.GTE, exp.GT, exp.EQ, exp.NEQ)):
            left = node.left
            right = node.right

            # Only process if comparing to a number (literal or cast)
            if not _is_numeric_value(right):
                return node

            # Check for ABS(date1 - date2) pattern
            sub_expr = _is_abs_with_subtraction(left)
            if sub_expr:
                left_date, right_date = _extract_date_operands_from_sub(sub_expr)
                if left_date and right_date:
                    datediff = _create_datediff(left_date, right_date)
                    # Wrap DATEDIFF in ABS
                    abs_datediff = exp.Abs(this=datediff)
                    node.set("this", abs_datediff)
                    return node

            # Check for epoch days pattern: CAST(EXTRACT(epoch FROM ...) / 86400 AS BIGINT)
            sub_expr = _is_epoch_days_pattern(left)
            if sub_expr:
                left_date, right_date = _extract_date_operands_from_sub(sub_expr)
                if left_date and right_date:
                    datediff = _create_datediff(left_date, right_date)
                    node.set("this", datediff)
                    return node

            # Check for parenthesized subtraction: (date1 - date2)
            if isinstance(left, exp.Paren) and isinstance(left.this, exp.Sub):
                left_date, right_date = _extract_date_operands_from_sub(left.this)
                if left_date and right_date:
                    datediff = _create_datediff(left_date, right_date)
                    node.set("this", datediff)
                    return node

            # Check for direct subtraction: date1 - date2
            if isinstance(left, exp.Sub):
                left_date, right_date = _extract_date_operands_from_sub(left)
                if left_date and right_date:
                    datediff = _create_datediff(left_date, right_date)
                    node.set("this", datediff)
                    return node

        return node

    # Apply transformations iteratively until no more changes occur
    # This ensures nested range operations are fully transformed
    max_iterations = 10
    for i in range(max_iterations):
        old_sql = tree.sql()
        tree = tree.transform(transformer)  # copy=True by default for proper transformation
        new_sql = tree.sql()
        if old_sql == new_sql:
            break
    return tree


def transpile_query(sql: str, source_dialect: str = "postgres", target_dialect: str = "databricks") -> str:
    """
    Transpile a SQL query from one dialect to another.

    Args:
        sql: The SQL query to transpile
        source_dialect: The source SQL dialect (default: "postgres")
        target_dialect: The target SQL dialect (default: "databricks")

    Returns:
        The transpiled SQL query
    """
    try:
        # Parse the SQL
        tree = sqlglot.parse_one(sql, read=source_dialect)

        # Apply custom transformations for PostgreSQL -> Databricks
        if source_dialect == "postgres" and target_dialect == "databricks":
            tree = _transform_date_operations(tree)

        # Generate the target dialect SQL
        transpiled = tree.sql(dialect=target_dialect)
        return transpiled
    except Exception as e:
        raise ValueError(f"Error transpiling query: {e}") from e


def transpile_file(input_path: str, output_path: str = None, source_dialect: str = "postgres", target_dialect: str = "databricks") -> str:
    """
    Transpile SQL from a file.

    Args:
        input_path: Path to the input SQL file
        output_path: Path to save the transpiled SQL (optional)
        source_dialect: The source SQL dialect (default: "postgres")
        target_dialect: The target SQL dialect (default: "databricks")

    Returns:
        The transpiled SQL query
    """
    input_file = Path(input_path)

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    sql = input_file.read_text()
    transpiled = transpile_query(sql, source_dialect, target_dialect)

    if output_path:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(transpiled)
        print(f"Transpiled SQL saved to: {output_path}")

    return transpiled


def main():
    """
    Example usage of the transpiler.
    """
    # Example PostgreSQL query with epoch extraction pattern
    example_query = """
    WITH seed_a AS (
        SELECT c.concept_id AS src_id
        FROM concept c
        WHERE c.vocabulary_id = 'SNOMED'
          AND c.concept_code = '59621000'
          AND c.invalid_reason IS NULL
    ),
    std_a AS (
        SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
        FROM seed_a s
        LEFT JOIN concept_relationship cr
          ON cr.concept_id_1 = s.src_id
          AND cr.relationship_id = 'Maps to'
          AND cr.invalid_reason IS NULL
    ),
    desc_a AS (
        SELECT DISTINCT ca.descendant_concept_id AS concept_id
        FROM std_a sa
        JOIN concept_ancestor ca ON ca.ancestor_concept_id = sa.standard_id
        JOIN concept c ON c.concept_id = ca.descendant_concept_id
          AND c.standard_concept = 'S'
          AND c.domain_id = 'Condition'
          AND c.invalid_reason IS NULL
    ),
    condition_a_occurrences AS (
        SELECT DISTINCT co.person_id, co.condition_start_date::date AS start_date
        FROM condition_occurrence co
        JOIN desc_a da ON co.condition_concept_id = da.concept_id
    )
    SELECT COUNT(DISTINCT person_id)
    FROM condition_a_occurrences
    WHERE (start_date - '2020-01-01'::date) <= 30;
    """

    print("Original PostgreSQL query:")
    print("-" * 80)
    print(example_query)
    print("\n")

    transpiled = transpile_query(example_query)

    print("Transpiled Databricks Spark SQL:")
    print("-" * 80)
    print(transpiled)
    print("\n")


if __name__ == "__main__":
    main()
