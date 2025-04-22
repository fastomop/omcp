# Installation Guide 🚀

## ⚠️ Pre-Alpha Stage Warning

**OMCP is currently in pre-alpha development stage**. This means:

- Features may change significantly between versions
- Breaking changes can occur without notice
- Documentation may be incomplete or outdated
- Bugs and unexpected behaviors are likely

We appreciate your patience and feedback as we work toward a stable release!

## Installing from GitHub 📦

### Prerequisites

- [Git](https://git-scm.com/)
- [uv (Python package installer) ](https://docs.astral.sh/uv/)
    - On MacOS/Linux
    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```
    - On Windows
    ```powershell
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    ```

- Python 3.13 or higher (Let UV install and manage python versions within a virtual environment)


### Step 1: Clone the Repository

```bash
git clone https://github.com/vvcb/omcp.git
cd omcp
```

### Step 2: Install the package with dependencies

UV automatically creates the virtual environment with the correct python version and dependencies.

#### With DuckDB Support (Default)

```bash
uv sync --extra duckdb
```

#### With PostgreSQL Support

!!!warning
    Not implemented yet.

```bash
uv sync --extra postgres
```

### Step 3: Activate the virtual environment

```bash
source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
```


## Using the Synthetic Database 🗄️

OMCP comes with a synthetic OMOP database (located at `/synthetic_data/synthea.duckdb`) for testing and development purposes. This database follows the OMOP Common Data Model and contains fictional patient data available at [Synthea](https://synthetichealth.github.io/synthea/).

### Setting Up the Environment Variable

Create a `.env` file in your project root with the connection string:

```bash
echo 'DB_CONNECTION_STRING="duckdb:///full/path/to/omcp/synthetic_data/synthea.duckdb"' > .env
```

Or set it directly in your environment:

```bash
# Linux/Mac
export DB_CONNECTION_STRING="duckdb:///full/path/to/omcp/synthetic_data/synthea.duckdb"

# Windows (Command Prompt)
set DB_CONNECTION_STRING=duckdb:///full/path/to/omcp/synthetic_data/synthea.duckdb

# Windows (PowerShell)
$env:DB_CONNECTION_STRING="duckdb:///full/path/to/omcp/synthetic_data/synthea.duckdb"
```

### Using the Database for Experiments

The synthetic database contains a complete OMOP CDM schema with fictional patient data. You can:

1. Explore tables like `person`, `condition_occurrence`, `drug_exposure`, etc.
2. Run queries against it to test your applications
3. Use it as a sandbox for learning the OMOP data model

It is easy to do this using the [DuckDB UI](https://duckdb.org/2025/03/12/duckdb-ui.html) by running the following command from the `/synthetic_data` directory.

```bash
duckdb -ui synthea.duckdb
```

!!! warning
    DuckDB does not allow multiple processes to open a connection to the database at the same time when at least one of them has write access.
    To avoid running into problems, close any open connections to the database before running the MCP server.

## Integrating with Claude Desktop 🤖

OMCP provides a Model Context Protocol server that can integrate with Claude Desktop.

### Step 1: Install Claude Desktop

Download and install [Claude Desktop](https://claude.ai/download) from the official website.
On Linux, use https://github.com/aaddrick/claude-desktop-debian/ or similar until an official release becomes available.


### Step 2: Configure Claude Desktop to Use OMCP

1. Open or create the Claude Desktop configuration file:

```bash
# Linux
mkdir -p ~/.config/Claude
nano ~/.config/Claude/claude_desktop_config.json

# macOS
mkdir -p ~/Library/Application\ Support/Claude
nano ~/Library/Application\ Support/Claude/claude_desktop_config.json

# Windows
notepad %APPDATA%\Claude\claude_desktop_config.json
```

2. Add the following configuration:

```json
{
    "mcpServers": {
        "omop_mcp": {
            "command": "mcp",
            "args": [
                "run",
                "/path/to/omcp/src/omcp/main.py"
            ]
        }
    }
}
```

Replace `/path/to/omcp` with the actual path to your OMCP installation.

### Step 3: Launch Claude Desktop

Start Claude Desktop and the OMCP server should automatically be available for use. You can verify the connection by asking Claude to query the OMOP database.

## Troubleshooting 🔧

### Common Issues

#### DuckDB Connection Problems

If you encounter errors connecting to the DuckDB database:

1. Verify the file path in your connection string is correct
2. Ensure the database file exists at the specified location
3. Check permissions on the database file

#### Python Version Issues

OMCP requires Python 3.13+. To check your Python version:

```bash
python --version
```

#### Claude Desktop Integration Issues

If Claude Desktop doesn't recognize the OMCP server:

1. Verify the configuration file is in the correct location
2. Ensure the path to main.py is correct
3. Restart Claude Desktop

## Getting Help 💬

If you encounter issues not covered here:

- Open an issue on our [GitHub repository](https://github.com/vvcb/omcp/issues)
- Join our community discussions
- Check the FAQ section in our documentation

## Next Steps 👣

Now that you have OMCP installed and configured:

- Explore the [Contributing Guidelines](./contributing.md) if you'd like to help improve OMCP
