# Welcome to OMCP

## What is OMCP?

OMCP (OMOP Model Context Protocol) is an open-source server that enables Large Language Models (LLMs) to interact with healthcare databases that follow the OMOP Common Data Model. It provides a structured way for AI systems to:

- Query healthcare data with appropriate security and privacy controls
- Perform cohort discovery and selection
- Generate statistical analyses and insights from clinical data
- Maintain data lineage and provenance tracking
- Access standardized healthcare terminologies and concept mappings

## OMOP Common Data Model

The [Observational Medical Outcomes Partnership (OMOP) Common Data Model (CDM)](https://www.ohdsi.org/data-standardization/) is a standardized data model designed to organize healthcare data into a common structure. It enables systematic analysis across disparate observational databases and facilitates collaborative research in the healthcare domain.

## Model Context Protocol

[The Model Context Protocol (MCP)](https://modelcontextprotocol.io/) is a framework that enables structured interaction between Large Language Models (LLMs) and databases. OMCP combines this protocol with the OMOP data model to create a powerful system for healthcare data analysis.

## Key Features

- **Secure API Layer**: Controlled access to OMOP CDM databases with authentication and authorization
- **LLM-Friendly Interface**: Structured protocols for AI models to interact with healthcare data
- **OMOP CDM Integration**: Seamless connection to any OMOP-compliant database
- **Query Translation**: Converts natural language or structured requests into optimized SQL
- **Data Governance**: Ensures compliance with healthcare data regulations like HIPAA
- **Extensible Architecture**: Support for plugins and custom extensions

## Use Cases

- Clinical research and cohort discovery
- Population health analytics
- Healthcare quality measurement
- Drug safety surveillance
- Clinical decision support
- Medical knowledge extraction

## Getting Started

See our [Installation Guide](./installation.md) to set up OMCP in your environment.

## Architecture

OMCP sits between LLMs and OMOP CDM databases, providing a structured, secure interface for AI models to query and analyze healthcare data without direct database access.

```
+-------+     +--------------+     +----------------+
|  LLM  | <-> | OMCP Server  | <-> | OMOP Database  |
+-------+     +--------------+     +----------------+
             /                \
   Natural Language      Structured Queries
     Requests            & Data Validation
```

## Community & Support

- [GitHub Repository](https://github.com/yourusername/omcp)
- [Community Forum](https://community.omcp.org)
- [Issue Tracker](https://github.com/yourusername/omcp/issues)
- [Contributing Guidelines](./contributing.md)

## License

OMCP is available under the [MIT License](https://opensource.org/license/mit).
