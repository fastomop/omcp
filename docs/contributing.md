# Contributing to OMCP 🤝

Thank you for your interest in contributing to OMCP (OMOP Model Context Protocol Server)! This document provides guidelines and instructions to help you get started with contributing to the project.

## Repository 📂

The OMCP project is hosted on GitHub:
- Repository: [https://github.com/fastomop/omcp](https://github.com/fastomop/omcp)
- Issue Tracker: [https://github.com/fastomop/omcp/issues](https://github.com/fastomop/omcp/issues)

## Ways to Contribute 🌟

There are many ways to contribute to OMCP:

- Reporting bugs and issues
- Suggesting new features or improvements
- Improving documentation
- Submitting code changes and fixes
- Helping other users in discussions
- Testing pre-release versions

## Development Setup 💻

1. Fork the repository on GitHub

2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR-USERNAME/omcp.git
   cd omcp
   ```
3. Set up a development environment:
   ```bash
   uv venv .venv
   source .venv/bin/activate
   ```
4. Install dependencies with development extras:
   ```bash
   uv sync --dev --extra duckdb
   ```
5. Set up pre-commit hooks:
   ```bash
   pre-commit install
   ```

## Development Workflow 🔄

1. Create a new branch for your feature or fix:
   ```bash
   git checkout -b feature-or-fix-name
   ```

2. Make your changes following our coding standards

3. Run the tests to ensure your changes don't break existing functionality

4. Update documentation if necessary

5. Commit your changes with a descriptive message:
   ```bash
   git commit -m "Add feature X" -m "This feature adds the ability to do X, which helps with Y"
   ```

6. Push your branch to GitHub:
   ```bash
   git push origin feature-or-fix-name
   ```

7. Open a pull request against the main branch

## Coding Standards 📝

- Follow PEP 8 style guidelines
- Use type hints
- Write docstrings for functions and classes
- Add tests for new functionality
- Keep commits focused and atomic

## Pull Request Process 🔍

1. Update the README.md or documentation with details of changes if needed
2. Make sure all CI checks pass
3. Get approval from at least one maintainer
4. Once approved, a maintainer will merge your PR

## Reporting Bugs 🐛

When reporting bugs, please include:

- A clear, descriptive title
- Steps to reproduce the issue
- Expected behavior
- Actual behavior
- Any relevant logs or screenshots
- Your environment details (OS, Python version, etc.)

## Feature Requests 💡

Feature requests are welcome! When submitting a feature request:

- Describe the problem you're trying to solve
- Explain how your feature would solve it
- Provide examples of how the feature would be used

## Code of Conduct 🤲

In all interactions, we expect all contributors to:

- Be respectful and inclusive
- Value different viewpoints and experiences
- Accept constructive criticism
- Focus on what's best for the community
- Show empathy towards other community members

## Questions? ❓

If you have any questions about contributing, feel free to:

- Open an issue on GitHub
- Reach out to the maintainers
- Ask in the project's discussion forums

Thank you for contributing to OMCP! 🎉
