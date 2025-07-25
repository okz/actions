# AGENT Instructions for AI coding tools

This project already provides environment details for GitHub's coding agent in
[AI_ENVIRONMENT.md](AI_ENVIRONMENT.md). Codex expects an `AGENTS.md` file to
locate similar guidance. This file points both agents to the same instructions.

## Quick Setup

Use Python 3.12. Install dependencies and run tests with:

```bash
# Minimal environment (works everywhere - codex, agent, manual dev)
pip install -e ".[dev]" && pytest -m "not azurite and not external_service and not slow"

# Full environment (requires external services)
pip install -e ".[dev]" && pytest
```

For coverage information, run:

```bash
pytest --cov=actions_package --cov-report=term-missing -v
```

See the **AI_ENVIRONMENT.md** document for complete environment and workflow
information, and **TESTING.md** for detailed testing guidelines across different environments.
