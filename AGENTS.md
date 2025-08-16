# AGENT Instructions for AI coding tools

This project already provides environment details for GitHub's coding agent in
[AI_ENVIRONMENT.md](AI_ENVIRONMENT.md). Codex expects an `AGENTS.md` file to
locate similar guidance. This file points both agents to the same instructions.

## Quick Setup

Use Python 3.12. Install dependencies and run tests with:

```bash
pip install -e ".[dev]" && pytest
```

For coverage information, run:

```bash
pytest --cov=ice_stream --cov-report=term-missing -v
```

See the **AI_ENVIRONMENT.md** document for complete environment and workflow
information.
