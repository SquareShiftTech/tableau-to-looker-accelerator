# Contributing Guide

## 🚀 Quick Setup

```bash
# 1. Install dependencies
uv sync --group dev

# 2. Install pre-commit hooks (REQUIRED!)
uv run pre-commit install

# 3. Test setup
uv run pre-commit run --all-files
```

> **⚠️ Important**
>
> Pre-commit hooks are **REQUIRED**. If you skip step 2, your commits won't be checked and may fail CI.

---

## 🔄 Development Workflow

1. Make your changes
2. Stage your changes:
   ```bash
   git add .
   ```
3. Commit your changes:
   ```bash
   git commit -m "your message"
   ```

Pre-commit will automatically run:
- Formats code with Ruff
- Fixes linting issues
- Runs tests

If hooks fail, fix the issues and commit again.

---

## 🧪 Manual Testing (Optional)

```bash
# Run tests
uv run pytest

# Format code
uv run ruff format .

# Check linting
uv run ruff check --fix .
```

---

## 🆘 Troubleshooting

```bash
# Check if hooks are installed
ls -la .git/hooks/pre-commit

# Reinstall hooks
uv run pre-commit install

# Test hooks
uv run pre-commit run --all-files
```

That's it! The hooks handle everything else automatically.
