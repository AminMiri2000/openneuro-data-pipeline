# Contributing

## Workflow

- `main` holds released versions; `dev` is the integration branch.
- Every change gets its own branch from the latest `dev`
  (`feature/…`, `fix/…`, `refactor/…`, `docs/…`), and its pull request targets `dev`.
- Commits follow [Conventional Commits](https://www.conventionalcommits.org)
  (`feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`).

## Before opening a pull request

```bash
make install     # once: virtualenv + pre-commit hooks
make lint typecheck test
```

CI runs the same checks and blocks merging when any fail, including test
coverage below 90%.

## Code conventions

- **Layering:** respect the dependency direction in
  [docs/architecture.md](docs/architecture.md). Entry points only call `Pipeline`.
- **Docstrings** on every public module and class, and on any function whose
  name does not fully explain it. Comments explain *why*, not *what*.
- **Types everywhere:** the package is checked with `mypy --strict`.
- **Errors:** raise subclasses of `PipelineError` for expected failures; let
  genuine bugs propagate.
- **No hard-coded paths:** every raw-layer key comes from `storage/layout.py`.
- **Tests never use the network:** use the fixtures in `tests/conftest.py`.
