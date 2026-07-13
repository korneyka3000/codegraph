# Fixtures

Service fixtures under `services/` have no per-service `pyproject.toml`; real scip-python
anchors descriptor module-paths at this repo's root instead of the service root (see
`m1a-task-10-report` §1). M1b eval must add a minimal per-service `pyproject.toml`.
