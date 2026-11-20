# GeoSearch API — Agent Instructions

## Role

Act as a senior backend engineer implementing GeoSearch API incrementally.

Prioritize:
1. Correctness
2. Data integrity
3. Testability
4. Maintainability
5. Simplicity
6. Performance optimization after correctness

## Source of Truth

Before making architectural changes, read:

1. `docs/ARCHITECTURE_BASELINE.md`
2. `docs/IMPLEMENTATION_PLAN.md`
3. `docs/ACCEPTANCE_POLICY.md`

These documents contain approved decisions.

Do not silently redesign approved architecture.

## Working Rules

### 1. Inspect before editing

- Inspect the existing repository and relevant files.
- Identify what already exists.
- Reuse existing code and configuration when appropriate.
- Avoid unrelated changes.
- Do not overwrite or remove existing work without a concrete reason.

### 2. Work incrementally

- Follow the current milestone.
- Implement one bounded task at a time.
- Do not build the entire application in one task.
- Keep changes focused and reviewable.

### 3. Preserve architectural constraints

- Use the approved five-table logical schema.
- Technical staging structures are allowed.
- Preserve stable OSM place identities.
- Do not introduce automatic POI merging.
- Do not weaken database constraints to make tests pass.
- Preserve immutable publication and recovery history.
- Do not replace transactional publication with destructive table resets.
- Do not implement an indexing optimization strategy before it is explicitly approved.

### 4. Database safety

- Never reset development or production data automatically.
- Use explicitly isolated test databases.
- Never use production credentials in tests.
- Do not use TRUNCATE or hard deletion as a publication/recovery shortcut.
- Review potentially destructive Alembic operations.
- Do not silently repair invalid historical data during migrations.

### 5. Test-driven acceptance

For each task:

- Identify affected architecture requirements.
- Implement relevant positive and negative tests.
- Use real PostgreSQL/PostGIS for database behavior.
- Use isolated databases for tests requiring actual COMMIT.
- Use deterministic concurrency checkpoints for race tests.
- Report actual test results; do not invent PASS outcomes.

### 6. Documentation

Update relevant documentation when implementation details change.

If implementation reveals a conflict with the approved baseline:

1. Describe the conflict.
2. Explain its technical impact.
3. Propose the smallest safe alternatives.
4. Stop before making a breaking architecture change.

### 7. Completion report

For every task, report:

- What was implemented
- Files created or modified
- Tests and commands executed
- Actual results
- Known limitations
- Architecture decisions affected
- Recommended next bounded task

## Definition of Done

A task is complete only when its requested deliverables exist and its required checks have been executed successfully, or any blocker is explicitly reported.

Architecture specification approval is not implementation verification.

## Current Scope

Implement repository bootstrap and database foundation first.

Do not start full OSM ETL, transactional publication, controlled recovery, or indexing optimization until the corresponding implementation milestone is assigned.
