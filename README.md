# Temporal Workflow (Python)

A starting point for building durable workflows with Python and [Temporal](https://temporal.io/).

Temporal coordinates long-running processes, persists workflow progress, and
supports retries and recovery when workers or services fail.

> This directory currently contains documentation only. The application code,
> dependency manifest, and tests have not been added yet. The setup below prepares
> a local environment; it does not start an application worker.

## Prerequisites

- Python 3.10 or newer.
- The [Temporal CLI](https://docs.temporal.io/cli).
- A terminal such as PowerShell.

## Local setup

From this project directory, create a virtual environment and install the
Temporal Python SDK:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install temporalio
```

If PowerShell blocks activation, use the virtual environment's interpreter
directly instead:

```powershell
.\.venv\Scripts\python.exe -m pip install temporalio
```

Once application code is added, record its dependencies in a dependency manifest
so installations are reproducible.

## Start a local Temporal server

In a separate terminal, run:

```powershell
temporal server start-dev
```

Default local endpoints:

| Service | Address |
| --- | --- |
| Temporal server | `localhost:7233` |
| Temporal Web UI | <http://localhost:8233> |
| Namespace | `default` |

The development server is intended for local use, not production. To preserve
local workflow history across server restarts, use a database file:

```powershell
temporal server start-dev --db-filename temporal-dev.db
```

## Application structure

The following is a suggested layout, not an inventory of existing files:

```text
temporal_workflow/
|-- README.md
|-- pyproject.toml
|-- workflows.py
|-- activities.py
|-- worker.py
|-- start_workflow.py
`-- tests/
```

- **Workflows** define durable orchestration and must remain deterministic.
- **Activities** perform external I/O and other side effects, such as API calls
  and database operations.
- **Worker** connects to Temporal, registers workflows and activities, and polls
  a task queue.
- **Workflow starter** connects to Temporal and starts a workflow execution.
- **Tests** verify workflow behavior and activity implementations.

## Running the application

After implementing the suggested modules:

1. Start the local Temporal server.
2. Run the worker in another terminal:

   ```powershell
   python worker.py
   ```

3. Start a workflow from another terminal using the same Python environment:

   ```powershell
   python start_workflow.py
   ```

4. Open the Temporal Web UI to inspect execution history and activity results.

These commands assume the suggested scripts have been created. The worker and
workflow starter must use the same server address, namespace, and task queue.

## Development guidelines

- Keep network requests, filesystem access, and database operations in activities,
  rather than workflow code.
- Use Temporal's workflow APIs for time and asynchronous coordination.
- Configure activity timeouts and retry policies explicitly.
- Design activities to be idempotent because they may execute more than once.
- Do not commit credentials or local environment files.
- Add workflow and activity tests before relying on the application.

## Resources

- [Temporal documentation](https://docs.temporal.io/)
- [Python SDK documentation](https://docs.temporal.io/develop/python)
- [Python SDK API reference](https://python.temporal.io/)
- [Official Python samples](https://github.com/temporalio/samples-python)
