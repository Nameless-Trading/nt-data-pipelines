# nt-data-pipelines
Data pipelines for data aggregation, signal computation, portfolio construction, and live trading.

## Set Up

Create Python virtual environment using uv. Learn how to install uv [here](https://docs.astral.sh/uv/getting-started/installation/).

```bash
uv sync
```

Activate virtual environment.

```bash
source .venv/bin/activate # MacOS/Linux
```

Create .env file with at least these variables (you can get these from Andrew).
```
PREFECT_API_URL=
PREFECT_CLIENT_CUSTOM_HEADERS=
```

## Development

To run a pipeline locally add the following to the bottom of a *_flow.py file. For example:

```python
if __name__ == '__main__':
    benchmark_daily_flow()
```

Then you can run it using:

```bash
python pipelines/*_flow.py
```

## Deployment

To deploy a pipeline you need to add it to the `serve()` function in the `pipelines/__main__.py` file. For example:

```python
serve(
    ... other flows,
    benchmark_backfill_flow.to_deployment(name="benchmark-backfill-flow"),
)
```

Note that most of the flows in the `serve()` function don't run on a schedule. This allows for them to be triggered from the UI remotely. The only flow ran on a schedule is the `daily_flow()`. If you're flow needs to run on a daily schedule add it there.

Flows will automatically be deployed to production when their corresponding code is added to the `main` branch.
