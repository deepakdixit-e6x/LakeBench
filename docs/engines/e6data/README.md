# e6data

Use the synchronous [e6data Python connector](https://github.com/e6data/e6data-python-connector)
to run TPC-H, TPC-DS, and ClickBench queries against existing catalog tables.
The adapter pins connector 2.3.15, matching the API used during implementation.

```bash
pip install 'lakebench[e6data]'
```

## TPC-DS 1 TB runner

From a source checkout, use the wrapper for existing Delta tables in
`tpcds_1000_delta`:

```bash
uv sync --extra e6data
export E6DATA_HOST='your-cluster-hostname'
export E6DATA_USERNAME='your-email'
export E6DATA_TOKEN='your-access-token'

# Single-query smoke test:
uv run --extra e6data python scripts/run_e6data_tpcds.py --catalog YOUR_CATALOG --queries q3

# Full SF1000 query stream:
uv run --extra e6data python scripts/run_e6data_tpcds.py --catalog YOUR_CATALOG
```

The wrapper defaults to TLS on port 443. For a plaintext endpoint, add
`--port 80 --insecure`. You can also set `E6DATA_CATALOG` instead of passing
`--catalog`. Use `--iterations 3` for three measured runs or `--output path.json`
to choose the report location. By default, JSON reports go under `reports/e6data/`.
Reports include per-query success, timings, errors, SQL, and connector telemetry;
the process exits with status 1 if any query fails. Results from completed
iterations are retained if a later iteration is interrupted. All runs use SF1000;
the wrapper does not generate or load data.

## Python API

```python
import logging
import os

from lakebench.benchmarks import TPCH
from lakebench.engines import E6Data

logging.basicConfig(level=logging.INFO)

with E6Data(
    host=os.environ["E6DATA_HOST"],
    port=443,
    secure=True,
    username=os.environ["E6DATA_USERNAME"],
    password=os.environ["E6DATA_TOKEN"],
    catalog_name=os.environ["E6DATA_CATALOG"],
    schema_name=os.environ["E6DATA_DATABASE"],
    # Optional: supply the remote cluster's total cores, size, version and cost.
    # total_cores=64,
    # compute_size="2 workers",
    # engine_version="<cluster version>",
    # cost_per_hour=12.0,
    # connection_options={"cluster_name": "benchmark", "auto_resume": False},
) as engine:
    benchmark = TPCH(
        engine=engine,
        scenario_name="e6data-tpch",
        scale_factor=1000,  # Must match the data already in the catalog.
        query_list=["q6"],  # Omit to run the full default stream.
        save_results=False,
    )
    benchmark.run(mode="query")
    print(benchmark.results)
```

Replace `TPCH` with `TPCDS` or `ClickBench` for those datasets. Queries qualify
table names with `catalog_name` and `schema_name`; the connector receives the
same catalog and database. No input folder is required for query runs.
Existing tables must have the benchmark's canonical names, columns, types and
scale factor. The adapter does not create, load, optimize, or analyze tables;
`load` and `load_and_query` raise an error before submitting work.

LakeBench currently uses SQLGlot's Spark-compatible renderer for e6data because
its pinned upstream SQLGlot package does not contain e6data's custom E6 dialect.
Query rendering is covered by unit tests, but a live e6data cluster is required
to verify SQL compatibility and result correctness before reporting comparisons.
ELTBench is not registered for this query-only adapter.

Each timed query consumes every result batch and closes its cursor, including
when execution or fetching fails. Timing includes connector execution, result
transfer, decoding, and cursor cleanup. The `query_id` and fetched `row_count`
are recorded in execution telemetry. `execute_sql_query(..., return_data=True)`
returns a pandas DataFrame for interactive inspection.

Configure result caching on the target cluster as required for the comparison;
the adapter does not change cluster settings. Run a warm-up separately if needed.
Remote cores and engine version remain unknown unless explicitly supplied.
The connector version is recorded separately in engine properties.

Use `connection_options` for connector settings such as `ssl_cert`,
`grpc_options`, `cluster_name`, `auto_resume`, or `require_fastbinary`. Host,
port, credentials, catalog, database and TLS must use the named engine arguments.

To persist results to a Delta log, set `save_results=True` and `result_table_uri`
on the benchmark. The e6data extra installs Delta support on Python 3.10+;
the Python client must have filesystem access and credentials for that location.
Otherwise, results remain available through `benchmark.results`.
