"""Run TPC-DS SF1000 queries on existing e6data Delta tables.

Example:
    uv run --extra e6data python scripts/run_e6data_tpcds.py --catalog YOUR_CATALOG

Set E6DATA_HOST, E6DATA_USERNAME and E6DATA_TOKEN before running.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

logger = logging.getLogger(__name__)


def positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("Must be a positive integer.")
    return number


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--catalog", default=os.environ.get("E6DATA_CATALOG"), help="Catalog name, or E6DATA_CATALOG.")
    parser.add_argument("--host", default=os.environ.get("E6DATA_HOST"), help="Cluster hostname, or E6DATA_HOST.")
    parser.add_argument(
        "--username", default=os.environ.get("E6DATA_USERNAME"), help="Account email, or E6DATA_USERNAME."
    )
    parser.add_argument("--schema", default="tpcds_1000_delta", help="Existing database (default: tpcds_1000_delta).")
    parser.add_argument("--port", type=positive_int, default=443, help="Cluster port (default: 443).")
    parser.add_argument(
        "--insecure", action="store_true", help="Use plaintext instead of TLS; usually paired with --port 80."
    )
    parser.add_argument("--cluster-name", help="Optional connector cluster_name for cluster routing.")
    parser.add_argument("--ssl-cert", type=Path, help="Optional PEM certificate file for TLS.")
    parser.add_argument(
        "--queries", nargs="+", help="Optional query subset, e.g. --queries q3 q6. Default: full stream 0."
    )
    parser.add_argument("--iterations", type=positive_int, default=1, help="Number of measured runs (default: 1).")
    parser.add_argument(
        "--output", type=Path, help="JSON report path; default: reports/e6data/tpcds_sf1000_<timestamp>.json."
    )
    args = parser.parse_args(argv)
    for name in ("catalog", "host", "username"):
        value = getattr(args, name)
        if not value or not value.strip():
            parser.error(f"Supply --{name} or set E6DATA_{name.upper()}.")
    if not os.environ.get("E6DATA_TOKEN", "").strip():
        parser.error("Set E6DATA_TOKEN to your e6data access token.")
    if not args.schema.strip():
        parser.error("--schema must not be empty.")
    if args.port > 65535:
        parser.error("--port must be between 1 and 65535.")
    if args.ssl_cert is not None and not args.ssl_cert.is_file():
        parser.error("--ssl-cert must point to an existing PEM certificate file.")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    from lakebench.benchmarks import TPCDS
    from lakebench.engines import E6Data

    if args.queries:
        unknown = sorted(set(args.queries) - set(TPCDS.QUERY_REGISTRY))
        if unknown:
            raise SystemExit(f"Unknown TPC-DS queries: {', '.join(unknown)}. Use names such as q3, q14a, q14b.")

    started_at = datetime.now(timezone.utc)
    output = args.output or Path("reports/e6data") / f"tpcds_sf1000_{started_at.strftime('%Y%m%dT%H%M%S%fZ')}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    options = {}
    if args.cluster_name:
        options["cluster_name"] = args.cluster_name
    if args.ssl_cert:
        options["ssl_cert"] = str(args.ssl_cert)

    report = {
        "started_at": started_at.isoformat(),
        "benchmark": "TPCDS",
        "scale_factor": 1000,
        "catalog": args.catalog,
        "schema": args.schema,
        "host": args.host,
        "port": args.port,
        "secure": not args.insecure,
        "requested_iterations": args.iterations,
        "completed_iterations": 0,
        "completed": False,
        "results": [],
    }
    logger.info("Running TPC-DS SF1000 against %s.%s", args.catalog, args.schema)
    with E6Data(
        host=args.host,
        port=args.port,
        secure=not args.insecure,
        username=args.username,
        password=os.environ["E6DATA_TOKEN"],
        catalog_name=args.catalog,
        schema_name=args.schema,
        connection_options=options,
    ) as engine:
        benchmark = TPCDS(
            engine=engine,
            scenario_name="e6data-tpcds-1tb-delta",
            scale_factor=1000,
            query_list=args.queries,
            save_results=False,
        )
        report["queries"] = benchmark.query_list
        try:
            for iteration in range(1, args.iterations + 1):
                logger.info("Run %s/%s", iteration, args.iterations)
                start_index = len(benchmark.results)
                try:
                    benchmark.run(mode="query")
                finally:
                    report["results"].extend(
                        {**result, "runner_iteration": iteration} for result in benchmark.results[start_index:]
                    )
                report["completed_iterations"] = iteration
                output.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
            report["completed"] = True
        finally:
            report["finished_at"] = datetime.now(timezone.utc).isoformat()
            output.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
            logger.info("Results saved to %s", output.resolve())

    failures = sum(not result["success"] for result in report["results"])
    logger.info("Finished: %s queries, %s failed", len(report["results"]), failures)
    return 1 if failures else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(main())
