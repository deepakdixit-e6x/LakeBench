from __future__ import annotations

from importlib.metadata import version
from typing import Any, Mapping, Optional

from .base import BaseEngine


class E6Data(BaseEngine):
    """Run query benchmarks against tables registered in an e6data catalog.

    Attributes
    ----------
    host : str
        Cluster hostname.
    username : str
        e6data account email.
    password : str
        e6data access token.
    catalog_name : str
        Catalog containing the benchmark tables.
    schema_name : str
        Database containing the benchmark tables.
    port : int, optional
        Cluster port, normally 80 or 443.
    secure : bool, optional
        Enable TLS for the connector.
    connection_options : mapping, optional
        Additional connector options, such as ssl_cert, cluster_name and grpc_options.
    total_cores : int, optional
        Remote cluster cores, if known; never inferred from the client machine.
    compute_size : str, optional
        Remote cluster size label used in benchmark results.
    engine_version : str, optional
        Remote engine version, if known.
    cost_per_hour : float, optional
        Total cluster hourly cost used for estimated query cost.
    """

    # Use Spark-compatible SQL rendering: the upstream SQLGlot dependency does
    # not ship e6data's custom E6 dialect. Live compatibility must be checked on
    # the target cluster before comparing benchmark results.
    SQLGLOT_DIALECT = "spark"
    SUPPORTS_MOUNT_PATH = False
    SUPPORTS_ONELAKE = False
    SUPPORTS_SCHEMA_PREP = False
    SUPPORTS_LOAD = False
    REQUIRED_MODULES = ("e6data_python_connector", "pandas")
    INSTALL_EXTRA = "e6data"

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        catalog_name: str,
        schema_name: str,
        port: int = 80,
        secure: bool = False,
        connection_options: Optional[Mapping[str, Any]] = None,
        total_cores: Optional[int] = None,
        compute_size: Optional[str] = None,
        engine_version: Optional[str] = None,
        cost_per_hour: Optional[float] = None,
    ):
        for name, value in (
            ("host", host),
            ("username", username),
            ("password", password),
            ("catalog_name", catalog_name),
            ("schema_name", schema_name),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string.")
        if total_cores is not None and (
            isinstance(total_cores, bool) or not isinstance(total_cores, int) or total_cores <= 0
        ):
            raise ValueError("total_cores must be a positive integer when supplied.")
        options = dict(connection_options or {})
        reserved = {"host", "port", "username", "password", "catalog", "database", "secure"}.intersection(options)
        if reserved:
            raise ValueError(f"Use the named engine parameters instead of connection_options for: {sorted(reserved)}")

        super().__init__()
        from e6data_python_connector import Connection

        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.total_cores = total_cores
        self.compute_size = compute_size
        self.version = engine_version or "unknown"
        self._configure_cost_inputs(cost_per_hour=cost_per_hour)
        self.extended_engine_metadata.update(
            {
                "connector_version": version("e6data-python-connector"),
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "sql_rendering_dialect": self.SQLGLOT_DIALECT,
            }
        )
        self.connection = Connection(
            host=host,
            port=port,
            username=username,
            password=password,
            catalog=catalog_name,
            database=schema_name,
            secure=secure,
            **options,
        )

    def get_total_cores(self) -> Optional[int]:
        return self.total_cores

    def get_compute_size(self) -> str:
        return self.compute_size or (f"{self.total_cores}vCore" if self.total_cores is not None else "unknown")

    def execute_sql_query(self, query: str, return_data: bool = False, context_decorator: Optional[str] = None):
        with self.connection.cursor(catalog_name=self.catalog_name, db_name=self.schema_name) as cursor:
            query_id = cursor.execute(query)
            if return_data:
                import pandas as pd

                rows = cursor.fetchall()
                return pd.DataFrame(rows, columns=[column[0] for column in cursor.description])

            # Drain server batches within the timer without retaining all rows.
            # Cursor.close() clears the server query handle, including on error.
            row_count = 0
            for batch in cursor.fetchall_buffer():
                row_count += len(batch)
            return {"query_id": str(query_id), "row_count": str(row_count)}

    def execute_sql_statement(self, statement: str, context_decorator: Optional[str] = None):
        return self.execute_sql_query(statement, context_decorator=context_decorator)

    def load_parquet_to_delta(self, *args, **kwargs):
        raise NotImplementedError("E6Data supports query benchmarks on existing catalog tables. Load data separately.")

    def load_delimited_to_delta(self, *args, **kwargs):
        raise NotImplementedError("E6Data supports query benchmarks on existing catalog tables. Load data separately.")

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
