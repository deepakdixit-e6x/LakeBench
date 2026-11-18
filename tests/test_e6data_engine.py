import sys
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import sqlglot
from sqlglot import exp

from lakebench.benchmarks import TPCDS, TPCH, ClickBench
from lakebench.engines import E6Data
from lakebench.engines import e6data as engine_module


@pytest.fixture
def connector(monkeypatch):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.execute.return_value = "query-123"
    cursor.fetchall_buffer.return_value = iter([[(1,), (2,)], [(3,)]])
    factory = MagicMock(return_value=connection)
    monkeypatch.setitem(sys.modules, "e6data_python_connector", SimpleNamespace(Connection=factory))
    monkeypatch.setattr(E6Data, "verify_dependencies", classmethod(lambda cls: None))
    monkeypatch.setattr(engine_module, "version", lambda package: "2.3.15")
    return SimpleNamespace(factory=factory, connection=connection, cursor=cursor)


def make_engine(**kwargs):
    return E6Data(
        host="cluster.example",
        username="bench@example.com",
        password="secret-token",
        catalog_name="benchmark_catalog",
        schema_name="tpch",
        **kwargs,
    )


def test_connection_configuration_and_metadata(connector):
    engine = make_engine(port=443, secure=True, connection_options={"ssl_cert": b"certificate", "auto_resume": False})
    connector.factory.assert_called_once_with(
        host="cluster.example",
        port=443,
        username="bench@example.com",
        password="secret-token",
        catalog="benchmark_catalog",
        database="tpch",
        secure=True,
        ssl_cert=b"certificate",
        auto_resume=False,
    )
    assert engine.version == "unknown"
    assert engine.get_total_cores() is None
    assert engine.get_compute_size() == "unknown"
    assert engine.extended_engine_metadata["connector_version"] == "2.3.15"
    assert "secret-token" not in repr(engine.extended_engine_metadata)


def test_remote_compute_and_cost(connector):
    engine = make_engine(total_cores=64, compute_size="2 workers", engine_version="test-engine", cost_per_hour=12)
    assert engine.get_total_cores() == 64
    assert engine.get_compute_size() == "2 workers"
    assert engine.version == "test-engine"
    assert engine.get_job_cost(3600000) == Decimal("12")


@pytest.mark.parametrize("total_cores", [0, -1, True, 1.5])
def test_invalid_remote_core_count(connector, total_cores):
    with pytest.raises(ValueError, match="positive integer"):
        make_engine(total_cores=total_cores)
    connector.factory.assert_not_called()


def test_reserved_connection_options_rejected(connector):
    with pytest.raises(ValueError, match="named engine parameters"):
        make_engine(connection_options={"database": "another_schema"})
    connector.factory.assert_not_called()


def test_query_drains_batches_and_returns_telemetry(connector):
    engine = make_engine()
    assert engine.execute_sql_query("SELECT value FROM sample") == {"query_id": "query-123", "row_count": "3"}
    connector.connection.cursor.assert_called_once_with(catalog_name="benchmark_catalog", db_name="tpch")
    connector.cursor.execute.assert_called_once_with("SELECT value FROM sample")
    connector.cursor.fetchall.assert_not_called()
    connector.connection.cursor.return_value.__exit__.assert_called_once_with(None, None, None)


@pytest.mark.parametrize("failure_point", ["execute", "fetch"])
def test_query_errors_propagate_and_release_cursor(connector, failure_point):
    engine = make_engine()
    if failure_point == "execute":
        connector.cursor.execute.side_effect = RuntimeError("execution failed")
    else:

        def failing_batches():
            yield [(1,)]
            raise RuntimeError("fetch failed")

        connector.cursor.fetchall_buffer.return_value = failing_batches()
    with pytest.raises(RuntimeError, match="failed"):
        engine.execute_sql_query("SELECT 1")
    assert connector.connection.cursor.return_value.__exit__.call_args.args[0] is RuntimeError


def test_return_data_preserves_columns_for_empty_results(connector):
    pytest.importorskip("pandas")
    engine = make_engine()
    connector.cursor.description = [("value",)]
    connector.cursor.fetchall.return_value = []
    result = engine.execute_sql_query("SELECT 1 WHERE false", return_data=True)
    assert list(result.columns) == ["value"]
    assert result.empty


def test_engine_context_closes_connection_on_error(connector):
    with pytest.raises(RuntimeError):
        with make_engine():
            raise RuntimeError("failed")
    connector.connection.close.assert_called_once_with()


@pytest.mark.parametrize("benchmark_class", [TPCH, TPCDS, ClickBench])
def test_all_canonical_queries_render_and_qualify_existing_tables(connector, benchmark_class):
    engine = make_engine()
    options = {} if benchmark_class is ClickBench else {"scale_factor": 1000}
    benchmark = benchmark_class(engine=engine, scenario_name="e6data", **options)
    assert benchmark.benchmark_impl is None
    for name in benchmark.QUERY_REGISTRY:
        query = benchmark._return_query_definition(name)
        expression = sqlglot.parse_one(query, read=engine.SQLGLOT_DIALECT)
        tables = [table for table in expression.find_all(exp.Table) if table.name in benchmark.TABLE_REGISTRY]
        assert tables, name
        assert all(table.catalog == "benchmark_catalog" and table.db == "tpch" for table in tables), name


@pytest.mark.parametrize("mode", ["query", "power_test"])
def test_benchmark_runs_without_input_files_and_records_telemetry(connector, mode):
    benchmark = TPCH(engine=make_engine(), scenario_name="e6data", query_list=["q6"], scale_factor=1000)
    benchmark.run(mode=mode)
    assert len(benchmark.results) == 1
    result = benchmark.results[0]
    assert result["success"] is True
    assert result["execution_telemetry"]["query_id"] == "query-123"
    assert result["execution_telemetry"]["row_count"] == "3"
    assert "benchmark_catalog.tpch.lineitem" in result["sql_text"]


def test_benchmark_records_fetch_failure(connector):
    connector.cursor.fetchall_buffer.side_effect = RuntimeError("fetch failed")
    benchmark = TPCH(engine=make_engine(), scenario_name="e6data", query_list=["q6"], scale_factor=1000)
    benchmark.run(mode="query")
    assert benchmark.results[0]["success"] is False
    assert "fetch failed" in benchmark.results[0]["error_message"]
    assert connector.connection.cursor.return_value.__exit__.call_args.args[0] is RuntimeError


@pytest.mark.parametrize("mode", ["load", "load_and_query"])
def test_load_modes_fail_before_any_queries_are_submitted(connector, mode):
    benchmark = TPCH(engine=make_engine(), scenario_name="e6data", scale_factor=1000)
    with pytest.raises(NotImplementedError, match="existing catalog tables"):
        benchmark.run(mode=mode)
    connector.connection.cursor.assert_not_called()
    assert benchmark.results == []
