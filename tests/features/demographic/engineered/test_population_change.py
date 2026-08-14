"""
Offline tests for the demographic population-change feature.

These tests exercise the SQL rendering and the build dispatch without any BigQuery
access. A fake client records the SQL it is asked to run, so the tests assert that
the rendered query references the resolved input and output tables and produces the
expected feature columns, and that a dry run builds the SQL without executing it.
"""
from network_idx.features.demographic.engineered import population_change


class _FakeQueryJob:
    def result(self):
        return None


class FakeClient:
    def __init__(self):
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        return _FakeQueryJob()


def test_render_sql_substitutes_tables_and_feature_columns():
    sql = population_change.render_sql("proj.ds.out", "proj.ds.in")
    assert "`proj.ds.out`" in sql
    assert "`proj.ds.in`" in sql
    # The engineered feature columns the model consumes must be present.
    assert "pop_ch_avg" in sql
    assert "pop_pctch_avg" in sql
    assert "tract_geoid" in sql
    # No unresolved template placeholders remain.
    assert "{output_table}" not in sql
    assert "{input_table}" not in sql


def test_input_table_resolves_from_registry():
    ref = population_change.input_table_ref()
    assert ref.endswith("neighborhood_scout_census_tract")


def test_build_executes_rendered_sql_with_client():
    client = FakeClient()
    population_change.build(client=client)
    assert len(client.queries) == 1
    assert population_change.output_table_ref() in client.queries[0].replace("`", "")


def test_build_dry_run_does_not_execute(capsys):
    client = FakeClient()
    population_change.build(client=client, dry_run=True)
    assert client.queries == []
    printed = capsys.readouterr().out
    assert "pop_ch_avg" in printed
