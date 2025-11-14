CREATE TABLE schema_table (
    pipeline_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    "materialized" BOOLEAN NOT NULL,
    has_computed_id BOOLEAN NOT NULL,
    has_tenant BOOLEAN NOT NULL,
    read_only BOOLEAN NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE schema_table_column (
    pipeline_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    data_type TEXT NOT NULL,
    nullable BOOLEAN NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE schema_table_archive_from_file (
    pipeline_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    filename TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE schema_table_archive_pg (
    pipeline_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    pg_url TEXT NOT NULL,
    pg_query TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE rule (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    "materialized" BOOLEAN NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE rule_param (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    "key" TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    expr_type TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE aggr_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    fn_name TEXT NOT NULL,
    arg_var TEXT
) WITH ('materialized' = 'true');

CREATE TABLE int_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    value BIGINT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE str_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    value TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE var_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    var_name TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE sql_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    template TEXT ARRAY NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE dict_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    dict_id TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE dict_entry (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    dict_id TEXT NOT NULL,
    key TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    expr_type TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE array_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    array_id TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE array_entry (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    array_id TEXT NOT NULL,
    "index" INTEGER NOT NULL,
    expr_id TEXT NOT NULL,
    expr_type TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE body_goal (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    goal_id TEXT NOT NULL,
    "index" INTEGER NOT NULL,
    table_name TEXT NOT NULL,
    negated BOOLEAN NOT NULL,
    id_var TEXT
) WITH ('materialized' = 'true');

CREATE TABLE goal_arg (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    goal_id TEXT NOT NULL,
    "key" TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    expr_type TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE body_match (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    match_id TEXT NOT NULL,
    left_expr_id TEXT NOT NULL,
    left_expr_type TEXT NOT NULL,
    right_expr_id TEXT NOT NULL,
    right_expr_type TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE body_sql_cond (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    cond_id TEXT NOT NULL,
    sql_expr_id TEXT NOT NULL
) WITH ('materialized' = 'true');
