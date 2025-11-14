/*
aggr_fn_name(fn_name: "count", sql_fn_name: "COUNT")
*/
CREATE MATERIALIZED VIEW aggr_fn_name AS
    SELECT 'count' AS fn_name, 'COUNT' AS sql_fn_name;
