/*
table_dependency(pipeline_id:, table_name:, parent_table_name:) <-
    rule(pipeline_id:, table_name:, rule_id:)
    body_fact(pipeline_id:, rule_id:, table_name: parent_table_name)
    #all_records_inserted(pipeline_id:)
table_dependency(pipeline_id:, table_name:, parent_table_name:) <-
    table_dependency(pipeline_id:, table_name:, parent_table_name:)
    table_dependency(pipeline_id:, table_name: parent_table_name, parent_table_name:)
*/
DECLARE RECURSIVE VIEW table_dependency (pipeline_id TEXT, table_name TEXT, parent_table_name TEXT);
CREATE MATERIALIZED VIEW table_dependency AS
    SELECT DISTINCT
        rule.pipeline_id,
        rule.table_name,
        body_fact.table_name AS parent_table_name
    FROM rule
    JOIN body_fact
        ON rule.pipeline_id = body_fact.pipeline_id
        AND rule.rule_id = body_fact.rule_id
    -- JOIN all_records_inserted
    --     ON rule.pipeline_id = all_records_inserted.pipeline_id

    UNION

    SELECT DISTINCT
        t1.pipeline_id,
        t1.table_name,
        t2.parent_table_name
    FROM table_dependency AS t1
    JOIN table_dependency AS t2
        ON t1.pipeline_id = t2.parent_table_name
        AND t1.parent_table_name = t2.table_name;

/*
table_without_dependencies(pipeline_id:, table_name:) <-
    rule(pipeline_id:, table_name:, rule_id:)
    not body_fact(pipeline_id:, rule_id:)
*/
CREATE MATERIALIZED VIEW table_without_dependencies AS
    SELECT DISTINCT
        rule.pipeline_id,
        rule.table_name
    FROM rule
    WHERE NOT EXISTS (
        SELECT 1
        FROM body_fact
        WHERE rule.pipeline_id = body_fact.pipeline_id
        AND rule.rule_id = body_fact.rule_id
    );

/*
table_output_order(pipeline_id:, table_name:, order: 0) <-
    schema_table(pipeline_id:, table_name:)
table_output_order(pipeline_id:, table_name:, order: 0) <-
    table_without_dependencies(pipeline_id:, table_name:)
table_output_order(pipeline_id:, table_name:, order: max<order>+1) <-
    table_dependency(pipeline_id:, table_name:, parent_table_name:)
    table_output_order(pipeline_id:, table_name: parent_table_name, order:)
    table_name != parent_table_name
*/
DECLARE RECURSIVE VIEW table_output_order (pipeline_id TEXT, table_name TEXT, "order" INTEGER);
CREATE MATERIALIZED VIEW table_output_order AS
    SELECT DISTINCT
        pipeline_id,
        table_name,
        0 AS "order"
    FROM schema_table

    UNION

    SELECT DISTINCT
        table_without_dependencies.pipeline_id,
        table_without_dependencies.table_name,
        0 AS "order"
    FROM table_without_dependencies

    UNION

    SELECT DISTINCT
        table_dependency.pipeline_id,
        table_dependency.table_name,
        MAX(table_output_order."order")+1 AS "order"
    FROM table_output_order
    JOIN table_dependency
        ON table_dependency.pipeline_id = table_output_order.pipeline_id
        AND table_dependency.parent_table_name = table_output_order.table_name
    WHERE table_dependency.parent_table_name != table_dependency.table_name
    GROUP BY table_dependency.pipeline_id, table_dependency.table_name;

/*
first_table(pipeline_id:, table_name: min<table_name>, order:) <-
    table_output_order(pipeline_id:, table_name:, order:)
    #all_records_inserted(pipeline_id:)
*/
CREATE MATERIALIZED VIEW first_table AS
    SELECT DISTINCT
        table_output_order.pipeline_id,
        MIN(table_output_order.table_name) AS table_name,
        table_output_order."order"
    FROM table_output_order
    -- JOIN all_records_inserted
    --     ON table_output_order.pipeline_id = all_records_inserted.pipeline_id
    GROUP BY table_output_order.pipeline_id, table_output_order."order";

/*
only_one_table_in_group(pipeline_id:, order:) <-
    table_output_order(pipeline_id:, table_name:, order:)
    count<> = 1
*/
CREATE MATERIALIZED VIEW only_one_table_in_group AS
    SELECT DISTINCT
        table_output_order.pipeline_id,
        table_output_order."order"
    FROM table_output_order
    GROUP BY table_output_order.pipeline_id, table_output_order."order"
    HAVING COUNT(DISTINCT table_output_order.table_name) = 1;

/*
next_table_within_same_order(pipeline_id:, prev_table_name:, next_table_name:) <-
    table_output_order(pipeline_id:, table_name: prev_table_name, order:)
    table_output_order(pipeline_id:, table_name:, order:)
    next_table_name := min<table_name>
    prev_table_name < table_name
*/
CREATE MATERIALIZED VIEW next_table_within_same_order AS
    SELECT DISTINCT
        prev.pipeline_id,
        prev.table_name AS prev_table_name,
        MIN("next".table_name) AS next_table_name
    FROM table_output_order AS prev
    JOIN table_output_order AS "next"
        ON prev.pipeline_id = "next".pipeline_id
        AND prev."order" = "next"."order"
    WHERE prev.table_name < "next".table_name
    GROUP BY prev.pipeline_id, prev.table_name;

/*
next_table(pipeline_id:, prev_table_name:, next_table_name:) <-
    first_table(pipeline_id:, table_name: prev_table_name, order: 0)
    next_table_within_same_order(pipeline_id:, prev_table_name:, next_table_name:)
next_table(pipeline_id:, prev_table_name:, next_table_name:) <-
    first_table(pipeline_id:, table_name: prev_table_name, order: 0)
    first_table(pipeline_id:, table_name: next_table_name, order: 1)
    not next_table_within_same_order(pipeline_id:, prev_table_name:)
next_table(pipeline_id:, prev_table_name:, next_table_name:) <-
    next_table(pipeline_id:, next_table_name: prev_table_name)
    next_table_within_same_order(pipeline_id:, prev_table_name:, next_table_name:)
next_table(pipeline_id:, prev_table_name:, next_table_name:) <-
    next_table(pipeline_id:, next_table_name: prev_table_name)
    table_output_order(pipeline_id:, table_name: prev_table_name, order:)
    first_table(pipeline_id:, table_name: next_table_name, order: order+1)
    not next_table_within_same_order(pipeline_id:, prev_table_name:)
*/
DECLARE RECURSIVE VIEW next_table (pipeline_id TEXT, prev_table_name TEXT, next_table_name TEXT);
CREATE MATERIALIZED VIEW next_table AS
    SELECT DISTINCT
        first_table.pipeline_id,
        next_table_within_same_order.prev_table_name,
        next_table_within_same_order.next_table_name
    FROM first_table
    JOIN next_table_within_same_order
        ON first_table.pipeline_id = next_table_within_same_order.pipeline_id
        AND first_table.table_name = next_table_within_same_order.prev_table_name
    WHERE first_table."order" = 0
    
    UNION

    SELECT DISTINCT
        prev.pipeline_id,
        prev.table_name AS prev_table_name,
        "next".table_name AS next_table_name
    FROM first_table AS prev
    JOIN first_table AS "next"
        ON prev.pipeline_id = "next".pipeline_id
    WHERE prev."order" = 0 AND "next"."order" = 1
    AND NOT EXISTS (
        SELECT 1
        FROM next_table_within_same_order
        WHERE prev.pipeline_id = next_table_within_same_order.pipeline_id
        AND prev.table_name = next_table_within_same_order.prev_table_name
    )

    UNION

    SELECT DISTINCT
        next_table.pipeline_id,
        next_table_within_same_order.prev_table_name,
        next_table_within_same_order.next_table_name
    FROM next_table
    JOIN next_table_within_same_order
        ON next_table.pipeline_id = next_table_within_same_order.pipeline_id
        AND next_table.next_table_name = next_table_within_same_order.prev_table_name
    
    UNION

    SELECT DISTINCT
        next_table.pipeline_id,
        next_table.next_table_name AS prev_table_name,
        first_table.table_name AS next_table_name
    FROM next_table
    JOIN table_output_order
        ON next_table.pipeline_id = table_output_order.pipeline_id
        AND next_table.next_table_name = table_output_order.table_name
    JOIN first_table
        ON next_table.pipeline_id = first_table.pipeline_id
        AND table_output_order."order"+1 = first_table."order"
    WHERE NOT EXISTS (
        SELECT 1
        FROM next_table_within_same_order
        WHERE next_table.pipeline_id = next_table_within_same_order.pipeline_id
        AND next_table.next_table_name = next_table_within_same_order.prev_table_name
    );

-- /*
-- next_table(pipeline_id:, prev_table_name:, next_table_name:, order:) <-
--     first_table(pipeline_id:, table_name: prev_table_name, order:)
--     not only_one_table_in_group(pipeline_id:, order:)
--     table_output_order(pipeline_id:, table_name:, order:)
--     next_table_name := min<table_name>
--     prev_table_name < table_name
-- next_table(pipeline_id:, prev_table_name:, next_table_name:, order:) <-
--     first_table(pipeline_id:, table_name: prev_table_name, order: prev_order)
--     only_one_table_in_group(pipeline_id:, order: prev_order)
--     order := prev_order+1
--     table_output_order(pipeline_id:, table_name:, order:)
--     next_table_name := min<table_name>
-- next_table(pipeline_id:, prev_table_name:, next_table_name:, order:) <-
--     next_table(pipeline_id:, next_table_name: prev_table_name, order:)
--     not only_one_table_in_group(pipeline_id:, order:)
--     table_output_order(pipeline_id:, table_name:, order:)
--     next_table_name := min<table_name>
--     prev_table_name < table_name
-- next_table(pipeline_id:, prev_table_name:, next_table_name:, order:) <-
--     next_table(pipeline_id:, next_table_name: prev_table_name, order: prev_order)
--     only_one_table_in_group(pipeline_id:, order: prev_order)
--     order := prev_order+1
--     table_output_order(pipeline_id:, table_name:, order:)
--     next_table_name := min<table_name>
-- */
-- DECLARE RECURSIVE VIEW next_table (pipeline_id TEXT, prev_table_name TEXT, next_table_name TEXT, "order" INTEGER);
-- CREATE MATERIALIZED VIEW next_table AS
--     SELECT DISTINCT
--         first_table.pipeline_id,
--         first_table.table_name AS prev_table_name,
--         MIN(table_output_order.table_name) AS next_table_name,
--         first_table."order"
--     FROM first_table
--     JOIN table_output_order
--         ON first_table.pipeline_id = table_output_order.pipeline_id
--         AND first_table."order" = table_output_order."order"
--     WHERE first_table.table_name < table_output_order.table_name
--     AND NOT EXISTS (
--         SELECT 1
--         FROM only_one_table_in_group
--         WHERE first_table.pipeline_id = only_one_table_in_group.pipeline_id
--         AND first_table."order" = only_one_table_in_group."order"
--     )
--     GROUP BY first_table.pipeline_id, first_table.table_name, first_table."order"

--     UNION

--     SELECT DISTINCT
--         first_table.pipeline_id,
--         first_table.table_name AS prev_table_name,
--         MIN(table_output_order.table_name) AS next_table_name,
--         first_table."order"
--     FROM first_table
--     JOIN only_one_table_in_group
--         ON first_table.pipeline_id = only_one_table_in_group.pipeline_id
--         AND first_table."order" = only_one_table_in_group."order"
--     JOIN table_output_order
--         ON first_table.pipeline_id = table_output_order.pipeline_id
--         AND first_table."order"+1 = table_output_order."order"
--     GROUP BY first_table.pipeline_id, first_table.table_name, first_table."order"
    
--     UNION
    
--     SELECT DISTINCT
--         next_table.pipeline_id,
--         next_table.next_table_name AS prev_table_name,
--         MIN(table_output_order.table_name) AS next_table_name,
--         next_table."order"
--     FROM next_table
--     JOIN table_output_order
--         ON next_table.pipeline_id = table_output_order.pipeline_id
--         AND next_table."order" = table_output_order."order"
--     WHERE next_table.next_table_name < table_output_order.table_name
--     AND NOT EXISTS (
--         SELECT 1
--         FROM only_one_table_in_group
--         WHERE next_table.pipeline_id = only_one_table_in_group.pipeline_id
--         AND next_table."order" = only_one_table_in_group."order"
--     )
--     GROUP BY next_table.pipeline_id, next_table.next_table_name, next_table."order"
    
--     UNION
    
--     SELECT DISTINCT
--         next_table.pipeline_id,
--         next_table.next_table_name AS prev_table_name,
--         MIN(table_output_order.table_name) AS next_table_name,
--         next_table."order"
--     FROM next_table
--     JOIN only_one_table_in_group
--         ON next_table.pipeline_id = only_one_table_in_group.pipeline_id
--         AND next_table."order" = only_one_table_in_group."order"
--     JOIN table_output_order
--         ON next_table.pipeline_id = table_output_order.pipeline_id
--         AND next_table."order"+1 = table_output_order."order"
--     GROUP BY next_table.pipeline_id, next_table.next_table_name, next_table."order";

/*
fact_alias(pipeline_id:, rule_id:, table_name:, alias:, negated:, fact_index:) <-
    body_fact(pipeline_id:, rule_id:, fact_id:, table_name:, negated:, index: fact_index)
    alias := `{{fact_id}}:{{table_name}}` 
*/
CREATE MATERIALIZED VIEW fact_alias AS
    SELECT DISTINCT
        body_fact.pipeline_id,
        body_fact.rule_id,
        body_fact.fact_id,
        body_fact."index" AS fact_index,
        body_fact.table_name,
        body_fact.negated,
        (body_fact.fact_id || ':' || body_fact.table_name) AS alias
    FROM body_fact;

/*
first_fact_alias(
    pipeline_id:, rule_id:, table_name: argmin<table_name, fact_index>, alias: argmin<alias, fact_index>,
    negated:, fact_index: min<fact_index>
) <-
    fact_alias(pipeline_id:, rule_id:, table_name:, alias:, negated:, fact_index:)
    #all_records_inserted(pipeline_id:)
*/
CREATE MATERIALIZED VIEW first_fact_alias AS
    SELECT DISTINCT
        fact_alias.pipeline_id,
        fact_alias.rule_id,
        ARG_MIN(fact_alias.fact_id, fact_alias.fact_index) AS fact_id,
        ARG_MIN(fact_alias.table_name, fact_alias.fact_index) AS table_name,
        ARG_MIN(fact_alias.alias, fact_alias.fact_index) AS alias,
        fact_alias.negated,
        MIN(fact_alias.fact_index) AS fact_index
    FROM fact_alias
    -- JOIN all_records_inserted
    --     ON fact_alias.pipeline_id = all_records_inserted.pipeline_id
    GROUP BY fact_alias.pipeline_id, fact_alias.rule_id, fact_alias.negated;

/*
adjacent_facts(
    pipeline_id:, rule_id:, negated:, prev_fact_id:, next_fact_id: argmin<next_fact_id, next_fact_index>
) <-
    fact_alias(pipeline_id:, rule_id:, fact_id: prev_fact_id, negated:, fact_index: prev_fact_index)
    fact_alias(pipeline_id:, rule_id:, fact_id: next_fact_id, negated:, fact_index: next_fact_index)
    prev_fact_index < next_fact_index
*/
CREATE MATERIALIZED VIEW adjacent_facts AS
    SELECT DISTINCT
        prev_fact.pipeline_id,
        prev_fact.rule_id,
        prev_fact.negated,
        prev_fact.fact_id AS prev_fact_id,
        ARG_MIN(next_fact.fact_id, next_fact.fact_index) AS next_fact_id
    FROM fact_alias AS prev_fact
    JOIN fact_alias AS next_fact
        ON prev_fact.pipeline_id = next_fact.pipeline_id
        AND prev_fact.rule_id = next_fact.rule_id
        AND prev_fact.negated = next_fact.negated
    WHERE prev_fact.fact_index < next_fact.fact_index
    GROUP BY prev_fact.pipeline_id, prev_fact.rule_id, prev_fact.negated, prev_fact.fact_id;

/*
table_first_rule(pipeline_id:, table_name:, rule_id: min<rule_id>) <-
    rule(pipeline_id:, table_name:, rule_id:)
    #all_records_inserted(pipeline_id:)
*/
CREATE MATERIALIZED VIEW table_first_rule AS
    SELECT DISTINCT
        rule.pipeline_id,
        rule.table_name,
        MIN(rule.rule_id) AS rule_id
    FROM rule
    -- JOIN all_records_inserted
    --     ON rule.pipeline_id = all_records_inserted.pipeline_id
    GROUP BY rule.pipeline_id, rule.table_name;

/*
table_next_rule(pipeline_id:, table_name:, prev_rule_id:, next_rule_id:) <-
    table_first_rule(pipeline_id:, table_name:, rule_id: prev_rule_id)
    rule(pipeline_id:, table_name:, rule_id:)
    next_rule_id := min<rule_id>
    prev_rule_id < rule_id
table_next_rule(pipeline_id:, table_name:, prev_rule_id:, next_rule_id:) <-
    table_next_rule(pipeline_id:, table_name:, next_rule_id: prev_rule_id)
    rule(pipeline_id:, table_name:, rule_id:)
    next_rule_id := min<rule_id>
    prev_rule_id < rule_id
*/
DECLARE RECURSIVE VIEW table_next_rule (pipeline_id TEXT, table_name TEXT, prev_rule_id TEXT, next_rule_id TEXT);
CREATE MATERIALIZED VIEW table_next_rule AS
    SELECT DISTINCT
        table_first_rule.pipeline_id,
        table_first_rule.table_name,
        table_first_rule.rule_id AS prev_rule_id,
        MIN(rule.rule_id) AS next_rule_id
    FROM table_first_rule
    JOIN rule
        ON rule.pipeline_id = table_first_rule.pipeline_id
        AND rule.table_name = table_first_rule.table_name
    WHERE table_first_rule.rule_id < rule.rule_id
    GROUP BY table_first_rule.pipeline_id, table_first_rule.table_name, table_first_rule.rule_id

    UNION

    SELECT DISTINCT
        table_next_rule.pipeline_id,
        table_next_rule.table_name,
        table_next_rule.next_rule_id AS prev_rule_id,
        MIN(rule.rule_id) AS next_rule_id
    FROM table_next_rule
    JOIN rule
        ON rule.pipeline_id = table_next_rule.pipeline_id
        AND rule.table_name = table_next_rule.table_name
    WHERE table_next_rule.next_rule_id < rule.rule_id
    GROUP BY table_next_rule.pipeline_id, table_next_rule.table_name, table_next_rule.next_rule_id;

/*
array_expr_length(pipeline_id:, rule_id:, expr_id:, length: count<>) <-
    array_expr(pipeline_id:, rule_id:, expr_id:, array_id:)
    array_entry(pipeline_id:, rule_id:, array_id:)
*/
CREATE MATERIALIZED VIEW array_expr_length AS
    SELECT DISTINCT
        array_expr.pipeline_id,
        array_expr.rule_id,
        array_expr.expr_id,
        COUNT(*) AS length
    FROM array_expr
    JOIN array_entry
        ON array_expr.pipeline_id = array_entry.pipeline_id
        AND array_expr.rule_id = array_entry.rule_id
        AND array_expr.array_id = array_entry.array_id
    GROUP BY array_expr.pipeline_id, array_expr.rule_id, array_expr.expr_id;

/*
sql_expr_template_part(pipeline_id:, rule_id:, expr_id:, part:, index:) <-
    sql_expr(pipeline_id:, rule_id:, expr_id:, template:)
    (part, index) <- unnest(template)
*/
CREATE MATERIALIZED VIEW sql_expr_template_part AS
    SELECT DISTINCT
        sql_expr.pipeline_id,
        sql_expr.rule_id,
        sql_expr.expr_id,
        t.part,
        t."index"
    FROM sql_expr
    CROSS JOIN UNNEST(sql_expr.template) WITH ORDINALITY AS t (part, "index");

/*
var_mentioned_in_sql_expr(pipeline_id:, rule_id:, expr_id:, var_name:) <-
    sql_expr(pipeline_id:, rule_id:, expr_id:, template:)
    sql_expr_template_part(pipeline_id:, rule_id:, expr_id:, part:)
    part ~ "{{[a-z_][a-zA-Z0-9_]*}}"
    var_name := part[2:-2]
*/
CREATE MATERIALIZED VIEW var_mentioned_in_sql_expr AS
    SELECT DISTINCT
        sql_expr.pipeline_id,
        sql_expr.rule_id,
        sql_expr.expr_id,
        SUBSTRING(t.part FROM 3 FOR (CHAR_LENGTH(t.part)-4)) AS var_name
    FROM sql_expr
    JOIN sql_expr_template_part AS t
        ON sql_expr.pipeline_id = t.pipeline_id
        AND sql_expr.rule_id = t.rule_id
        AND sql_expr.expr_id = t.expr_id
    WHERE t.part RLIKE '^\{\{[a-zA-Z_][A-Za-z0-9_:]*\}\}$';

/*
var_mentioned_in_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, var_name:, access_prefix: "") <-
    var_expr(pipeline_id:, rule_id:, expr_id:, var_name:)
    expr_type := "var_expr"
var_mentioned_in_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, var_name:, access_prefix: "") <-
    aggr_expr(pipeline_id:, rule_id:, expr_id:, arg_var: var_name)
    expr_type := 'aggr_expr'
var_mentioned_in_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, var_name:, access_prefix: "") <-
    var_mentioned_in_sql_expr(pipeline_id:, rule_id:, expr_id:, var_name:)
    expr_type := 'sql_expr'
var_mentioned_in_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "array_expr", var_name:, access_prefix:) <-
    array_expr(pipeline_id:, rule_id:, expr_id:, array_id:)
    array_entry(pipeline_id:, rule_id:, array_id:, expr_id: entry_expr_id, expr_type: entry_expr_type, index:)
    var_mentioned_in_expr(
        pipeline_id:, rule_id:, expr_id: entry_expr_id, expr_type: entry_expr_type,
        var_name:, access_prefix: prev_access_prefix)
    access_prefix := `[{{index}}]{{prev_access_prefix}}`
var_mentioned_in_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "dict_expr", var_name:, access_prefix:) <-
    dict_expr(pipeline_id:, rule_id:, expr_id:, dict_id:)
    dict_entry(pipeline_id:, rule_id:, dict_id:, key:, expr_id: entry_expr_id, expr_type: entry_expr_type)
    var_mentioned_in_expr(
        pipeline_id:, rule_id:, expr_id: entry_expr_id, expr_type: entry_expr_type,
        var_name:, access_prefix: prev_access_prefix)
    access_prefix := `['{{key}}']{{prev_access_prefix}}`
*/
DECLARE RECURSIVE VIEW var_mentioned_in_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, expr_type TEXT, var_name TEXT, access_prefix TEXT);
CREATE MATERIALIZED VIEW var_mentioned_in_expr AS
    SELECT DISTINCT
        var_expr.pipeline_id,
        var_expr.rule_id,
        var_expr.expr_id,
        'var_expr' AS expr_type,
        var_expr.var_name,
        '' AS access_prefix
    FROM var_expr

    UNION

    SELECT DISTINCT
        aggr_expr.pipeline_id,
        aggr_expr.rule_id,
        aggr_expr.expr_id,
        'aggr_expr' AS expr_type,
        aggr_expr.arg_var AS var_name,
        '' AS access_prefix
    FROM aggr_expr

    UNION

    SELECT DISTINCT
        var_mentioned_in_sql_expr.pipeline_id,
        var_mentioned_in_sql_expr.rule_id,
        var_mentioned_in_sql_expr.expr_id,
        'sql_expr' AS expr_type,
        var_mentioned_in_sql_expr.var_name,
        '' AS access_prefix
    FROM var_mentioned_in_sql_expr

    UNION

    SELECT DISTINCT
        array_expr.pipeline_id,
        array_expr.rule_id,
        array_expr.expr_id,
        CAST('array_expr' AS TEXT) AS expr_type,
        var_mentioned_in_expr.var_name,
        ('[' || array_entry."index" || ']' || var_mentioned_in_expr.access_prefix) AS access_prefix
    FROM array_expr
    JOIN array_entry
        ON array_expr.pipeline_id = array_entry.pipeline_id
        AND array_expr.rule_id = array_entry.rule_id
        AND array_entry.array_id = array_expr.array_id
    JOIN var_mentioned_in_expr
        ON array_expr.pipeline_id = var_mentioned_in_expr.pipeline_id
        AND array_expr.rule_id = var_mentioned_in_expr.rule_id
        AND array_entry.expr_id = var_mentioned_in_expr.expr_id
        AND array_entry.expr_type = var_mentioned_in_expr.expr_type

    UNION

    SELECT DISTINCT
        dict_expr.pipeline_id,
        dict_expr.rule_id,
        dict_expr.expr_id,
        'dict_expr' AS expr_type,
        var_mentioned_in_expr.var_name,
        ('[''' || dict_entry.key || ''']' || var_mentioned_in_expr.access_prefix) AS access_prefix
    FROM dict_expr
    JOIN dict_entry
        ON dict_expr.pipeline_id = dict_entry.pipeline_id
        AND dict_expr.rule_id = dict_entry.rule_id
        AND dict_entry.dict_id = dict_expr.dict_id
    JOIN var_mentioned_in_expr
        ON dict_expr.pipeline_id = var_mentioned_in_expr.pipeline_id
        AND dict_expr.rule_id = var_mentioned_in_expr.rule_id
        AND dict_entry.expr_id = var_mentioned_in_expr.expr_id
        AND dict_entry.expr_type = var_mentioned_in_expr.expr_type;

/*
var_bound_in_fact(pipeline_id:, rule_id:, fact_id:, key:, negated:, var_name:, fact_index:, sql:) <-
    fact_arg(pipeline_id:, fact_id:, expr_id:, expr_type:)
    var_mentioned_in_expr(pipeline_id:, expr_id:, expr_type:, var_name:)
    fact_alias(pipeline_id:, rule_id:, fact_id:, negated:, fact_index:)
    sql := `"{{alias}}"."{{key}}"{{access_prefix}}`
*/
CREATE MATERIALIZED VIEW var_bound_in_fact AS
    SELECT DISTINCT
        fact_arg.pipeline_id,
        fact_alias.rule_id,
        fact_arg.fact_id,
        fact_arg.key,
        fact_alias.negated,
        fact_alias.fact_index,
        var_mentioned_in_expr.var_name,
        ('"' || fact_alias.alias || '"."' || fact_arg.key || '"' || var_mentioned_in_expr.access_prefix) AS sql
    FROM fact_arg
    JOIN var_mentioned_in_expr
        ON fact_arg.pipeline_id = var_mentioned_in_expr.pipeline_id
        AND fact_arg.expr_id = var_mentioned_in_expr.expr_id
        AND fact_arg.expr_type = var_mentioned_in_expr.expr_type
    JOIN fact_alias
        ON fact_arg.pipeline_id = fact_alias.pipeline_id
        AND fact_arg.fact_id = fact_alias.fact_id;

/*
canonical_fact_var_sql(pipeline_id:, rule_id:, var_name:, sql: argmin<sql, fact_index>, fact_index: min<fact_index>) <-
    var_bound_in_fact(pipeline_id:, rule_id:, var_name:, negated: false, sql:, fact_index:)
    #all_records_inserted(pipeline_id:)
*/
CREATE MATERIALIZED VIEW canonical_fact_var_sql AS
    SELECT DISTINCT
        var_bound_in_fact.pipeline_id,
        var_bound_in_fact.rule_id,
        var_bound_in_fact.var_name,
        MIN(var_bound_in_fact.fact_index) AS fact_index,
        ARG_MIN(var_bound_in_fact.sql, var_bound_in_fact.fact_index) AS sql
    FROM var_bound_in_fact
    -- JOIN all_records_inserted
    --     ON var_bound_in_fact.pipeline_id = all_records_inserted.pipeline_id
    WHERE NOT var_bound_in_fact.negated
    GROUP BY var_bound_in_fact.pipeline_id, var_bound_in_fact.rule_id, var_bound_in_fact.var_name;

/*
match_var_dependency(pipeline_id:, rule_id:, var_name:, parent_var_name:) <-
    body_match(pipeline_id:, rule_id:, match_id:, left_expr_id:, left_expr_type:, right_expr_id:, right_expr_type:)
    var_mentioned_in_expr(pipeline_id:, expr_id: left_expr_id, expr_type: left_expr_type, var_name:)
    var_mentioned_in_expr(pipeline_id:, expr_id: right_expr_id, expr_type: right_expr_type, var_name: parent_var_name)
    #all_records_inserted(pipeline_id:)
    not var_bound_in_fact(pipeline_id:, rule_id:, var_name:, negated: false)
match_var_dependency(pipeline_id:, rule_id:, var_name:, parent_var_name:) <-
    match_var_dependency(pipeline_id:, rule_id:, var_name:, parent_var_name: middle_var_name)
    match_var_dependency(pipeline_id:, rule_id:, var_name: middle_var_name, parent_var_name:)
*/
DECLARE RECURSIVE VIEW match_var_dependency (pipeline_id TEXT, rule_id TEXT, var_name TEXT, parent_var_name TEXT);
CREATE MATERIALIZED VIEW match_var_dependency AS
    SELECT DISTINCT
        body_match.pipeline_id,
        body_match.rule_id,
        l.var_name,
        r.var_name AS parent_var_name
    FROM body_match
    JOIN var_mentioned_in_expr AS l
        ON body_match.pipeline_id = l.pipeline_id
        AND body_match.left_expr_id = l.expr_id
        AND body_match.left_expr_type = l.expr_type
    JOIN var_mentioned_in_expr AS r
        ON body_match.pipeline_id = r.pipeline_id
        AND body_match.right_expr_id = r.expr_id
        AND body_match.right_expr_type = r.expr_type
    -- JOIN all_records_inserted
    --     ON body_match.pipeline_id = all_records_inserted.pipeline_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM var_bound_in_fact
        WHERE var_bound_in_fact.pipeline_id = body_match.pipeline_id
        AND var_bound_in_fact.rule_id = body_match.rule_id
        AND var_bound_in_fact.var_name = l.var_name
        AND NOT var_bound_in_fact.negated)

    UNION

    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.var_name,
        b.parent_var_name
    FROM match_var_dependency AS a
    JOIN match_var_dependency AS b
        ON a.pipeline_id = b.pipeline_id
        AND a.rule_id = b.rule_id
        AND a.parent_var_name = b.var_name;
