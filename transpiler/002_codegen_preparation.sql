/*
table_dependency(pipeline_id:, table_name:, parent_table_name:) <-
    rule(pipeline_id:, table_name:, rule_id:)
    body_goal(pipeline_id:, rule_id:, table_name: parent_table_name)
table_dependency(pipeline_id:, table_name:, parent_table_name:) <-
    table_dependency(pipeline_id:, table_name:, parent_table_name:)
    table_dependency(pipeline_id:, table_name: parent_table_name, parent_table_name:)
*/
DECLARE RECURSIVE VIEW table_dependency (pipeline_id TEXT, table_name TEXT, parent_table_name TEXT);
CREATE MATERIALIZED VIEW table_dependency AS
    SELECT DISTINCT
        rule.pipeline_id,
        rule.table_name,
        body_goal.table_name AS parent_table_name
    FROM rule
    JOIN body_goal
        ON rule.pipeline_id = body_goal.pipeline_id
        AND rule.rule_id = body_goal.rule_id
    
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
table_output_order(pipeline_id:, table_name:, order: 0) <-
    schema_table(pipeline_id:, table_name:)
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
goal_alias(pipeline_id:, rule_id:, table_name:, alias:, negated:, goal_index:) <-
    body_goal(pipeline_id:, rule_id:, goal_id:, table_name:, negated:, index: goal_index)
    alias := `{{goal_id}}:{{table_name}}` 
*/
CREATE MATERIALIZED VIEW goal_alias AS
    SELECT DISTINCT
        body_goal.pipeline_id,
        body_goal.rule_id,
        body_goal.goal_id,
        body_goal."index" AS goal_index,
        body_goal.table_name,
        body_goal.negated,
        (body_goal.goal_id || ':' || body_goal.table_name) AS alias
    FROM body_goal;

/*
first_goal_alias(
    pipeline_id:, rule_id:, table_name: argmin<table_name, goal_index>, alias: argmin<alias, goal_index>,
    negated:, goal_index: min<goal_index>
) <-
    goal_alias(pipeline_id:, rule_id:, table_name:, alias:, negated:, goal_index:)
*/
CREATE MATERIALIZED VIEW first_goal_alias AS
    SELECT DISTINCT
        goal_alias.pipeline_id,
        goal_alias.rule_id,
        ARG_MIN(goal_alias.goal_id, goal_alias.goal_index) AS goal_id,
        ARG_MIN(goal_alias.table_name, goal_alias.goal_index) AS table_name,
        ARG_MIN(goal_alias.alias, goal_alias.goal_index) AS alias,
        goal_alias.negated,
        MIN(goal_alias.goal_index) AS goal_index
    FROM goal_alias
    GROUP BY goal_alias.pipeline_id, goal_alias.rule_id, goal_alias.negated;

/*
adjacent_goals(
    pipeline_id:, rule_id:, negated:, prev_goal_id:, next_goal_id: argmin<next_goal_id, next_goal_index>
) <-
    goal_alias(pipeline_id:, rule_id:, goal_id: prev_goal_id, negated:, goal_index: prev_goal_index)
    goal_alias(pipeline_id:, rule_id:, goal_id: next_goal_id, negated:, goal_index: next_goal_index)
    prev_goal_index < next_goal_index
*/
CREATE MATERIALIZED VIEW adjacent_goals AS
    SELECT DISTINCT
        prev_goal.pipeline_id,
        prev_goal.rule_id,
        prev_goal.negated,
        prev_goal.goal_id AS prev_goal_id,
        ARG_MIN(next_goal.goal_id, next_goal.goal_index) AS next_goal_id
    FROM goal_alias AS prev_goal
    JOIN goal_alias AS next_goal
        ON prev_goal.pipeline_id = next_goal.pipeline_id
        AND prev_goal.rule_id = next_goal.rule_id
        AND prev_goal.negated = next_goal.negated
    WHERE prev_goal.goal_index < next_goal.goal_index
    GROUP BY prev_goal.pipeline_id, prev_goal.rule_id, prev_goal.negated, prev_goal.goal_id;

/*
table_first_rule(pipeline_id:, table_name:, rule_id: min<rule_id>) <-
    rule(pipeline_id:, table_name:, rule_id:)
*/
CREATE MATERIALIZED VIEW table_first_rule AS
    SELECT DISTINCT
        rule.pipeline_id,
        rule.table_name,
        MIN(rule.rule_id) AS rule_id
    FROM rule
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
var_bound_in_goal(pipeline_id:, rule_id:, goal_id:, key:, negated:, var_name:, goal_index:, sql:) <-
    goal_arg(pipeline_id:, goal_id:, expr_id:, expr_type:)
    var_mentioned_in_expr(pipeline_id:, expr_id:, expr_type:, var_name:)
    goal_alias(pipeline_id:, rule_id:, goal_id:, negated:, goal_index:)
    sql := `"{{alias}}"."{{key}}"{{access_prefix}}`
*/
CREATE MATERIALIZED VIEW var_bound_in_goal AS
    SELECT DISTINCT
        goal_arg.pipeline_id,
        goal_alias.rule_id,
        goal_arg.goal_id,
        goal_arg.key,
        goal_alias.negated,
        goal_alias.goal_index,
        var_mentioned_in_expr.var_name,
        ('"' || goal_alias.alias || '"."' || goal_arg.key || '"' || var_mentioned_in_expr.access_prefix) AS sql
    FROM goal_arg
    JOIN var_mentioned_in_expr
        ON goal_arg.pipeline_id = var_mentioned_in_expr.pipeline_id
        AND goal_arg.expr_id = var_mentioned_in_expr.expr_id
        AND goal_arg.expr_type = var_mentioned_in_expr.expr_type
    JOIN goal_alias
        ON goal_arg.pipeline_id = goal_alias.pipeline_id
        AND goal_arg.goal_id = goal_alias.goal_id;

/*
canonical_goal_var_sql(pipeline_id:, rule_id:, var_name:, sql: argmin<sql, goal_index>, goal_index: min<goal_index>) <-
    var_bound_in_goal(pipeline_id:, rule_id:, var_name:, negated: false, sql:, goal_index:)
*/
CREATE MATERIALIZED VIEW canonical_goal_var_sql AS
    SELECT DISTINCT
        var_bound_in_goal.pipeline_id,
        var_bound_in_goal.rule_id,
        var_bound_in_goal.var_name,
        MIN(var_bound_in_goal.goal_index) AS goal_index,
        ARG_MIN(var_bound_in_goal.sql, var_bound_in_goal.goal_index) AS sql
    FROM var_bound_in_goal
    WHERE NOT var_bound_in_goal.negated
    GROUP BY var_bound_in_goal.pipeline_id, var_bound_in_goal.rule_id, var_bound_in_goal.var_name;

/*
match_var_dependency(pipeline_id:, rule_id:, var_name:, parent_var_name:) <-
    body_match(pipeline_id:, rule_id:, match_id:, left_expr_id:, left_expr_type:, right_expr_id:, right_expr_type:)
    var_mentioned_in_expr(pipeline_id:, expr_id: left_expr_id, expr_type: left_expr_type, var_name:)
    var_mentioned_in_expr(pipeline_id:, expr_id: right_expr_id, expr_type: right_expr_type, var_name: parent_var_name)
    not var_bound_in_goal(pipeline_id:, rule_id:, var_name:, negated: false)
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
    WHERE NOT EXISTS (
        SELECT 1
        FROM var_bound_in_goal
        WHERE var_bound_in_goal.pipeline_id = body_match.pipeline_id
        AND var_bound_in_goal.rule_id = body_match.rule_id
        AND var_bound_in_goal.var_name = l.var_name
        AND NOT var_bound_in_goal.negated)

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
