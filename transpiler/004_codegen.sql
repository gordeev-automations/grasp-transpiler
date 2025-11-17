
/*
# same variable in the same rule can be bound several times.
# we select one binding as canonical, and use it for grouping and return expressions.
*/

/*
canonical_var_bound_sql(pipeline_id:, rule_id:, var_name:, sql:, aggregated: false) <-
    canonical_fact_var_sql(pipeline_id:, rule_id:, var_name:, sql:)
canonical_var_bound_sql(pipeline_id:, rule_id:, var_name:, sql: min<sql>, aggregated: some<aggregated>) <-
    var_bound_via_match(pipeline_id:, rule_id:, var_name:, sql:, aggregated:)
    not canonical_fact_var_sql(pipeline_id:, rule_id:, var_name:)
*/
DECLARE RECURSIVE VIEW var_bound_via_match (pipeline_id TEXT, rule_id TEXT, match_id TEXT, var_name TEXT, sql TEXT, aggregated BOOLEAN);
DECLARE RECURSIVE VIEW canonical_var_bound_sql (pipeline_id TEXT, rule_id TEXT, var_name TEXT, sql TEXT, aggregated BOOLEAN);
CREATE MATERIALIZED VIEW canonical_var_bound_sql AS
    SELECT DISTINCT
        canonical_fact_var_sql.pipeline_id,
        canonical_fact_var_sql.rule_id,
        canonical_fact_var_sql.var_name,
        canonical_fact_var_sql.sql,
        false AS aggregated
    FROM canonical_fact_var_sql

    UNION

    SELECT DISTINCT
        var_bound_via_match.pipeline_id,
        var_bound_via_match.rule_id,
        var_bound_via_match.var_name,
        MIN(var_bound_via_match.sql) AS sql,
        SOME(var_bound_via_match.aggregated) AS aggregated
    FROM var_bound_via_match
    WHERE NOT EXISTS (
        SELECT 1
        FROM canonical_fact_var_sql
        WHERE canonical_fact_var_sql.pipeline_id = var_bound_via_match.pipeline_id
        AND canonical_fact_var_sql.rule_id = var_bound_via_match.rule_id
        AND canonical_fact_var_sql.var_name = var_bound_via_match.var_name
    )
    GROUP BY var_bound_via_match.pipeline_id, var_bound_via_match.rule_id, var_bound_via_match.var_name;

/*
sql_expr_template_part_with_substitution(pipeline_id:, rule_id:, expr_id:, part:, index:) <-
    sql_expr_template_part(pipeline_id:, rule_id:, expr_id:, part: source_part, index:)
    source_part ~ "{{[a-z_][a-zA-Z0-9_]*}}"
    var_name := part[2:-2]
    canonical_var_bound_sql(pipeline_id:, rule_id:, var_name:, sql: part)
sql_expr_template_part_with_substitution(pipeline_id:, rule_id:, expr_id:, part:, index:) <-
    sql_expr_template_part(pipeline_id:, rule_id:, expr_id:, part:, index:)
    not (part ~ "{{[a-z_][a-zA-Z0-9_]*}}")
*/
DECLARE RECURSIVE VIEW sql_expr_template_part_with_substitution (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, part TEXT, "index" INTEGER);
CREATE MATERIALIZED VIEW sql_expr_template_part_with_substitution AS
    SELECT DISTINCT
        sql_expr_template_part.pipeline_id,
        sql_expr_template_part.rule_id,
        sql_expr_template_part.expr_id,
        canonical_var_bound_sql.sql AS part,
        sql_expr_template_part."index"
    FROM sql_expr_template_part
    JOIN canonical_var_bound_sql
        ON sql_expr_template_part.pipeline_id = canonical_var_bound_sql.pipeline_id
        AND sql_expr_template_part.rule_id = canonical_var_bound_sql.rule_id
        AND SUBSTRING(sql_expr_template_part.part FROM 3 FOR (CHAR_LENGTH(sql_expr_template_part.part)-4)) = canonical_var_bound_sql.var_name
    WHERE sql_expr_template_part.part RLIKE '^\{\{[a-zA-Z_][A-Za-z0-9_:]*\}\}$'

    UNION

    SELECT DISTINCT
        sql_expr_template_part.pipeline_id,
        sql_expr_template_part.rule_id,
        sql_expr_template_part.expr_id,
        sql_expr_template_part.part,
        sql_expr_template_part."index"
    FROM sql_expr_template_part
    WHERE NOT (sql_expr_template_part.part RLIKE '^\{\{[a-zA-Z_][A-Za-z0-9_:]*\}\}$');

/*
sql_expr_substitution_status(pipeline_id:, rule_id:, expr_id:, count: count<>) <-
    sql_expr_template_part_with_substitution(pipeline_id:, rule_id:, expr_id:, index:)
*/
DECLARE RECURSIVE VIEW sql_expr_substitution_status (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, count BIGINT);
CREATE MATERIALIZED VIEW sql_expr_substitution_status AS
    SELECT DISTINCT
        t.pipeline_id,
        t.rule_id,
        t.expr_id,
        COUNT(*) AS count
    FROM sql_expr_template_part_with_substitution AS t
    GROUP BY t.pipeline_id, t.rule_id, t.expr_id;

/*
sql_expr_all_vars_are_bound(pipeline_id:, rule_id:, expr_id:) <-
    sql_expr(pipeline_id:, rule_id:, expr_id:, template:)
    sql_expr_substitution_status(pipeline_id:, rule_id:, expr_id:, count:)
    count = array_length(template)
*/
DECLARE RECURSIVE VIEW sql_expr_all_vars_are_bound (pipeline_id TEXT, rule_id TEXT, expr_id TEXT);
CREATE MATERIALIZED VIEW sql_expr_all_vars_are_bound AS
    SELECT DISTINCT
        sql_expr.pipeline_id,
        sql_expr.rule_id,
        sql_expr.expr_id
    FROM sql_expr
    JOIN sql_expr_substitution_status
        ON sql_expr.pipeline_id = sql_expr_substitution_status.pipeline_id
        AND sql_expr.rule_id = sql_expr_substitution_status.rule_id
        AND sql_expr.expr_id = sql_expr_substitution_status.expr_id
    WHERE sql_expr_substitution_status.count = ARRAY_LENGTH(sql_expr.template);

/*
substituted_sql_expr(pipeline_id:, rule_id:, expr_id:, sql:) <-
    sql_expr_all_vars_are_bound(pipeline_id:, rule_id:, expr_id:)
    sql_expr_template_part_with_substitution(pipeline_id:, rule_id:, expr_id:, part:, index:)
    sql := join(array<part, order_by: [index]>, "")
*/
DECLARE RECURSIVE VIEW substituted_sql_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, sql TEXT);
CREATE MATERIALIZED VIEW substituted_sql_expr AS
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        ARRAY_TO_STRING(ARRAY_AGG(b.part ORDER BY b."index"), '') AS sql
    FROM sql_expr_all_vars_are_bound AS a
    JOIN sql_expr_template_part_with_substitution AS b
        ON a.pipeline_id = b.pipeline_id
        AND a.rule_id = b.rule_id
        AND a.expr_id = b.expr_id
    GROUP BY a.pipeline_id, a.rule_id, a.expr_id;

/*
substituted_aggr_expr(pipeline_id:, rule_id:, expr_id:, sql:) <-
    aggr_expr(pipeline_id:, rule_id:, expr_id:, fn_name: "count", arg_var: NULL)
    sql := "COUNT(*)"
substituted_aggr_expr(pipeline_id:, rule_id:, expr_id:, sql:) <-
    aggr_expr(pipeline_id:, rule_id:, expr_id:, fn_name:, arg_var: var_name)
    canonical_var_bound_sql(pipeline_id:, rule_id:, var_name:, sql: var_sql)
    aggr_fn_name(fn_name:, sql_fn_name:)
    sql := `{{sql_fn_name}}({{var_sql}})`
*/
DECLARE RECURSIVE VIEW substituted_aggr_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, sql TEXT);
CREATE MATERIALIZED VIEW substituted_aggr_expr AS
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        'COUNT(*)' AS sql
    FROM aggr_expr AS a
    WHERE a.fn_name = 'count' AND a.arg_var IS NULL

    UNION

    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        (aggr_fn_name.sql_fn_name || '(' || canonical_var_bound_sql.sql || ')') AS sql
    FROM aggr_expr AS a
    JOIN canonical_var_bound_sql
        ON a.pipeline_id = canonical_var_bound_sql.pipeline_id
        AND a.rule_id = canonical_var_bound_sql.rule_id
        AND a.arg_var = canonical_var_bound_sql.var_name
    JOIN aggr_fn_name
        ON a.fn_name = aggr_fn_name.fn_name;

DECLARE RECURSIVE VIEW substituted_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, expr_type TEXT, sql TEXT, aggregated BOOLEAN);

/*
substituted_array_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated: some<aggregated>) <-
    array_expr(pipeline_id:, rule_id:, expr_id:, array_id:)
    array_entry(
        pipeline_id:, rule_id:, array_id:, index:,
        expr_id: element_expr_id, expr_type: element_expr_type)
    substituted_expr(
        pipeline_id:, rule_id:, expr_id: element_expr_id, expr_type: element_expr_type,
        sql: element_sql, aggregated:)
    sql := "ARRAY[" ++ join(array<element_sql, order_by: [index]>, ", ") ++ "]"
*/
DECLARE RECURSIVE VIEW substituted_array_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, sql TEXT, aggregated BOOLEAN);
CREATE MATERIALIZED VIEW substituted_array_expr AS
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        ('ARRAY[' || ARRAY_TO_STRING(ARRAY_AGG(c.sql ORDER BY b."index"), ', ') || ']') AS sql,
        SOME(c.aggregated) AS aggregated
    FROM array_expr AS a
    JOIN array_entry AS b
        ON a.pipeline_id = b.pipeline_id
        AND a.rule_id = b.rule_id
        AND a.array_id = b.array_id
    JOIN substituted_expr AS c
        ON b.pipeline_id = c.pipeline_id
        AND b.rule_id = c.rule_id
        AND b.expr_id = c.expr_id
        AND b.expr_type = c.expr_type
    GROUP BY a.pipeline_id, a.rule_id, a.expr_id;

/*
substituted_dict_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated: some<aggregated>) <-
    dict_expr(pipeline_id:, rule_id:, expr_id:, dict_id:)
    dict_entry(pipeline_id:, rule_id:, dict_id:, key:, expr_id: value_expr_id, expr_type: value_expr_type)
    substituted_expr(pipeline_id:, rule_id:, expr_id: value_expr_id, expr_type: value_expr_type, sql: value_sql, aggregated:)
    sql := "MAP[" ++ join(array<`'{{key}}', {{value_sql}}`>, ", ") ++ "]"
*/
DECLARE RECURSIVE VIEW substituted_dict_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, sql TEXT, aggregated BOOLEAN);
CREATE MATERIALIZED VIEW substituted_dict_expr AS
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        ('MAP[' || ARRAY_TO_STRING(ARRAY_AGG('''' || b.key || '''' || ', ' || c.sql), ', ') || ']') AS sql,
        SOME(c.aggregated) AS aggregated
    FROM dict_expr AS a
    JOIN dict_entry AS b
        ON a.pipeline_id = b.pipeline_id
        AND a.rule_id = b.rule_id
        AND a.dict_id = b.dict_id
    JOIN substituted_expr AS c
        ON b.pipeline_id = c.pipeline_id
        AND b.rule_id = c.rule_id
        AND b.expr_id = c.expr_id
        AND b.expr_type = c.expr_type
    GROUP BY a.pipeline_id, a.rule_id, a.expr_id;

/*
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "sql_expr", sql:, aggregated: false) <-
    substituted_sql_expr(pipeline_id:, rule_id:, expr_id:, sql:)
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "int_expr", sql:, aggregated: false) <-
    int_expr(pipeline_id:, rule_id:, expr_id:, value:)
    sql := string(value)
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "str_expr", sql:, aggregated: false) <-
    str_expr(pipeline_id:, rule_id:, expr_id:, value:)
    sql := `'{{value}}'`
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "var_expr", sql:, aggregated:) <-
    var_expr(pipeline_id:, rule_id:, expr_id:, var_name:)
    canonical_var_bound_sql(pipeline_id:, rule_id:, var_name:, sql:, aggregated:)
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "aggr_expr", sql:, aggregated: true) <-
    substituted_aggr_expr(pipeline_id:, rule_id:, expr_id:, sql:)
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "array_expr", sql:, aggregated:) <-
    substituted_array_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated:)
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "dict_expr", sql:, aggregated:) <-
    substituted_dict_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated:)
*/
CREATE MATERIALIZED VIEW substituted_expr AS
    SELECT a.pipeline_id, a.rule_id, a.expr_id, CAST('sql_expr' AS TEXT) AS expr_type, a.sql, false AS aggregated
    FROM substituted_sql_expr AS a
    
    UNION
    
    SELECT b.pipeline_id, b.rule_id, b.expr_id, 'int_expr' AS expr_type, b.value AS sql, false AS aggregated
    FROM int_expr AS b

    UNION

    SELECT c.pipeline_id, c.rule_id, c.expr_id, 'str_expr' AS expr_type, ('''' || c.value || '''') AS sql, false AS aggregated
    FROM str_expr AS c

    UNION

    SELECT d.pipeline_id, d.rule_id, d.expr_id, 'var_expr' AS expr_type, canonical_var_bound_sql.sql, canonical_var_bound_sql.aggregated
    FROM var_expr AS d
    JOIN canonical_var_bound_sql
        ON d.pipeline_id = canonical_var_bound_sql.pipeline_id
        AND d.rule_id = canonical_var_bound_sql.rule_id
        AND d.var_name = canonical_var_bound_sql.var_name

    UNION

    SELECT e.pipeline_id, e.rule_id, e.expr_id, 'aggr_expr' AS expr_type, e.sql, true AS aggregated
    FROM substituted_aggr_expr AS e

    UNION

    SELECT f.pipeline_id, f.rule_id, f.expr_id, 'array_expr' AS expr_type, f.sql, f.aggregated
    FROM substituted_array_expr AS f

    UNION

    SELECT g.pipeline_id, g.rule_id, g.expr_id, 'dict_expr' AS expr_type, g.sql, g.aggregated
    FROM substituted_dict_expr AS g;


/*
match_right_expr_sql(pipeline_id:, rule_id:, match_id:, sql:, aggregated:) <-
    body_match(pipeline_id:, rule_id:, match_id:, right_expr_id:, right_expr_type:)
    substituted_expr(pipeline_id:, rule_id:, expr_id: right_expr_id, expr_type: right_expr_type, sql:, aggregated:)
*/
DECLARE RECURSIVE VIEW match_right_expr_sql (pipeline_id TEXT, rule_id TEXT, match_id TEXT, sql TEXT, aggregated BOOLEAN);
CREATE MATERIALIZED VIEW match_right_expr_sql AS
    SELECT DISTINCT a.pipeline_id, a.rule_id, a.match_id, b.sql, b.aggregated
    FROM body_match AS a
    JOIN substituted_expr AS b
        ON a.pipeline_id = b.pipeline_id
        AND a.rule_id = b.rule_id
        AND a.right_expr_id = b.expr_id
        AND a.right_expr_type = b.expr_type;

/*
var_bound_via_match(pipeline_id:, rule_id:, match_id:, var_name:, sql:, aggregated:) <-
    body_match(pipeline_id:, rule_id:, match_id:, left_expr_id:, left_expr_type:)
    var_mentioned_in_expr(pipeline_id:, expr_id: left_expr_id, expr_type: left_expr_type, var_name:, access_prefix:)
    match_right_expr_sql(pipeline_id:, rule_id:, match_id:, sql: right_sql, aggregated:)
    sql := `{{right_sql}}{{access_prefix}}`
var_bound_via_match(pipeline_id:, rule_id:, match_id: NULL, var_name:, sql:, aggregated:) <-
    rule_param(pipeline_id:, rule_id:, key: var_name, expr_id:, expr_type:)
    substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql:, aggregated:)
*/
CREATE MATERIALIZED VIEW var_bound_via_match AS
    SELECT DISTINCT
        body_match.pipeline_id,
        body_match.rule_id,
        body_match.match_id,
        var_mentioned_in_expr.var_name,
        (match_right_expr_sql.sql || var_mentioned_in_expr.access_prefix) AS sql,
        match_right_expr_sql.aggregated
    FROM body_match
    JOIN var_mentioned_in_expr
        ON body_match.pipeline_id = var_mentioned_in_expr.pipeline_id
        AND body_match.rule_id = var_mentioned_in_expr.rule_id
        AND body_match.left_expr_id = var_mentioned_in_expr.expr_id
        AND body_match.left_expr_type = var_mentioned_in_expr.expr_type
    JOIN match_right_expr_sql
        ON body_match.pipeline_id = match_right_expr_sql.pipeline_id
        AND body_match.rule_id = match_right_expr_sql.rule_id
        AND body_match.match_id = match_right_expr_sql.match_id
    
    UNION
    
    SELECT DISTINCT
        rule_param.pipeline_id,
        rule_param.rule_id,
        NULL AS match_id,
        rule_param.key AS var_name,
        substituted_expr.sql AS sql,
        substituted_expr.aggregated
    FROM rule_param
    JOIN substituted_expr
        ON rule_param.pipeline_id = substituted_expr.pipeline_id
        AND rule_param.rule_id = substituted_expr.rule_id
        AND rule_param.expr_id = substituted_expr.expr_id
        AND rule_param.expr_type = substituted_expr.expr_type;

/*
neg_fact_where_cond(pipeline_id:, rule_id:, fact_id:, sql_lines:) <-
    fact_alias(pipeline_id:, rule_id:, fact_id:, alias:, table_name:, negated: true)
    fact_arg(pipeline_id:, rule_id:, fact_id:, key:, expr_id:, expr_type:)
    substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql: expr_sql)
    [first_cond, *rest_cond] := array<`"{{alias}}"."{{key}}" = {{expr_sql}}`>
    sql_lines := [
        "NOT EXISTS (SELECT 1",
        `FROM "{{table_name}}" AS "{{alias}}"`,
        `WHERE {{first_cond}}`,
        *map(rest_cond, x -> `AND {{x}}`),
        ")",
    ]
*/
CREATE MATERIALIZED VIEW neg_fact_where_cond AS
    SELECT DISTINCT
        fact_alias.pipeline_id,
        fact_alias.rule_id,
        fact_alias.fact_id,
        ARRAY_CONCAT(
            ARRAY[
                'NOT EXISTS (SELECT 1',
                ('FROM "' || fact_alias.table_name || '" AS "' || fact_alias.alias || '"'),
                ('WHERE ' || ARRAY_AGG('"' || fact_alias.alias || '"."' || fact_arg.key || '" = ' || substituted_expr.sql)[1])
            ],
            TRANSFORM(
                TEXT_ARRAY_DROP_LEFT(
                    ARRAY_AGG('"' || fact_alias.alias || '"."' || fact_arg.key || '" = ' || substituted_expr.sql),
                    CAST(1 AS INTEGER UNSIGNED)),
                x -> 'AND ' || x)
        ) AS sql_lines
    FROM fact_alias
    JOIN fact_arg
        ON fact_alias.pipeline_id = fact_arg.pipeline_id
        AND fact_alias.rule_id = fact_arg.rule_id
        AND fact_alias.fact_id = fact_arg.fact_id
    JOIN substituted_expr
        ON fact_arg.pipeline_id = substituted_expr.pipeline_id
        AND fact_arg.rule_id = substituted_expr.rule_id
        AND fact_arg.expr_id = substituted_expr.expr_id
        AND fact_arg.expr_type = substituted_expr.expr_type
    WHERE fact_alias.negated
    GROUP BY fact_alias.pipeline_id, fact_alias.rule_id, fact_alias.fact_id, fact_alias.alias, fact_alias.table_name;

/*
sql_where_cond(pipeline_id:, rule_id:, cond_id:, sql:) <-
    body_sql_cond(pipeline_id:, rule_id:, cond_id:, sql_expr_id: expr_id)
    substituted_sql_expr(pipeline_id:, rule_id:, expr_id:, sql:)
*/
CREATE MATERIALIZED VIEW sql_where_cond AS
    SELECT DISTINCT
        body_sql_cond.pipeline_id,
        body_sql_cond.rule_id,
        body_sql_cond.cond_id,
        substituted_sql_expr.sql
    FROM body_sql_cond
    JOIN substituted_sql_expr
        ON body_sql_cond.pipeline_id = substituted_sql_expr.pipeline_id
        AND body_sql_cond.rule_id = substituted_sql_expr.rule_id
        AND body_sql_cond.sql_expr_id = substituted_sql_expr.expr_id;

/*
substituted_match_left_expr_with_right_expr(pipeline_id:, rule_id:, match_id:, expr_id:, expr_type_id:, sql:) <-
    body_match(
        pipeline_id:, rule_id:, match_id:, left_expr_id: expr_id, left_expr_type: expr_type,
        right_expr_id:, right_expr_type:)
    substituted_expr(pipeline_id:, rule_id:, expr_id: right_expr_id, expr_type: right_expr_type, sql:)
substituted_match_left_expr_with_right_expr(pipeline_id:, rule_id:, match_id:, expr_id:, expr_type_id:, sql:) <-
    substituted_match_left_expr_with_right_expr(
        pipeline_id:, rule_id:, match_id:, expr_id: right_expr_id, expr_type: right_expr_type,
        sql: right_expr_sql)
    right_expr_type = 'array_expr'
    array_expr(pipeline_id:, rule_id:, expr_id: right_expr_id, array_id:)
    array_entry(pipeline_id:, rule_id:, array_id:, index:, expr_id:, expr_type:)
    sql := `{{right_expr_sql}}[{{index}}]`
substituted_match_left_expr_with_right_expr(pipeline_id:, rule_id:, match_id:, expr_id:, expr_type_id:, sql:) <-
    substituted_match_left_expr_with_right_expr(
        pipeline_id:, rule_id:, match_id:, expr_id: right_expr_id, expr_type: right_expr_type,
        sql: right_expr_sql)
    right_expr_type = 'dict_expr'
    dict_expr(pipeline_id:, rule_id:, expr_id: right_expr_id, dict_id:)
    dict_entry(pipeline_id:, rule_id:, array_id:, key:, expr_id:, expr_type:)
    sql := `{{right_expr_sql}}['{{key}}']`
*/
DECLARE RECURSIVE VIEW substituted_match_left_expr_with_right_expr (pipeline_id TEXT, rule_id TEXT, match_id TEXT, expr_id TEXT, expr_type TEXT, sql TEXT);
CREATE MATERIALIZED VIEW substituted_match_left_expr_with_right_expr AS
    SELECT DISTINCT
        body_match.pipeline_id,
        body_match.rule_id,
        body_match.match_id,
        body_match.left_expr_id AS expr_id,
        body_match.left_expr_type AS expr_type,
        substituted_expr.sql
    FROM body_match
    JOIN substituted_expr
        ON substituted_expr.pipeline_id = body_match.pipeline_id
        AND substituted_expr.rule_id = body_match.rule_id
        AND substituted_expr.expr_id = body_match.right_expr_id
        AND substituted_expr.expr_type = body_match.right_expr_type
    
    UNION
    
    SELECT DISTINCT
        array_entry.pipeline_id,
        array_entry.rule_id,
        substituted_match_left_expr_with_right_expr.match_id,
        array_entry.expr_id,
        array_entry.expr_type,
        (substituted_match_left_expr_with_right_expr.sql || '[' || array_entry."index" || ']') AS sql
    FROM substituted_match_left_expr_with_right_expr
    JOIN array_expr
        ON array_expr.pipeline_id = substituted_match_left_expr_with_right_expr.pipeline_id
        AND array_expr.rule_id = substituted_match_left_expr_with_right_expr.rule_id
        AND array_expr.expr_id = substituted_match_left_expr_with_right_expr.expr_id
    JOIN array_entry
        ON array_expr.pipeline_id = array_entry.pipeline_id
        AND array_expr.rule_id = array_entry.rule_id
        AND array_expr.array_id = array_entry.array_id
    WHERE substituted_match_left_expr_with_right_expr.expr_type = 'array_expr'

    UNION

    SELECT DISTINCT
        dict_entry.pipeline_id,
        dict_entry.rule_id,
        substituted_match_left_expr_with_right_expr.match_id,
        dict_entry.expr_id,
        dict_entry.expr_type,
        (substituted_match_left_expr_with_right_expr.sql || '[' || '''' || dict_entry.key || '''' || ']') AS sql
    FROM substituted_match_left_expr_with_right_expr
    JOIN dict_expr
        ON dict_expr.pipeline_id = substituted_match_left_expr_with_right_expr.pipeline_id
        AND dict_expr.rule_id = substituted_match_left_expr_with_right_expr.rule_id
        AND dict_expr.expr_id = substituted_match_left_expr_with_right_expr.expr_id
    JOIN dict_entry
        ON dict_expr.pipeline_id = dict_entry.pipeline_id
        AND dict_expr.rule_id = dict_entry.rule_id
        AND dict_expr.dict_id = dict_entry.dict_id
    WHERE substituted_match_left_expr_with_right_expr.expr_type = 'dict_expr';

/*
match_where_cond(pipeline_id:, rule_id:, match_id:, sql:) <-
    body_match(pipeline_id:, rule_id:, match_id:, left_expr_id:, left_expr_type:, right_expr_id:, right_expr_type:)
    left_expr_type = 'array_expr'
    array_expr_length(pipeline_id:, rule_id:, expr_id:, length:)
    substituted_expr(pipeline_id:, rule_id:, expr_id: right_expr_id, expr_type: right_expr_type, sql: expr_sql)
    sql := `ARRAY_LENGTH({{expr_sql}}) = {{length}}`
match_where_cond(pipeline_id:, rule_id:, match_id:, sql:) <-
    substituted_match_left_expr_with_right_expr(
        pipeline_id:, rule_id:, match_id:, expr_id:, expr_type_id:, sql: match_expr_sql)
    substituted_expr(pipeline_id:, rule_id:, expr_id: left_expr_id, expr_type: left_expr_type, sql: left_expr_sql)
    sql := `{{match_expr_sql}} = {{left_expr_sql}}`
*/
CREATE MATERIALIZED VIEW match_where_cond AS
    SELECT DISTINCT
        body_match.pipeline_id,
        body_match.rule_id,
        body_match.match_id,
        ('ARRAY_LENGTH(' || substituted_expr.sql || ') = ' || array_expr_length.length) AS sql
    FROM body_match
    JOIN array_expr_length
        ON body_match.pipeline_id = array_expr_length.pipeline_id
        AND body_match.rule_id = array_expr_length.rule_id
        AND body_match.left_expr_id = array_expr_length.expr_id
    JOIN substituted_expr
        ON body_match.pipeline_id = substituted_expr.pipeline_id
        AND body_match.rule_id = substituted_expr.rule_id
        AND body_match.right_expr_id = substituted_expr.expr_id
        AND body_match.right_expr_type = substituted_expr.expr_type
    WHERE body_match.left_expr_type = 'array_expr'

    UNION

    SELECT DISTINCT
        substituted_expr.pipeline_id,
        substituted_expr.rule_id,
        substituted_match_left_expr_with_right_expr.match_id,
        (substituted_match_left_expr_with_right_expr.sql || ' = ' || substituted_expr.sql) AS sql
    FROM substituted_match_left_expr_with_right_expr
    JOIN substituted_expr
        ON substituted_expr.pipeline_id = substituted_match_left_expr_with_right_expr.pipeline_id
        AND substituted_expr.rule_id = substituted_match_left_expr_with_right_expr.rule_id;

/*
where_cond(pipeline_id:, rule_id:, sql_lines: [sql]) <-
    sql_where_cond(pipeline_id:, rule_id:, sql:)
where_cond(pipeline_id:, rule_id:, sql_lines: [sql]) <-
    match_where_cond(pipeline_id:, rule_id:, sql:)
where_cond(pipeline_id:, rule_id:, sql_lines:) <-
    neg_fact_where_cond(pipeline_id:, rule_id:, sql_lines:)
*/

/*
var_join(pipeline_id:, rule_id:, fact_id:, var_name:, sql:) <-
    canonical_fact_var_sql(pipeline_id:, rule_id:, fact_index: prev_fact_index, var_name:, sql: prev_sql)
    var_bound_in_fact(pipeline_id:, rule_id:, fact_id:, fact_index: next_fact_index, var_name:, sql: next_sql, negated: false)
    prev_fact_index < next_fact_index
    sql := `{{next_sql}} = {{prev_sql}}`
*/
CREATE MATERIALIZED VIEW var_join AS
    SELECT DISTINCT
        var_bound_in_fact.pipeline_id,
        var_bound_in_fact.rule_id,
        var_bound_in_fact.fact_id,
        canonical_fact_var_sql.var_name,
        (var_bound_in_fact.sql || ' = ' || canonical_fact_var_sql.sql) AS sql
    FROM canonical_fact_var_sql
    JOIN var_bound_in_fact
        ON canonical_fact_var_sql.pipeline_id = var_bound_in_fact.pipeline_id
        AND canonical_fact_var_sql.rule_id = var_bound_in_fact.rule_id
        AND canonical_fact_var_sql.var_name = var_bound_in_fact.var_name
        AND NOT var_bound_in_fact.negated
    WHERE canonical_fact_var_sql.fact_index < var_bound_in_fact.fact_index;

/*
rule_join_sql(pipeline_id:, rule_id:, fact_id:, sql_lines:) <-
    first_fact_alias(pipeline_id:, rule_id:, fact_id:, table_name:, alias:, negated: false)
    sql_lines := [`FROM "{{table_name}}" AS "{{alias}}"`]
rule_join_sql(pipeline_id:, rule_id:, fact_id:, sql_lines:) <-
    rule_join_sql(pipeline_id:, rule_id:, fact_id: prev_fact_id, sql_lines: prev_sql_lines)
    adjacent_facts(pipeline_id:, rule_id:, prev_fact_id:, next_fact_id:)
    fact_alias(pipeline_id:, rule_id:, table_name:, fact_id: next_fact_id, alias: next_alias)
    not var_join(pipeline_id:, rule_id:, fact_id: next_fact_id)
    sql_lines := [*prev_sql_lines, `CROSS JOIN "{{table_name}}" AS "{{next_alias}}"`]
rule_join_sql(pipeline_id:, rule_id:, fact_id: next_fact_id, sql_lines:) <-
    rule_join_sql(pipeline_id:, rule_id:, fact_id: prev_fact_id, sql_lines: prev_sql_lines)
    adjacent_facts(pipeline_id:, rule_id:, prev_fact_id:, next_fact_id:)
    fact_alias(pipeline_id:, rule_id:, table_name:, fact_id: next_fact_id, alias: next_alias)
    var_join(pipeline_id:, rule_id:, fact_id: next_fact_id, sql:)
    [first_cond, *rest_conds] := array<sql>
    sql_lines := [
        *prev_sql_lines,
        `JOIN "{{table_name}}" AS "{{next_alias}}"`,
        `ON {{first_cond}}`,
        *map(rest_conds, x -> `AND {{x}}`),
    ]
*/
DECLARE RECURSIVE VIEW rule_join_sql (pipeline_id TEXT, rule_id TEXT, fact_id TEXT, sql_lines TEXT ARRAY);
CREATE MATERIALIZED VIEW rule_join_sql AS
    SELECT DISTINCT
        first_fact_alias.pipeline_id,
        first_fact_alias.rule_id,
        first_fact_alias.fact_id,
        ARRAY['  FROM "' || first_fact_alias.table_name || '" AS "' || first_fact_alias.alias || '"'] AS sql_lines
    FROM first_fact_alias
    WHERE NOT first_fact_alias.negated
    
    UNION

    SELECT DISTINCT
        next_fact_alias.pipeline_id,
        next_fact_alias.rule_id,
        next_fact_alias.fact_id,
        ARRAY_CONCAT(
            prev_rule_join_sql.sql_lines,
            ARRAY['CROSS JOIN "' || next_fact_alias.table_name || '" AS "' || next_fact_alias.alias || '"']) AS sql_lines
    FROM rule_join_sql AS prev_rule_join_sql
    JOIN adjacent_facts
        ON prev_rule_join_sql.pipeline_id = adjacent_facts.pipeline_id
        AND prev_rule_join_sql.rule_id = adjacent_facts.rule_id
        AND prev_rule_join_sql.fact_id = adjacent_facts.prev_fact_id
    JOIN fact_alias AS next_fact_alias
        ON adjacent_facts.pipeline_id = next_fact_alias.pipeline_id
        AND adjacent_facts.rule_id = next_fact_alias.rule_id
        AND adjacent_facts.next_fact_id = next_fact_alias.fact_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM var_join
        WHERE adjacent_facts.pipeline_id = var_join.pipeline_id
        AND adjacent_facts.rule_id = var_join.rule_id
        AND adjacent_facts.next_fact_id = var_join.fact_id
    )

    UNION

    SELECT DISTINCT
        next_fact_alias.pipeline_id,
        next_fact_alias.rule_id,
        next_fact_alias.fact_id,
        ARRAY_CONCAT(
            prev_rule_join_sql.sql_lines,
            ARRAY['JOIN "' || next_fact_alias.table_name || '" AS "' || next_fact_alias.alias || '"'],
            ARRAY['ON ' || ARRAY_AGG(var_join.sql)[1]],
            TRANSFORM(
                TEXT_ARRAY_DROP_LEFT(
                    ARRAY_AGG(var_join.sql),
                    CAST(1 AS INTEGER UNSIGNED)),
                x -> 'AND ' || x)
        ) AS sql_lines
    FROM rule_join_sql AS prev_rule_join_sql
    JOIN adjacent_facts
        ON prev_rule_join_sql.pipeline_id = adjacent_facts.pipeline_id
        AND prev_rule_join_sql.rule_id = adjacent_facts.rule_id
        AND prev_rule_join_sql.fact_id = adjacent_facts.prev_fact_id
    JOIN fact_alias AS next_fact_alias
        ON adjacent_facts.pipeline_id = next_fact_alias.pipeline_id
        AND adjacent_facts.rule_id = next_fact_alias.rule_id
        AND adjacent_facts.next_fact_id = next_fact_alias.fact_id
    JOIN var_join
        ON adjacent_facts.pipeline_id = var_join.pipeline_id
        AND adjacent_facts.rule_id = var_join.rule_id
        AND adjacent_facts.next_fact_id = var_join.fact_id
    GROUP BY next_fact_alias.pipeline_id, next_fact_alias.rule_id, next_fact_alias.fact_id, prev_rule_join_sql.sql_lines, next_fact_alias.table_name, next_fact_alias.alias;

/*
unaggregated_param_expr(pipeline_id:, rule_id:, key:, expr_id:, expr_type:, sql:) <-
    rule_param(pipeline_id:, rule_id:, key:, expr_id:, expr_type:)
    substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql:, aggregated: false)
*/
CREATE MATERIALIZED VIEW unaggregated_param_expr AS
    SELECT DISTINCT
        rule_param.pipeline_id,
        rule_param.rule_id,
        rule_param.key,
        rule_param.expr_id,
        rule_param.expr_type,
        substituted_expr.sql
    FROM rule_param
    JOIN substituted_expr
        ON rule_param.pipeline_id = substituted_expr.pipeline_id
        AND rule_param.rule_id = substituted_expr.rule_id
        AND rule_param.expr_id = substituted_expr.expr_id
        AND rule_param.expr_type = substituted_expr.expr_type
    WHERE NOT substituted_expr.aggregated;

/*
grouped_by_sql(pipeline_id:, rule_id:, sql:) <-
    unaggregated_param_expr(pipeline_id:, rule_id:, sql: param_sql)
    exprs_sql := join(array<param_sql>, ", ")
    sql := `GROUP BY {{exprs_sql}}`
*/
CREATE MATERIALIZED VIEW grouped_by_sql AS
    SELECT DISTINCT
        unaggregated_param_expr.pipeline_id,
        unaggregated_param_expr.rule_id,
        ('GROUP BY ' || ARRAY_TO_STRING(ARRAY_AGG(unaggregated_param_expr.sql), ', ')) AS sql
    FROM unaggregated_param_expr
    GROUP BY unaggregated_param_expr.pipeline_id, unaggregated_param_expr.rule_id;

/*
join_sql(pipeline_id:, rule_id:, sql_lines:) <-
    # if there is only one fact, then there is no adjacent fact
    rule_join_sql(pipeline_id:, rule_id:, fact_id:, sql_lines:)
    not adjacent_facts(pipeline_id:, rule_id:, next_fact_id: fact_id)
    not adjacent_facts(pipeline_id:, rule_id:, prev_fact_id: fact_id)
join_sql(pipeline_id:, rule_id:, sql_lines:) <-
    # pick the last fact_id, for which there is no next one
    adjacent_facts(pipeline_id:, rule_id:, next_fact_id: last_fact_id)
    not adjacent_facts(pipeline_id:, rule_id:, prev_fact_id: last_fact_id)
    rule_join_sql(pipeline_id:, rule_id:, fact_id: last_fact_id, sql_lines:)
*/
CREATE MATERIALIZED VIEW join_sql AS
    SELECT DISTINCT
        rule_join_sql.pipeline_id,
        rule_join_sql.rule_id,
        rule_join_sql.sql_lines
    FROM rule_join_sql
    WHERE NOT EXISTS (
        SELECT 1
        FROM adjacent_facts
        WHERE rule_join_sql.pipeline_id = adjacent_facts.pipeline_id
        AND rule_join_sql.rule_id = adjacent_facts.rule_id
        AND adjacent_facts.next_fact_id = rule_join_sql.fact_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM adjacent_facts
        WHERE rule_join_sql.pipeline_id = adjacent_facts.pipeline_id
        AND rule_join_sql.rule_id = adjacent_facts.rule_id
        AND adjacent_facts.prev_fact_id = rule_join_sql.fact_id
    )
 
    UNION

    SELECT DISTINCT
        rule_join_sql.pipeline_id,
        rule_join_sql.rule_id,
        rule_join_sql.sql_lines
    FROM rule_join_sql
    JOIN adjacent_facts AS a
        ON rule_join_sql.pipeline_id = a.pipeline_id
        AND rule_join_sql.rule_id = a.rule_id
        AND rule_join_sql.fact_id = a.prev_fact_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM adjacent_facts
        WHERE rule_join_sql.pipeline_id = adjacent_facts.pipeline_id
        AND rule_join_sql.rule_id = adjacent_facts.rule_id
        AND adjacent_facts.prev_fact_id = a.next_fact_id
    );

/*
select_sql(pipeline_id:, rule_id:, sql_lines:) <-
    rule_param(pipeline_id:, rule_id:, key:, expr_id:, expr_type:)
    substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql: expr_sql)
    grouped_by_sql(pipeline_id:, rule_id:, sql: group_by_sql)
    join_sql(pipeline_id:, rule_id:, sql_lines: join_sql_lines)
    columns_sql := join(array<`{{expr_sql}} AS "{{key}}"`>, ", ")
    sql_lines := [
        `SELECT DISTINCT {{columns_sql}}`,
        *join_sql_lines,
        group_by_sql,
    ]
*/
CREATE MATERIALIZED VIEW select_sql AS
    SELECT DISTINCT
        rule_param.pipeline_id,
        rule_param.rule_id,
        ARRAY_CONCAT(
            ARRAY['SELECT DISTINCT ' || ARRAY_TO_STRING(ARRAY_AGG(substituted_expr.sql || ' AS "' || rule_param.key || '"'), ', ')],
            join_sql.sql_lines,
            ARRAY[grouped_by_sql.sql]
        ) AS sql_lines
        -- ('SELECT ' || ARRAY_TO_STRING(ARRAY_AGG(substituted_expr.sql || ' AS "' || rule_param.key || '"'), ', ') || ' ' || join_sql.sql || ' ' || grouped_by_sql.sql) AS sql
    FROM rule_param
    JOIN substituted_expr
        ON rule_param.pipeline_id = substituted_expr.pipeline_id
        AND rule_param.rule_id = substituted_expr.rule_id
        AND rule_param.expr_id = substituted_expr.expr_id
        AND rule_param.expr_type = substituted_expr.expr_type
    JOIN grouped_by_sql
        ON rule_param.pipeline_id = grouped_by_sql.pipeline_id
        AND rule_param.rule_id = grouped_by_sql.rule_id
    JOIN join_sql
        ON rule_param.pipeline_id = join_sql.pipeline_id
        AND rule_param.rule_id = join_sql.rule_id
    GROUP BY rule_param.pipeline_id, rule_param.rule_id, join_sql.sql_lines, grouped_by_sql.sql;

/*
table_view_sql(pipeline_id:, table_name:, rule_id:, sql_lines:) <-
    table_first_rule(pipeline_id:, table_name:, rule_id:)
    select_sql(pipeline_id:, rule_id:, sql_lines: rule_sql_lines)
    sql_lines := [
        `CREATE MATERIALIZED VIEW "{{table_name}}" AS`,
        *rule_sql_lines,
    ]
table_view_sql(pipeline_id:, table_name:, rule_id:, sql_lines:) <-
    table_view_sql(pipeline_id:, table_name:, rule_id: prev_rule_id, sql_lines: prev_sql_lines)
    table_next_rule(pipeline_id:, table_name:, prev_rule_id:, next_rule_id:)
    select_sql(pipeline_id:, rule_id: next_rule_id, sql_lines: next_sql_lines)
    table_next_rule(pipeline_id:, table_name:, prev_rule_id: next_rule_id)
    sql_lines := [*prev_sql_lines, "UNION", *next_sql_lines]
table_view_sql(pipeline_id:, table_name:, rule_id:, sql_lines:) <-
    table_view_sql(pipeline_id:, table_name:, rule_id: prev_rule_id, sql_lines: prev_sql_lines)
    table_next_rule(pipeline_id:, table_name:, prev_rule_id:, next_rule_id:)
    select_sql(pipeline_id:, rule_id: next_rule_id, sql_lines: next_sql_lines)
    not table_next_rule(pipeline_id:, table_name:, prev_rule_id: next_rule_id)
    sql_lines := [*prev_sql_lines, "UNION", *next_sql_lines, ";"]
*/
DECLARE RECURSIVE VIEW table_view_sql (pipeline_id TEXT, table_name TEXT, rule_id TEXT, sql_lines TEXT ARRAY);
CREATE MATERIALIZED VIEW table_view_sql AS
    SELECT DISTINCT
        table_first_rule.pipeline_id,
        table_first_rule.table_name,
        table_first_rule.rule_id,
        ARRAY_CONCAT(
            ARRAY['CREATE MATERIALIZED VIEW "' || table_first_rule.table_name || '" AS'],
            select_sql.sql_lines
        ) AS sql_lines
    FROM table_first_rule
    JOIN select_sql
        ON table_first_rule.pipeline_id = select_sql.pipeline_id
        AND table_first_rule.rule_id = select_sql.rule_id
    
    UNION

    SELECT DISTINCT
        tn.pipeline_id,
        tn.table_name,
        tn.next_rule_id,
        ARRAY_CONCAT(
            table_view_sql.sql_lines,
            ARRAY['UNION'],
            select_sql.sql_lines
        ) AS sql_lines
    FROM table_view_sql
    JOIN table_next_rule AS tn
        ON tn.pipeline_id = table_view_sql.pipeline_id
        AND tn.table_name = table_view_sql.table_name
        AND tn.prev_rule_id = table_view_sql.rule_id
    JOIN select_sql
        ON tn.pipeline_id = select_sql.pipeline_id
        AND tn.next_rule_id = select_sql.rule_id
    JOIN table_next_rule AS tnn
        ON tn.pipeline_id = tnn.pipeline_id
        AND tn.table_name = tnn.table_name
        AND tn.next_rule_id = tnn.prev_rule_id

    UNION

    SELECT DISTINCT
        tn.pipeline_id,
        tn.table_name,
        tn.next_rule_id,
        ARRAY_CONCAT(
            table_view_sql.sql_lines,
            ARRAY['UNION'],
            select_sql.sql_lines,
            ARRAY[';']
        ) AS sql_lines
    FROM table_view_sql
    JOIN table_next_rule AS tn
        ON tn.pipeline_id = table_view_sql.pipeline_id
        AND tn.table_name = table_view_sql.table_name
        AND tn.prev_rule_id = table_view_sql.rule_id
    JOIN select_sql
        ON tn.pipeline_id = select_sql.pipeline_id
        AND tn.next_rule_id = select_sql.rule_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM table_next_rule AS tnn
        WHERE tn.pipeline_id = tnn.pipeline_id
        AND tn.table_name = tnn.table_name
        AND tn.next_rule_id = tnn.prev_rule_id
    );

/*
view_full_sql(pipeline_id:, table_name:, sql_lines:) <-
    table_view_sql(pipeline_id:, table_name:, rule_id:, sql_lines:)
    not table_next_rule(pipeline_id:, table_name:, prev_rule_id: rule_id)
*/
CREATE MATERIALIZED VIEW view_full_sql AS
    SELECT DISTINCT
        table_view_sql.pipeline_id,
        table_view_sql.table_name,
        table_view_sql.sql_lines
    FROM table_view_sql
    WHERE NOT EXISTS (
        SELECT 1
        FROM table_next_rule AS tn
        WHERE tn.pipeline_id = table_view_sql.pipeline_id
        AND tn.table_name = table_view_sql.table_name
        AND tn.prev_rule_id = table_view_sql.rule_id
    );

/*
pipeline_views_sql(pipeline_id:, table_name:, sql_lines:) <-
    first_table(pipeline_id:, table_name:, order: 1)
    view_full_sql(pipeline_id:, table_name:, sql_lines:)
pipeline_views_sql(pipeline_id:, table_name:, sql_lines:) <-
    pipeline_views_sql(pipeline_id:, table_name: prev_table_name, sql_lines: prev_sql_lines)
    next_table(pipeline_id:, prev_table_name:, next_table_name: table_name)
    view_full_sql(pipeline_id:, table_name:, sql_lines: next_sql_lines)
    sql_lines := [*prev_sql_lines, *next_sql_lines]
*/
DECLARE RECURSIVE VIEW pipeline_views_sql (pipeline_id TEXT, table_name TEXT, sql_lines TEXT ARRAY);
CREATE MATERIALIZED VIEW pipeline_views_sql AS
    SELECT DISTINCT
        first_table.pipeline_id,
        first_table.table_name,
        view_full_sql.sql_lines
    FROM first_table
    JOIN view_full_sql
        ON first_table.pipeline_id = view_full_sql.pipeline_id
        AND first_table.table_name = view_full_sql.table_name
    WHERE first_table."order" = 1

    UNION

    SELECT DISTINCT
        next_table.pipeline_id,
        next_table.next_table_name,
        ARRAY_CONCAT(
            pipeline_views_sql.sql_lines,
            view_full_sql.sql_lines
        ) AS sql_lines
    FROM pipeline_views_sql
    JOIN next_table
        ON next_table.pipeline_id = pipeline_views_sql.pipeline_id
        AND next_table.prev_table_name = pipeline_views_sql.table_name
    JOIN view_full_sql
        ON next_table.pipeline_id = view_full_sql.pipeline_id
        AND next_table.next_table_name = view_full_sql.table_name;

/*
full_pipeline_sql(pipeline_id:, sql_lines:) <-
    pipeline_views_sql(pipeline_id:, table_name:, sql_lines:)
    not next_table(pipeline_id:, prev_table_name: table_name)
*/
CREATE MATERIALIZED VIEW full_pipeline_sql AS
    SELECT DISTINCT
        pipeline_views_sql.pipeline_id,
        pipeline_views_sql.sql_lines
    FROM pipeline_views_sql
    WHERE NOT EXISTS (
        SELECT 1
        FROM next_table
        WHERE next_table.pipeline_id = pipeline_views_sql.pipeline_id
        AND next_table.prev_table_name = pipeline_views_sql.table_name
    );
