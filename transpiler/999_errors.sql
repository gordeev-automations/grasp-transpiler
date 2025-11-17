/*
error:unbound_var_in_negative_fact(pipeline_id:, rule_id:, fact_id:, var_name:) <-
    var_bound_in_fact(pipeline_id:, rule_id:, fact_id:, var_name:, negated: true)
    not var_bound_in_fact(pipeline_id:, rule_id:, var_name:, negated: false)
*/
CREATE MATERIALIZED VIEW "error:unbound_var_in_negative_fact" AS
    SELECT DISTINCT
        var_bound_in_fact.pipeline_id,
        var_bound_in_fact.rule_id,
        var_bound_in_fact.fact_id,
        var_bound_in_fact.var_name
    FROM var_bound_in_fact
    WHERE var_bound_in_fact.negated
    AND NOT EXISTS (
        SELECT 1
        FROM var_bound_in_fact
        WHERE var_bound_in_fact.pipeline_id = var_bound_in_fact.pipeline_id
        AND var_bound_in_fact.rule_id = var_bound_in_fact.rule_id
        AND var_bound_in_fact.var_name = var_bound_in_fact.var_name
        AND NOT var_bound_in_fact.negated
    );

/*
error:neg_fact_sql_unresolved(pipeline_id:, rule_id:, fact_id:) <-
    body_fact(pipeline_id:, rule_id:, fact_id:, negated: true)
    fact_arg(pipeline_id:, rule_id:, fact_id:, expr_id:, expr_type:)
    not substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type:)
*/
CREATE MATERIALIZED VIEW "error:neg_fact_sql_unresolved" AS
    SELECT DISTINCT
        body_fact.pipeline_id,
        body_fact.rule_id,
        body_fact.fact_id
    FROM body_fact
    JOIN fact_arg
        ON body_fact.pipeline_id = fact_arg.pipeline_id
        AND body_fact.rule_id = fact_arg.rule_id
        AND body_fact.fact_id = fact_arg.fact_id
    WHERE body_fact.negated
    AND NOT EXISTS (
        SELECT 1
        FROM substituted_expr
        WHERE pipeline_id = fact_arg.pipeline_id
        AND rule_id = fact_arg.rule_id
        AND expr_id = fact_arg.expr_id
        AND expr_type = fact_arg.expr_type);

/*
error:match_right_expr_unresolved(pipeline_id:, rule_id:, match_id:) <-
    body_match(pipeline_id:, rule_id:, left_expr_id:, left_expr_type:)
    not substituted_expr(pipeline_id:, expr_id: left_expr_id, expr_type: left_expr_type)
*/
CREATE MATERIALIZED VIEW "error:match_right_expr_unresolved" AS
    SELECT DISTINCT
        body_match.pipeline_id,
        body_match.rule_id,
        body_match.match_id
    FROM body_match
    WHERE NOT EXISTS (
        SELECT 1
        FROM substituted_expr
        WHERE pipeline_id = body_match.pipeline_id
        AND rule_id = body_match.rule_id
        AND expr_id = body_match.left_expr_id
        AND expr_type = body_match.left_expr_type
    );



/*
error(pipeline_id:, error_type: "unbound_var_in_negative_fact") <-
    error:unbound_var_in_negative_fact(pipeline_id:)
error(pipeline_id:, error_type: "neg_fact_sql_unresolved") <-
    error:neg_fact_sql_unresolved(pipeline_id:)
*/
CREATE MATERIALIZED VIEW "error" AS
    SELECT DISTINCT
        "error:unbound_var_in_negative_fact".pipeline_id AS pipeline_id,
        'unbound_var_in_negative_fact' AS error_type
    FROM "error:unbound_var_in_negative_fact"
    
    UNION
    
    SELECT DISTINCT
        "error:neg_fact_sql_unresolved".pipeline_id AS pipeline_id,
        'neg_fact_sql_unresolved' AS error_type
    FROM "error:neg_fact_sql_unresolved"

    UNION

    SELECT DISTINCT
        "error:match_right_expr_unresolved".pipeline_id AS pipeline_id,
        'match_right_expr_unresolved' AS error_type
    FROM "error:match_right_expr_unresolved";

