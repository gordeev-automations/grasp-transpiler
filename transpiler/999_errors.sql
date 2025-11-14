/*
error:unbound_var_in_negative_goal(pipeline_id:, rule_id:, goal_id:, var_name:) <-
    var_bound_in_goal(pipeline_id:, rule_id:, goal_id:, var_name:, negated: true)
    not var_bound_in_goal(pipeline_id:, rule_id:, var_name:, negated: false)
*/
CREATE MATERIALIZED VIEW "error:unbound_var_in_negative_goal" AS
    SELECT DISTINCT
        var_bound_in_goal.pipeline_id,
        var_bound_in_goal.rule_id,
        var_bound_in_goal.goal_id,
        var_bound_in_goal.var_name
    FROM var_bound_in_goal
    WHERE var_bound_in_goal.negated
    AND NOT EXISTS (
        SELECT 1
        FROM var_bound_in_goal
        WHERE var_bound_in_goal.pipeline_id = var_bound_in_goal.pipeline_id
        AND var_bound_in_goal.rule_id = var_bound_in_goal.rule_id
        AND var_bound_in_goal.var_name = var_bound_in_goal.var_name
        AND NOT var_bound_in_goal.negated
    );

/*
error:neg_goal_sql_unresolved(pipeline_id:, rule_id:, goal_id:) <-
    body_goal(pipeline_id:, rule_id:, goal_id:, negated: true)
    goal_arg(pipeline_id:, rule_id:, goal_id:, expr_id:, expr_type:)
    not substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type:)
*/
CREATE MATERIALIZED VIEW "error:neg_goal_sql_unresolved" AS
    SELECT DISTINCT
        body_goal.pipeline_id,
        body_goal.rule_id,
        body_goal.goal_id
    FROM body_goal
    JOIN goal_arg
        ON body_goal.pipeline_id = goal_arg.pipeline_id
        AND body_goal.rule_id = goal_arg.rule_id
        AND body_goal.goal_id = goal_arg.goal_id
    WHERE body_goal.negated
    AND NOT EXISTS (
        SELECT 1
        FROM substituted_expr
        WHERE pipeline_id = goal_arg.pipeline_id
        AND rule_id = goal_arg.rule_id
        AND expr_id = goal_arg.expr_id
        AND expr_type = goal_arg.expr_type);