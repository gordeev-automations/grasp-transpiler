from dsl import rule, goal, neg_goal, sql_cond, sql_expr, match, null, strval, dictval, aggr, array



def get_rules():
    return [

rule('last_node_ping', ['node_session_id', ('at', aggr('max', 'at'))], [
    goal('node_ping', 'node_session_id', 'at'),
]),

rule('missing_ping', ['node_session_id'], [
    goal('last_node_ping', 'node_session_id', 'at'),
    goal('node_ping', 'node_session_id', 'at', id='node_ping_id'),
    neg_goal('node_pong', 'node_session_id', 'node_ping_id'),
    sql_cond(['{{at}}', '< NOW() - INTERVAL 3 SECONDS']),
]),

rule('active_node', ['node_session_id'], [
    goal('node_started', 'node_session_id'),
    neg_goal('missing_ping', 'node_session_id'),
    neg_goal('node_stopped', 'node_session_id'),
    neg_goal('node_down', 'node_session_id'),
], materialized=True),

rule('last_task_assigned', ['session_id', ('at', aggr('max', 'at'))], [
    goal('task_assigned', 'session_id', 'at'),
]),

rule('last_task_ignored', ['session_id'], [
    goal('last_task_assigned', 'session_id', 'at'),
    goal('task_assigned', 'session_id', 'at', id='task_id'),
    neg_goal('task_started', 'session_id', 'task_id'),
    sql_cond(['{{at}}', '< NOW() - INTERVAL 3 SECONDS']),
]),

rule('active_session', [
    'node_session_id', ('session_id', 'node_session_id'), 'session_type',
    ('task_type', strval('')), ('desired_task_id', null),
], [
    goal('active_node', 'node_session_id', 'session_type'),
    neg_goal('last_task_ignored', ('session_id', 'node_session_id')),
]),
rule('active_session', [
    'node_session_id', 'session_id', 'session_type',
    'task_type', 'desired_task_id',
], [
    goal('session_started', 'session_id', 'parent_session_id', 'start_task_id'),
    goal('task_assigned', 'desired_task_id', 'task_type', id='start_task_id'),
    goal('active_session', 'node_session_id', ('session_id', 'parent_session_id'), 'session_type'),
    neg_goal('last_task_ignored', 'session_id'),
    neg_goal('session_finished', 'session_id'),
    neg_goal('session_crashed', 'session_id'),
    neg_goal('session_shutdown', 'session_id'),
]),

rule('unresponsive_session', ['session_id'], [
    goal('active_session', 'session_id'),
    goal('task_assigned', 'session_id', 'at', id='task_id'),
    neg_goal('task_started', 'task_id'),
    sql_cond(['{{at}}', ' < NOW() - INTERVAL 3 SECONDS']),
]),

rule('running_task', ['session_id', 'task_id', 'task_type', 'desired_task_id'], [
    goal('active_session', 'session_id'),
    goal('task_assigned', 'session_id', 'task_type', 'desired_task_id', id='task_id'),
    goal('task_started', 'session_id', 'task_id'),
    neg_goal('task_succeeded', 'task_id'),
    neg_goal('task_failed', 'task_id'),
]),

rule('tg_bot_dialog', ['tenant', 'tg_bot_username', 'tg_account_id'], [
    goal('tg_update', 'tenant', 'tg_bot_username',
        ('tg_update', dictval(('message', dictval(('chat', dictval(('id', 'tg_account_id')))))))),
    sql_cond(['{{tg_account_id}}', ' > 0']),
]),
rule('tg_bot_dialog', ['tenant', 'tg_bot_username', 'tg_account_id'], [
    goal('tg_update', 'tenant', 'tg_bot_username',
        ('tg_update', dictval(('callback_query', dictval(('message', dictval(('chat', dictval(('id', 'tg_account_id')))))))))),
    sql_cond(['{{tg_account_id}}', ' > 0']),
]),

rule('static_response_sent_in_tg_bot_dialog', [
    'tg_bot_dialog_id', 'tenant', 'tg_bot_username', 'tg_account_id',
    'triggering_tg_update_id', 'static_response_type', 'at',
], [
    goal('static_response_sent_by_tg_bot', 'tenant', 'tg_bot_username', 'tg_account_id',
        'triggering_tg_update_id', 'static_response_type', 'at'),
    goal('tg_bot_dialog', 'tenant', 'tg_bot_username', 'tg_account_id', id='tg_bot_dialog_id'),
]),

rule('command_from_tg_peer_received', [
    'tg_bot_dialog_id', 'tenant', 'tg_bot_username', 'tg_account_id',
    'message_tg_update_id', 'command', 'args',
], [
    goal('tg_update', 'tenant', 'tg_bot_username',
        ('tg_update', dictval(
            ('message', dictval(
                'text', 'message_id',
                ('chat', dictval('id'))))))),
    goal('tg_bot_dialog', 'tenant', 'tg_bot_username', 'tg_account_id', id='tg_bot_dialog_id'),
    match('cmd_with_arg', sql_expr(['SUBSTRING(CAST(','{{text}}',' AS TEXT) FROM 2)'])),
    match(array('command', 'arg'), sql_expr(['SPLIT_PART(','{{cmd_with_arg}}',', \' \', 2)'])),
    match('args', sql_expr(['SPLIT(','{{arg}}',', \':\')'])),
    match('tg_account_id', sql_expr(['CAST(','{{id}}',' AS BIGINT)'])),
    match('tg_message_id', sql_expr(['CAST(','{{message_id}}',' AS BIGINT)'])),
    sql_cond(['LEFT(', '{{text}}', ', 1) = \'/\'']),
    sql_cond(['{{command}}', ' RLIKE \'^\\/\\[A-Za-z0-9_]+$\'']),
]),

rule('command_from_tg_peer_received', [
    'tg_bot_dialog_id', 'tenant', 'tg_bot_username', 'tg_account_id',
    'message_tg_update_id', 'command', ('args', sql_expr(['CAST(ARRAY() AS TEXT ARRAY)'])),
], [
    goal('tg_update', 'tenant', 'tg_bot_username',
        ('tg_update', dictval(
            ('message', dictval(
                'text', 'message_id',
                ('chat', dictval('id'))))))),
    goal('tg_bot_dialog', 'tenant', 'tg_bot_username', 'tg_account_id', id='tg_bot_dialog_id'),
    match('cmd_with_arg', sql_expr(['SUBSTRING(CAST(','{{text}}',' AS TEXT) FROM 2)'])),
    match(array('command'), sql_expr(['SPLIT_PART(','{{cmd_with_arg}}',', \' \', 2)'])),
    match('tg_account_id', sql_expr(['CAST(','{{id}}',' AS BIGINT)'])),
    match('tg_message_id', sql_expr(['CAST(','{{message_id}}',' AS BIGINT)'])),
    sql_cond(['LEFT(CAST(', '{{text}}', ' AS TEXT), 1) = \'/\'']),
    sql_cond(['{{command}}', ' RLIKE \'^\\/\\[A-Za-z0-9_]+$\'']),
]),

rule('start_command_from_tg_peer_received', [
    'tg_bot_dialog_id', 'tenant', 'tg_bot_username', 'tg_account_id',
    'tg_message_timestamp', 'start_message_tg_update_id', ('start_arg', null),
], [
    goal('command_from_tg_peer_received', 'tg_bot_dialog_id', 'tenant', 'tg_bot_username',
        'tg_account_id', ('message_tg_update_id', 'start_message_tg_update_id'), 'tg_message_timestamp',
        ('command', strval('start')), ('args', array())),
]),

rule('start_command_from_tg_peer_received', [
    'tg_bot_dialog_id', 'tenant', 'tg_bot_username', 'tg_account_id',
    'tg_message_timestamp', 'start_message_tg_update_id', 'start_arg',
], [
    goal('command_from_tg_peer_received', 'tg_bot_dialog_id', 'tenant', 'tg_bot_username',
        'tg_account_id', ('message_tg_update_id', 'start_message_tg_update_id'), 'tg_message_timestamp',
        ('command', strval('start')), ('args', array('start_arg'))),
    sql_cond(['{{start_arg}}', ' RLIKE \'^[A-Za-z0-9_-]+=*$\'']),
]),

rule('last_start_command_from_tg_peer_received_at', [
    'tg_bot_dialog_id', 'tenant', 'tg_bot_username', 'tg_account_id',
    ('tg_message_timestamp', aggr('max', 'tg_message_timestamp')),
], [
    goal('start_command_from_tg_peer_received', 'tg_bot_dialog_id', 'tenant', 'tg_bot_username',
        'tg_account_id', 'tg_message_timestamp'),
]),

rule('last_start_command_from_tg_peer_received', [
    'tg_bot_dialog_id', 'tenant', 'tg_bot_username', 'tg_account_id',
    'start_message_tg_update_id', 'start_arg', 'tg_message_timestamp',
], [
    goal('last_start_command_from_tg_peer_received_at', 'tg_bot_dialog_id', 'tenant', 'tg_bot_username',
        'tg_account_id', 'tg_message_timestamp'),
    goal('start_command_from_tg_peer_received', 'tg_bot_dialog_id', 'tenant', 'tg_bot_username',
        'tg_account_id', 'start_message_tg_update_id', 'tg_message_timestamp', 'start_arg'),
]),

rule('callback_query_from_tg_peer_received', [
    'tg_bot_dialog_id', 'tenant', 'tg_bot_username', 'tg_account_id',
    'callback_query_tg_update_id', 'tg_message_id', 'args', 'tg_message_timestamp',
], [
    goal('tg_update', 'tenant', 'tg_bot_username',
        ('tg_update', dictval(
            'update_id',
            ('callback_query', dictval(
                'data',
                ('message', dictval(
                    'date', 'message_id',
                    ('chat', dictval('id'))))))))),
    goal('tg_bot_dialog', 'tenant', 'tg_bot_username', 'tg_account_id', id='tg_bot_dialog_id'),
    match('args', sql_expr(['SPLIT(CAST(','{{data}}',' AS TEXT), \':\')'])),
    match('tg_message_timestamp', sql_expr(['CAST(CAST(','{{date}}',' AS BIGINT)*1000 AS TIMESTAMP)'])),
    match('tg_message_id', sql_expr(['CAST(','{{message_id}}',' AS BIGINT)'])),
    match('tg_account_id', sql_expr(['CAST(','{{id}}',' AS BIGINT)'])),
    match('callback_query_tg_update_id', sql_expr(['CAST(','{{update_id}}',' AS BIGINT)'])),
]),

rule('tg_join_request_received', [
    'tg_bot_dialog_id', 'tenant', 'tg_bot_username', 'tg_account_id',
    'tg_chat_id', 'tg_update_id',
], [
    goal('tg_update', 'tenant', 'tg_bot_username',
        ('tg_update', dictval(
            'update_id',
            ('chat_join_request', dictval(
                ('chat', dictval(('id', 'id1'))),
                ('from_user', dictval(('id', 'id2')))))))),
    goal('tg_bot_dialog', 'tenant', 'tg_bot_username', 'tg_account_id', id='tg_bot_dialog_id'),
    match('tg_update_id', sql_expr(['CAST(','{{update_id}}',' AS BIGINT)'])),
    match('tg_chat_id', sql_expr(['CAST(','{{id1}}',' AS BIGINT)'])),
    match('tg_account_id', sql_expr(['CAST(','{{id2}}',' AS BIGINT)'])),
]),

rule('tg_bot_last_blocked', [
    'tg_bot_dialog_id', 'tenant', 'tg_bot_username', 'tg_account_id',
    ('at', aggr('max', 'discovered_at')),
], [
    goal('tg_bot_was_blocked', 'tenant', 'tg_bot_username', 'tg_account_id', 'discovered_at'),
    goal('tg_bot_dialog', 'tenant', 'tg_bot_username', 'tg_account_id', id='tg_bot_dialog_id'),
]),

rule('tg_bot_last_start', [
    'tg_bot_dialog_id', 'tenant', 'tg_bot_username', 'tg_account_id',
    ('at', aggr('max', 'tg_message_timestamp')),
], [
    goal('start_command_from_tg_peer_received', 'tenant', 'tg_bot_username', 'tg_account_id',
        'tg_message_timestamp'),
    goal('tg_bot_dialog', 'tenant', 'tg_bot_username', 'tg_account_id', id='tg_bot_dialog_id'),
]),

rule('tg_bot_currently_blocked', [
    'tg_bot_dialog_id', 'tenant', 'tg_bot_username', 'tg_account_id',
], [
    goal('tg_bot_last_blocked', 'tg_bot_dialog_id', 'tenant', 'tg_bot_username', 'tg_account_id'),
    neg_goal('tg_bot_last_start', 'tenant', 'tg_bot_username', 'tg_account_id'),
]),

rule('tg_bot_currently_blocked', [
    'tg_bot_dialog_id', 'tenant', 'tg_bot_username', 'tg_account_id',
], [
    goal('tg_bot_last_blocked', 'tg_bot_dialog_id', 'tenant', 'tg_bot_username', 'tg_account_id',
        ('at', 'last_banned_at')),
    goal('tg_bot_last_start', 'tenant', 'tg_bot_username', 'tg_account_id', ('at', 'last_start_at')),
    sql_cond(['{{last_banned_at}}', ' < ', '{{last_start_at}}']),
]),

rule('first_tg_message_after_static_response', [
    'tg_bot_dialog_id', 'triggering_tg_update_id', 'static_response_type',
    ('tg_message_timestamp', aggr('min', 'tg_message_timestamp')),
], [
    goal('static_response_sent_in_tg_bot_dialog', 'tenant', 'tg_bot_username', 'tg_account_id',
        'tg_bot_dialog_id', 'triggering_tg_update_id', 'static_response_type', ('at', 'static_response_at')),
    goal('tg_update', 'tenant', 'tg_bot_username',
        ('tg_update', dictval(('message', dictval('date', ('chat', dictval('id'))))))),
    match('tg_message_timestamp', sql_expr(['CAST(CAST(','{{date}}',' AS BIGINT)*1000 AS TIMESTAMP)'])),
    match('tg_account_id', sql_expr(['CAST(','{{id}}',' AS BIGINT)'])),
    sql_cond(['{{static_response_at}}', ' < ', '{{tg_message_timestamp}}']),
]),

rule('next_tg_message_after_static_response', [
    'tg_bot_dialog_id', 'triggering_tg_update_id', 'static_response_type',
    'tenant', 'tg_bot_username', 'tg_account_id', 'text', 'message_tg_update_id',
], [
    goal('static_response_sent_in_tg_bot_dialog', 'tenant',
        'tg_bot_dialog_id', 'triggering_tg_update_id', 'static_response_type'),
    goal('first_tg_message_after_static_response', 'tg_bot_dialog_id', 'triggering_tg_update_id',
        'static_response_type', 'first_tg_message_at'),
    goal('tg_update', 'tenant', 'tg_bot_username',
        ('tg_update', dictval('update_id', ('message', dictval(('text', 'text1'), 'date', ('chat', dictval('id'))))))),

    match('tg_message_timestamp', sql_expr(['CAST(CAST(','{{date}}',' AS BIGINT)*1000 AS TIMESTAMP)'])),
    match('tg_account_id', sql_expr(['CAST(','{{id}}',' AS BIGINT)'])),
    match('message_tg_update_id', sql_expr(['CAST(','{{update_id}}',' AS BIGINT)'])),
    match('text', sql_expr(['CAST(','{{text1}}',' AS TEXT)'])),
]),



# after_app

rule('active_session_available_task_type', [
    'session_id', 'task_type', 'children_group_keys',
], [
    goal('active_session', 'session_id', 'desired_task_id'),
    goal('available_task_type', 'session_id', 'task_type'),
    goal('desired_task', 'desired_task_id', 'children_group_keys'),
]),

rule('last_task_succeeded', [
    'desired_task_id', ('at', aggr('max', 'at')),
], [
    goal('task_assigned', 'desired_task_id', id='task_id'),
    goal('task_succeeded', 'task_id', 'at'),
]),

rule('last_task_failed', [
    'desired_task_id', ('at', aggr('max', 'at')),
], [
    goal('task_assigned', 'desired_task_id', id='task_id'),
    goal('task_failed', 'task_id', 'at'),
]),

rule('task_failed_attempts', [
    'desired_task_id', ('attempts_count', aggr('count')),
], [
    goal('task_failed', 'task_id', 'at'),
    goal('task_assigned', 'desired_task_id', id='task_id'),
    goal('last_task_succeeded', 'desired_task_id', ('at', 'last_task_succeeded_at')),
    sql_cond(['{{last_task_succeeded_at}}', ' < ', '{{at}}']),
]),

rule('task_failed_attempts', [
    'desired_task_id', ('attempts_count', aggr('count')),
], [
    goal('task_failed', 'task_id'),
    goal('task_assigned', 'desired_task_id', id='task_id'),
    neg_goal('last_task_succeeded', 'desired_task_id'),
]),

rule('task_retry_requested', [
    'desired_task_id', 'last_failed_at',
], [
    goal('desired_task', 'desired_task_id'),
    goal('last_task_failed', 'desired_task_id', ('at', 'last_failed_at')),
    neg_goal('last_task_succeeded', 'desired_task_id'),
    neg_goal('running_task', 'desired_task_id'),
]),

rule('last_task_retry_scheduled', [
    'desired_task_id', ('at', aggr('max', 'at')),
], [
    goal('task_retry_scheduled', 'desired_task_id', 'at'),
]),

rule('desired_task_to_assign', [
    'desired_task_id', 'session_id', ('last_retry_scheduled_at', 0),
], [
    goal('desired_task', 'desired_task_id', 'task_type', 'parent_group_keys'),
    goal('task_type', 'task_type', ('kept_running', False)),
    goal('active_session_available_task_type', 'session_id', 'task_type', ('children_group_keys', 'parent_group_keys')),
    neg_goal('last_task_succeeded', 'desired_task_id'),
    neg_goal('running_task', 'desired_task_id'),
    neg_goal('task_retry_scheduled', 'desired_task_id'),
]),
rule('desired_task_to_assign', [
    'desired_task_id', 'session_id', 'last_retry_scheduled_at',
], [
    goal('desired_task', 'desired_task_id', 'task_type', 'parent_group_keys'),
    goal('task_type', 'task_type', ('kept_running', False)),
    goal('active_session_available_task_type', 'session_id', 'task_type', ('children_group_keys', 'parent_group_keys')),
    neg_goal('last_task_succeeded', 'desired_task_id'),
    neg_goal('running_task', 'desired_task_id'),
    goal('last_task_retry_scheduled', 'desired_task_id', ('at', 'last_retry_scheduled_at')),
    sql_cond(['{{last_retry_scheduled_at}}', ' <= NOW()']),
]),
rule('desired_task_to_assign', [
    'desired_task_id', 'session_id', ('last_retry_scheduled_at', 0),
], [
    goal('desired_task', 'desired_task_id', 'task_type', 'parent_group_keys'),
    goal('task_type', 'task_type', ('kept_running', True)),
    goal('active_session_available_task_type', 'session_id', 'task_type', ('children_group_keys', 'parent_group_keys')),
    neg_goal('running_task', 'desired_task_id'),
]),

    ]