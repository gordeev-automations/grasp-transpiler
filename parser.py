import os
import functools

from lark import Lark, Tree, Token



def natural_num_generator():
    n = 1
    while True:
        yield n
        n += 1

def merge_records(records1, records2):
    result = {}
    for key, rows in records1.items():
        if key in records2:
            result[key] = [*rows, *records2[key]]
        else:
            result[key] = rows
    for key, rows in records2.items():
        if key not in result:
            result[key] = rows
    return result



def records_from_expr(expr, rule_id, expr_id, idgen):
    match expr:
        case Token(type='NUMBER', value=value):
            return 'int_expr', {
                'int_expr': [{
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'value': int(value),
                }]
            }
        case _:
            raise Exception(f"Invalid expr {expr}")



def records_from_fact_arg(fact_arg, rule_id, fact_id, idgen):
    expr_id = f'ex{next(idgen)}'
    match fact_arg:
        case Tree(data=Token(type='RULE', value='arg'), children=[
            Token(type='IDENTIFIER', value=key),            
        ]):
            return {
                'fact_arg': [{ 'rule_id': rule_id, 'fact_id': fact_id, 'key': key, 'expr_id': expr_id, 'expr_type': 'var_expr', }],
                'var_expr': [{ 'rule_id': rule_id, 'expr_id': expr_id, 'var_name': key, }],
            }
        case _:
            raise Exception(f"Invalid fact arg {fact_arg}")



def records_from_body_stmt(index, stmt, rule_id, idgen):
    match stmt:
        case Tree(data=Token(type='RULE', value='fact'), children=[
            Token(type='IDENTIFIER', value=table_name),
            Tree(data=Token(type='RULE', value='args'), children=fact_args),
        ]):
            fact_id = f'ft{next(idgen)}'
            args_records = [records_from_fact_arg(fa, rule_id, fact_id, idgen) for fa in fact_args]
            return functools.reduce(merge_records, [
                *args_records,
                { 'body_fact': [{ 'rule_id': rule_id, 'fact_id': fact_id, 'index': index, 'table_name': table_name, 'negated': False }] },
            ])
        case _:
            raise Exception(f"Invalid body stmt {stmt}")



def records_from_rule_param(rule_param, rule_id, idgen):
    expr_id = f'ex{next(idgen)}'
    match rule_param:
        case Tree(data=Token(type='RULE', value='arg'), children=[
            Token(type='IDENTIFIER', value=key),
            Tree(data=Token(type='RULE', value='expr'), children=[expr]),
        ]):
            expr_type, expr_records = records_from_expr(expr, rule_id, expr_id, idgen)
            return merge_records(
                expr_records,
                { 'rule_param': [{ 'rule_id': rule_id, 'key': key, 'expr_id': expr_id, 'expr_type': expr_type, }] },
            )
        case Tree(data=Token(type='RULE', value='arg'), children=[
            Token(type='IDENTIFIER', value=key),
        ]):
            return {
                'rule_param': [{ 'rule_id': rule_id, 'key': key, 'expr_id': expr_id, 'expr_type': 'var_expr', }],
                'var_expr': [{ 'rule_id': rule_id, 'expr_id': expr_id, 'var_name': key, }],
            }
        case _:
            raise Exception(f"Invalid rule param {rule_param}")



def records_from_rule_decl(table_name, rule_params, body_stmts, idgen):
    rule_id = f'ru{next(idgen)}'
    param_records = [records_from_rule_param(rp, rule_id, idgen) for rp in rule_params]
    body_stmts_records = [records_from_body_stmt(i, bs, rule_id, idgen) for (i, bs) in enumerate(body_stmts)]
    return functools.reduce(merge_records, [
        *param_records,
        *body_stmts_records,
        { 'rule': [{ 'rule_id': rule_id, 'table_name': table_name, }] },
    ])



def records_from_toplevel_decls(toplevel_decl, idgen):
    # print(toplevel_decl)
    match toplevel_decl:
        case Tree(data=Token(type='RULE', value='rule'), children=[
            Token(type='IDENTIFIER', value=table_name),
            Tree(data=Token(type='RULE', value='args'), children=rule_params),
        ]):
            return records_from_rule_decl(table_name, rule_params, [], idgen)
        case Tree(data=Token(type='RULE', value='rule'), children=[
            Token(type='IDENTIFIER', value=table_name),
            Tree(data=Token(type='RULE', value='args'), children=rule_params),
            Tree(data=Token(type='RULE', value='body_stmt'), children=[body_stmt]),
        ]):
            return records_from_rule_decl(table_name, rule_params, [body_stmt], idgen)
        case _:
            raise Exception(f"Invalid toplevel decl {toplevel_decl}")



def records_from_tree(tree, idgen):
    match tree:
        case Tree(data=Token(type='RULE', value='start'), children=toplevel_decls):
            return functools.reduce(
                merge_records,
                [records_from_toplevel_decls(d, idgen) for d in toplevel_decls])
        case _:
            raise Exception(f"Invalid tree {tree}")



def parse(text):
    scripts_dir = os.path.abspath(os.path.dirname(__file__))
    grammar_text = open(f'{scripts_dir}/grammar.lark', 'r').read()
    # propagate token positions
    # https://github.com/lark-parser/lark/issues/12#issuecomment-304404835
    parser = Lark(grammar_text, parser="earley", propagate_positions=True)
    tree = parser.parse(text)
    idgen = natural_num_generator()
    return records_from_tree(tree, idgen)

# import importlib
# import parser
# importlib.reload(parser); records = parser.parse(open('test/basic.test.grasp', 'r').read()); print(records)
