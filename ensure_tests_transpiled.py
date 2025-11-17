import os
import sys
import hashlib
import asyncio

import aiohttp
import json5

import parser



async def start_transaction(session, pipeline_name):
    url = f'/v0/pipelines/{pipeline_name}/start_transaction'
    async with session.post(url) as resp:
        if resp.status not in [200, 201]:
            body = await resp.text()
            raise Exception(f"Unexpected response {resp.status}: {body}")

async def fetch_pipeline_stats(session, pipeline_name):
    url = f'/v0/pipelines/{pipeline_name}/stats'
    async with session.get(url) as resp:
        return await resp.json()

async def commit_transaction(session, pipeline_name):
    url = f'/v0/pipelines/{pipeline_name}/commit_transaction'
    async with session.post(url) as resp:
        if resp.status not in [200, 201]:
            body = await resp.text()
            raise Exception(f"Unexpected response {resp.status}: {body}")

    while True:
        stats = await fetch_pipeline_stats(session, pipeline_name)
        match stats:
            case {'global_metrics': {'transaction_status': 'NoTransaction'}}:
                return
            case {'global_metrics': {'transaction_status': 'TransactionInProgress'}}:
                pass
            case {'global_metrics': {'transaction_status': 'CommitInProgress'}}:
                pass
            case _:
                raise Exception(f"Unexpected stats: {stats}")
        await asyncio.sleep(1)

async def insert_records(session, pipeline_name, records):
    insert_tokens = set()

    for table_name, rows in records.items():
        url = f'/v0/pipelines/{pipeline_name}/ingress/{table_name}'
        params = {'update_format': 'raw', 'array': 'true', 'format': 'json'}

        # for row in rows:
        #     rows0 = [row]

        #     print(f"Inserting {table_name}({row})")

        async with session.post(url, params=params, json=rows) as resp:
            if resp.status not in [200, 201]:
                body = await resp.text()
                raise Exception(f"Unexpected response {resp.status}: {body}")
            json_resp = await resp.json()
            insert_tokens.add(json_resp['token'])
            print(f"Inserted {len(rows)} records into {table_name}: {json_resp}")

            # token = json_resp['token']
            # while True:
            #     status = await fetch_ingest_status(session, pipeline_name, token)
            #     match status:
            #         case {'status': 'inprogress'}:
            #             await asyncio.sleep(1)
            #         case {'status': 'complete'}:
            #             break
            #             # print(f"Insert completed: {token}")
            #             # tokens.remove(token)
            #         case _:
            #             raise Exception(f"Unknown ingest status: {status}")

            # input(f"Next?")

    return insert_tokens

async def fetch_ingest_status(session, pipeline_name, token):
    url = f'/v0/pipelines/{pipeline_name}/completion_status'
    async with session.get(url, params={'token': token}) as resp:
        return await resp.json()

async def adhoc_query(session, pipeline_name, sql):
    url = f'/v0/pipelines/{pipeline_name}/query'
    async with session.get(url, params={'sql': sql, 'format': 'json', 'array': 'true'}) as resp:
        return await resp.json()

async def fetch_output_sql_lines(session, pipeline_name, pipeline_id):
    sql = f"SELECT sql_lines FROM full_pipeline_sql WHERE pipeline_id = '{pipeline_id}'"
    result = await adhoc_query(session, pipeline_name, sql)
    assert result
    # print(f"REUSLT: {result}")
    # assert len(result) == 1
    return result['sql_lines']

def file_hash(path):
    return hashlib.sha256(open(path, 'rb').read()).hexdigest()[:10]

def testcase_key(path):
    filename = os.path.basename(path)
    assert filename[-11:] == '.test.grasp'
    return filename[:-11]

def testcase_dest_path(testcase_path, cache_dir):
    return f'{cache_dir}/{testcase_key(testcase_path)}.{file_hash(testcase_path)}.sql'

def need_to_transpile(testcase_path, cache_dir):
    return not os.path.exists(testcase_dest_path(testcase_path, cache_dir))

async def enqueue_transpilation(testcase_path, pipeline_name, session):
    records0 = parser.parse(open(testcase_path, 'r').read())
    pipeline_id = f'{testcase_key(testcase_path)}:{file_hash(testcase_path)}'
    records = {}
    for table_name, rows in records0.items():
        records[table_name] = [{**r, 'pipeline_id': pipeline_id} for r in rows]
    tokens = await insert_records(session, pipeline_name, records)
    return (pipeline_id, tokens)

async def report_errors_if_any(session, pipeline_name, pipeline_id, testcase_path):
    sql = f"SELECT error_type FROM \"error\" WHERE pipeline_id = '{pipeline_id}'"
    result = await adhoc_query(session, pipeline_name, sql)
    if result:
        print(f"Error in {testcase_path}: {', '.join([x['error_type'] for x in result])}")
        return True
    return False 

async def write_output_sql(session, pipeline_name, pipeline_id, dest_path):
    print(f"Writing {dest_path}")
    sql_lines = await fetch_output_sql_lines(session, pipeline_name, pipeline_id)
    print(f"SQL LINES: {sql_lines}")
    with open(dest_path, 'w') as f:
        for line in sql_lines:
            f.write(line + '\n')



async def main(testcases_paths):
    feldera_url = 'http://localhost:8080'
    pipeline_name = 'transpiler'

    curr_dir = os.path.abspath(os.path.dirname(__file__))
    cache_dir = f'{curr_dir}/test/.grasp_cache'
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    pipeline_ids = {}
    queued_tokens = {}
    # fin_tokens = {}
    with_errors = {}
    async with aiohttp.ClientSession(feldera_url, timeout=aiohttp.ClientTimeout(sock_read=0,total=0)) as session:
        # await start_transaction(session, pipeline_name)
        for testcase_path in testcases_paths:
            if need_to_transpile(testcase_path, cache_dir):
                # insert all inputs at once, so it would transpile in parallel
                (pipeline_id, tokens) = await enqueue_transpilation(testcase_path, pipeline_name, session)
                queued_tokens[testcase_path] = tokens
                pipeline_ids[testcase_path] = pipeline_id
        # await commit_transaction(session, pipeline_name)

        # print(f"Queued: {queued}")

        # while queued_tokens or fin_tokens:
        while queued_tokens:
            for testcase_path, tokens in {**queued_tokens}.items():
                pipeline_id = pipeline_ids[testcase_path]
                for token in list(tokens):
                    status = await fetch_ingest_status(session, pipeline_name, token)
                    match status:
                        case {'status': 'inprogress'}:
                            pass
                        case {'status': 'complete'}:
                            print(f"Insert completed: {token}")
                            tokens.remove(token)
                        case _:
                            raise Exception(f"Unknown ingest status: {status}")
                
                # fin_token = fin_tokens.get(testcase_path, None)
                # if (not tokens) and (not fin_token):
                #     # del queued[testcase_path]
                #     # pass
                #     ftokens = await insert_records(session, pipeline_name, {
                #         'all_records_inserted': [{'pipeline_id': pipeline_id}]
                #     })
                #     assert len(ftokens) == 1
                #     fin_tokens[testcase_path] = ftokens.pop()
                #     del queued_tokens[testcase_path]
                if not tokens:
                    # del queued[testcase_path]
                    # pass
                    # ftokens = await insert_records(session, pipeline_name, {
                    #     'all_records_inserted': [{'pipeline_id': pipeline_id}]
                    # })
                    # assert len(ftokens) == 1
                    # fin_tokens[testcase_path] = ftokens.pop()
                    del queued_tokens[testcase_path]

            # for testcase_path, fin_token in {**fin_tokens}.items():
            #     if fin_token:
            #         status = await fetch_ingest_status(session, pipeline_name, fin_token)
            #         match status:
            #             case {'status': 'inprogress'}:
            #                 pass
            #             case {'status': 'complete'}:
            #                 print(f"Insert finalized for {testcase_path}")
            #                 any_errors_happened = await report_errors_if_any(session, pipeline_name, pipeline_id, testcase_path)
            #                 if any_errors_happened:
            #                     with_errors[testcase_path] = True
            #                 del fin_tokens[testcase_path]
            #             case _:
            #                 raise Exception(f"Unknown ingest status: {status}")

            # print(f"Queued: {queued}")
            await asyncio.sleep(1)

        paths_without_errors = (set(testcases_paths) - set(with_errors.keys())) & set(pipeline_ids.keys())
        for testcase_path in paths_without_errors:
            dest_path = testcase_dest_path(testcase_path, cache_dir)
            pipeline_id = pipeline_ids[testcase_path]
            await write_output_sql(session, pipeline_name, pipeline_id, dest_path)

    if with_errors:
        exit(1)



if __name__ == '__main__':
    asyncio.run(main(sys.argv[1:]), debug=True)
