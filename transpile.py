import os
import sys
import json5
import asyncio
import aiohttp



from example_source_dsl import get_rules
from dsl import rules_to_records, schemas_to_records



async def do_need_to_recompile_transpiler(session, pipeline_name, curr_transpiler_sql):
    url = f'/v0/pipelines/{pipeline_name}'
    async with session.get(url) as resp:
        json_resp = await resp.json()
        match json_resp:
            case {'error_code': 'UnknownPipelineName'}:
                print("Transpiler pipeline does not exist")
                return True
            case {'program_code': program_code} if program_code != curr_transpiler_sql:
                return True
    return False



async def fetch_pipeline_status(session, pipeline_name):
    url = f'/v0/pipelines/{pipeline_name}'
    async with session.get(url, params={'selector': 'status'}) as resp:
        return await resp.json()



async def recompile_transpiler(session, pipeline_name, transpiler_sql):
    status = await fetch_pipeline_status(session, pipeline_name)
    # print(f"STATUS: {status}")
    has_prev_version = not (status.get('error_code', None) == 'UnknownPipelineName')
    if has_prev_version:
        if status['deployment_status'] == 'Running':
            async with session.post(f'/v0/pipelines/{pipeline_name}/stop', params={'force': 'true'}) as resp:
                if resp.status not in [200, 202]:
                    body = await resp.text()
                    raise Exception(f"Unexpected response {resp.status}: {body}")

        while True:
            status = await fetch_pipeline_status(session, pipeline_name)
            if status['deployment_status'] == 'Stopped':
                break
            await asyncio.sleep(1)
        
        async with session.post(f'/v0/pipelines/{pipeline_name}/clear') as resp:
            if resp.status not in [200, 202]:
                body = await resp.text()
                raise Exception(f"Unexpected response {resp.status}: {body}")

        while True:
            status = await fetch_pipeline_status(session, pipeline_name)
            if status['storage_status'] == 'Cleared':
                break
            await asyncio.sleep(1)

    url = f'/v0/pipelines/{pipeline_name}'
    async with session.put(url, json={'program_code': transpiler_sql, 'name': pipeline_name}) as resp:
        if resp.status not   in [200, 201]:
            body = await resp.text()
            raise Exception(f"Unexpected response {resp.status}: {body}")
    print("Waiting for transpiler to compile")



async def wait_till_transpiler_compiled(session, pipeline_name):
    while True:
        status = await fetch_pipeline_status(session, pipeline_name)
        # print(f"Status: {status}")
        match status['program_status']:
            case 'Success':
                break
            case 'Pending' | 'CompilingSql' | 'SqlCompiled' | 'CompilingRust':
                pass
            case 'SqlError' | 'RustError' | 'SystemError':
                raise Exception("Transpiler failed to compile")
            case program_status:
                raise Exception(f"Unknown transpiler status: {program_status}")
        await asyncio.sleep(1)



async def ensure_transpiler_started(session, pipeline_name):
    url = f'/v0/pipelines/{pipeline_name}/start'
    async with session.post(url) as resp:
        if resp.status not in [200, 201, 202]:
            body = await resp.text()
            raise Exception(f"Unexpected response {resp.status}: {body}")
    while True:
        status = await fetch_pipeline_status(session, pipeline_name)
        if status['deployment_status'] == 'Running':
            break
        await asyncio.sleep(1)

def read_transpiler_sql():
    curr_dir = os.path.abspath(os.path.dirname(__file__))
    # select all *.sql files from transpiler/ directory
    # sort by name. Read in order, concatenate content and return
    sql_files = [f for f in os.listdir(f'{curr_dir}/transpiler') if f.endswith('.sql')]
    sql_files.sort()
    sql_files = [open(f'{curr_dir}/transpiler/{f}', 'r').read() for f in sql_files]
    return '\n'.join(sql_files)

async def ensure_transpiler_pipeline_is_ready(session, pipeline_name):
    curr_dir = os.path.abspath(os.path.dirname(__file__))
    transpiler_sql = read_transpiler_sql()
    # retrieve current version of program_code for transpiler pipeline
    # is it is not the same as on on the disc, recompile it
    if (await do_need_to_recompile_transpiler(session, pipeline_name, transpiler_sql)):
        await recompile_transpiler(session, pipeline_name, transpiler_sql)
    print("Waiting for transpiler to be ready")
    await wait_till_transpiler_compiled(session, pipeline_name)
    await ensure_transpiler_started(session, pipeline_name)



async def insert_records(session, pipeline_name, records):
    url = f'/v0/pipelines/{pipeline_name}/start_transaction'
    async with session.post(url) as resp:
        if resp.status not in [200, 201]:
            body = await resp.text()
            raise Exception(f"Unexpected response {resp.status}: {body}")

    for table_name, rows in records.items():
        url = f'/v0/pipelines/{pipeline_name}/ingress/{table_name}'
        params = {'update_format': 'raw', 'array': 'true', 'format': 'json'}
        async with session.post(url, params=params, json=rows) as resp:
            if resp.status not in [200, 201]:
                body = await resp.text()
                raise Exception(f"Unexpected response {resp.status}: {body}")
            json_resp = await resp.json()
            # print(f"Inserted records to table {table_name}: {json_resp}")

    url = f'/v0/pipelines/{pipeline_name}/commit_transaction'
    async with session.post(url) as resp:
        if resp.status not in [200, 201]:
            body = await resp.text()
            raise Exception(f"Unexpected response {resp.status}: {body}")



async def wait_till_complete(session, pipeline_id):
    pass



async def main(app_schema_path):
    feldera_url = 'http://localhost:8080'
    pipeline_name = 'transpiler'

    # scripts_dir = os.path.abspath(os.path.dirname(__file__))
    # framework_schema = json5.load(open(f'{scripts_dir}/../schema.json5', 'r'))
    app_schema = json5.load(open(app_schema_path, 'r'))
    rules = get_rules()

    # tables1 = framework_schema['tables']
    tables2 = app_schema['tables']
    # if set(tables1.keys()) & set(tables2.keys()):
    #     raise Exception(f"Conflicting tables names: {set(tables1.keys()) & set(tables2.keys())}")

    async with aiohttp.ClientSession(feldera_url, timeout=aiohttp.ClientTimeout(sock_read=0,total=0)) as session:
        await ensure_transpiler_pipeline_is_ready(session, pipeline_name)
        records1, pipeline_id = rules_to_records(rules)
        # records2 = schemas_to_records({**tables1, **tables2}, pipeline_id)
        records2 = schemas_to_records(tables2, pipeline_id)
        # print(f"SCHEMA RECORDS: {json.dumps(records2)}")
        records = {**records2, **records1}
        # for table_name, rows in records.items():
        #     print(f"Table: {table_name}")
        #     with open(f'{table_name}.json', 'w') as f:
        #         f.write(json.dumps(rows))
        await insert_records(session, pipeline_name, records)
        # await insert_records(session, pipeline_name, records1)
        await wait_till_complete(session, pipeline_id)



if __name__ == '__main__':
    asyncio.run(main(sys.argv[1]), debug=True)
