import json
import zipfile
import logging
import requests
import asyncio
from datetime import datetime, timedelta
from collections import Counter
from aiohttp import TCPConnector, ClientSession
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

default_args = {
    'owner': 'Marseille',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

def telecom_companies() -> None:
    sqlite_hook = SqliteHook(sqlite_get_conn='sqlite_Marseille')
    path_to_file = '/home/rtstudent/egrul.json.zip'
    count = 0
    with zipfile.ZipFile(path_to_file, 'r') as zf:
        file_names = zf.namelist()
        for name in file_names:
            with zf.open(name) as f:
                spr = f.read()
                data = json.loads(spr)
                for item in data:
                    try:
                        if item['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'][0:2] == '61.':
                            if item['inn'] == "":
                                item['inn'] = 0
                            okved = item['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'],
                            inn = item['inn'],
                            name = item['name'],
                            ogrn = item['ogrn'],
                            kpp = item['kpp'],
                            rows = [(okved, inn, name, ogrn, kpp), ]
                            fields = ['okved', 'inn', 'name', 'ogrn', 'kpp']
                            sqlite_hook.insert_rows(
                                table='telecom_companies',
                                rows=rows,
                                target_fields=fields
                            )
                            count += 1

                    except:
                        print('Что-то пошло не так!')
                        continue


def process_vacancies_data() -> None:
    def id_vac() -> list:
        ids = []
        for i in range(20):
            url_params = url_params = {
                "text": "middle python developer",
                "search_field": "name",
                "industry": "9",
                "per_page": "100",
                "page": f'{str(i)}'
            }
            result = requests.get('https://api.hh.ru/vacancies', params=url_params)
            vacancies = result.json().get('items')
            for vacancy in vacancies:
                ids.append(vacancy['id'])
        return ids
    async def get_vacancy(id: str,
                          session: ClientSession) -> json:
        url = f'/vacancies/{id}'
        async with session.get(url=url) as responce:
            vacancy_json = await responce.json()
            return vacancy_json

    async def main(list_id: list,
                   url: str = 'https://api.hh.ru/') -> None:
        connector = TCPConnector(limit=4)
        async with ClientSession(url, connector=connector) as session:
            task_list = []
            for id in list_id:
                task_list.append(asyncio.create_task(get_vacancy(id, session)))
            results = await asyncio.gather(*task_list)
        count = 1
        for result in results:
            try:
                key_skills_list = []
                company_name = result['employer']['name']
                position = result['name']
                job_description = result['description']
                key_skills = result['key_skills']
                for skill in key_skills:
                    skill = skill['name']
                    key_skills_list.append(skill)
                key_skills_for_write = ', '.join(key_skills_list)
                if key_skills_for_write == '':
                    continue
                else:
                    with open('Key_skills.txt', 'a', encoding='UTF-8') as file:
                        file.write(key_skills_for_write)
                    rows = [(company_name, position, job_description, key_skills_for_write), ]
                    fields = ['company_name', 'position', 'job_description', 'key_skills']
                    sqlite_hook.insert_rows(
                        table='vacancies',
                        rows=rows,
                        target_fields=fields
                    )
                    count += 1
                    if count == 101:
                        return

            except:
                print('Что-то пошло не так!')
                continue
    sqlite_hook = SqliteHook(sqlite_get_conn='sqlite_Marseille')
    ids = id_vac()
    asyncio.run(main(ids))
def key_skills() -> None:
    logger = logging.getLogger(__name__)
    with open('Key_skills.txt', 'r', encoding='UTF-8') as file:
        logger.info('Файл прочитан')
        top = dict(Counter(list(map(str, file.read().split(',')))))
        logger.info(f'ТОП скиллов: {sorted(top.items(), key=lambda x: x[1], reverse=True)}')
with DAG(
    dag_id='Marseille',
    default_args=default_args,
    description='Project',
    start_date=datetime(2023, 8, 10),
    schedule_interval='@daily'
) as dag:
    task1 = SqliteOperator(
        task_id='task1',
        sqlite_conn_id='sqlite_Marseille',
        sql='''CREATE TABLE IF NOT EXISTS telecom_companies (okved varchar,
                                                             inn bigint,
                                                             name varchar,
                                                             ogrn varchar,
                                                             kpp bigint);'''
    )
    task2 = SqliteOperator(
        task_id='task2',
        sqlite_conn_id='sqlite_Marseille',
        sql='''CREATE TABLE IF NOT EXISTS vacancies (company_name varchar,
                                                        position varchar,
                                                        job_description text,
                                                        key_skills varchar);'''
    )
    task3 = PythonOperator(
        task_id='task3',
        python_callable=telecom_companies,
    )
    task4 = PythonOperator(
        task_id='task4',
        python_callable=process_vacancies_data,
    )
    task5 = PythonOperator(
        task_id='task5',
        python_callable=key_skills,
    )

    [task1, task2] >> task3 >> task4 >> task5