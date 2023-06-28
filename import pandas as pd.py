import pandas as pd
import zipfile
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy import URL
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy import text
import json
import sqlite3
 
connection = sqlite3.connect('hw1.db') # Создание базы данных в файле test.db
cursor = connection.cursor() # Создаем курсор
# Оператор SQL для создания таблицы
create_names_table = """
CREATE TABLE IF NOT EXISTS telecom_companies(
    inn INTEGER,
    name TEXT,
    kpp INTEGER,
    ogrn INTEGER,
    okved INTEGER 
)
"""
cursor.execute(create_names_table) # Запускаем команду создания таблицы
connection.commit() # Фиксируем изменения
# Оператор SQL для вставки данных в таблицу 
insert_data = """
INSERT INTO telecom_companies (inn, name, kpp, ogrn, okved)
VALUES (?, ?, ?, ?, ?)
"""
# проходим по каждому файлу и добавляем данные в DataFrame
with zipfile.ZipFile('egrul.json1.zip', 'r') as f:
    file_names = f.namelist()
    for name in file_names:
        dt = pd.read_json(f.open(name))
        for i in range(len(fl)):
            try:
                if dt['data'][i]['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'] in okved_60:
                    okved = dt['data'][i]['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД']
                    name = dt['name'][i]
                    inn = dt['inn'][i]
                    ogrn = dt['ogrn'][i]
                    kpp = dt['kpp'][i]
                    row = (int(inn)),name, okved,(int(ogrn)),(int(kpp))
                    cursor.execute(insert_rows,row)
                    connection.commit()
            except Exception:
                
                print('error')
cursor.close()    
connection.close