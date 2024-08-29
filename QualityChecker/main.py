"""
QualityChecker - утилита для автоматизированной проверки таблиц ODS и STG на стандартные виды ошибок
"""
import logging
import os
import time

from conf import vertica_conn_dict, greenplum_conn_dict
from utils.databaseTools import run_sql
from utils.utils import read_file_content
from DataQuality import DataQuality

# -------------------------------------------------------------
# Доступные проверки

# 1. Дубли по ключам
# 2. Полностью пустые столбцы (NULL или '')
# 3. Текстовые поля длина которых достигла максимума
# 4. Наличие кривых символов (не utf-8)
# 5. Какая макс. tech_load_ts в ODS
# 6. Проверка корректности инкремента (тестируется, только для Vertica)
# 7. Статистика самых часто встречающихся значений в поле и их доля от всех.
# 8. Статистика длин текстовых полей. varchar самого большого значения и максимальный.
# 9. Сегментация
# 10. Количество строк в STG.
# 11. Количество строк в ODS.
# 12. Кол-во уникальных ключей без tech_load_ts
# 13. Дубли по ключу в stg
# 14. Какая макс. tech_load_ts в ODS в STG
# -------------------------------------------------------------


def get_tables(path: str, query: str) -> list:
    """
    Выполняет SQL-запрос для получения списка таблиц и возвращает результат.

    Если передан параметр `query`, он используется для выполнения запроса. 
    Если `query` равен `None`, функция загружает SQL-запрос из файла `get_tables_sql_query.sql`, 
    расположенного в указанной директории.

    Параметры:
    ----------
    path : str
        Путь к директории, где находится SQL-запрос, если `query` не указан.
    query : str
        SQL-запрос для выполнения. Если `None`, используется запрос из файла.

    Возвращает:
    -----------
    list
        Список таблиц, полученных в результате выполнения SQL-запроса.
    """
    if query == None:
        path = os.path.dirname(os.path.abspath(__file__))
        sql_query = read_file_content(f'{path}/get_tables_sql_query.sql')
    else:
        sql_query = query
    obj_list = run_sql(
        dialect,
        sql_query,
        connection)
    return obj_list


def main(query: str, ENV: str, checks_list: list):
    path = os.path.dirname(os.path.abspath(__file__))
    obj_list = get_tables(path=path, query=query)
    empty_tables = []
    b = time.strftime("%Y-%m-%d_%H-%M")
    report_name = f'{ENV}_report'
    logging.info(obj_list)
    for obj in obj_list:
        table_obj = DataQuality(obj)
        table_obj.checks = checks_list
        logging.info(f'Начало проверки таблицы  {table_obj.schema}.{table_obj.table}  select analyze_statistics(\'{table_obj.schema}.{table_obj.table}\')')
        logging.info(time.strftime("%Y-%m-%d %H:%M"))
        script = read_file_content(
        f'{path}/sql/work_with_meta/{dialect.lower()}/analyze_statistics.sql').format(
        table=table_obj.table, schema=table_obj.schema)
        run_sql(dialect, script, conn_dict=connection)
        if bool(run_sql(dialect, f'select 1 from {table_obj.schema}.{table_obj.table} limit 1', connection)):
            table_obj.execute_checks(dialect, path, connection)
            table_obj.create_xlsx(path, report_name, b)
        else:
            logging.warning(f'Таблица {table_obj.schema}.{table_obj.table} пустая')
            empty_tables.append(f'{table_obj.schema}.{table_obj.table}')
            logging.info(empty_tables)

    logging.info(f'Check Results in `QualityChecker/reports/{report_name}_{b}.xlsx')
    logging.info(empty_tables)
    logging.info(b)
    logging.info(time.strftime("%Y-%m-%d_%H-%M"))


if __name__ == '__main__':
    global connection, dialect
    ENV = ''
    dialect = ''
    checks_list = []
    print("Какое окружение?\n",
          "[1] DEV\n",
          "[2] TEST\n",
          "[3] PROD\n",)
    env_no = int(input())
    if env_no == 1:
        ENV = 'DEV'
    elif env_no == 2:
        ENV = 'TEST'
    elif env_no == 3:
        ENV = 'PROD'
    else:
        raise TypeError
    print("Какой диалект нужен?\n",
        "[1] - Vertica\n",
        "[2] - Greenplum\n")
    response = int(input())
    if response == 1:
        dialect = 'Vertica'
        connection = vertica_conn_dict[ENV]
    elif response == 2:
        dialect = 'Greenplum'
        connection = greenplum_conn_dict[ENV]
    else:
        raise TypeError
    print("Перечислите через запятую номера проверок\n",
          "0. Все\n"
          "1. Дубли по ключам\n",
          "2. Полностью пустые столбцы (NULL или '')\n",
          "3. Текстовые поля длина которых достигла максимума\n",
          "4. Наличие кривых символов (не utf-8)\n",
          "5. Какая макс. tech_load_ts в ODS\n",
          "6. Проверка корректности инкремента (тестируется)\n",
          "7. Статистика самых часто встречающихся значений в поле и их доля от всех.\n",
          "8. Статистика длин текстовых полей. varchar самого большого значения и максимальный.\n",
          "9. Сегментация\n",
          "10. Количество строк в STG.\n",
          "11. Количество строк в ODS.\n",
          "12. Кол-во уникальных ключей без tech_load_ts\n",
          "13. Дубли по ключу в stg\n",
          "14. Какая макс. tech_load_ts в ODS в STG\n")
    checks_list = list(map(int, input().split(',')))
    if 0 in checks_list:
        checks_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]

    print("Вы хотите использовать скрипт для получения таблиц из директории или ввести его здесь?\n",
          "[1] - Из директории",
          "[2] - Ввести тут",)
    response = int(input())
    if response == 1:
        query = None
    elif response == 2:
        print("Введите запрос. Запрос должен быть введен одной строкой.")
        query = str(input())
    else:
        raise TypeError
    logging.info(ENV, dialect, checks_list)
    main(query=query, ENV=ENV, checks_list=checks_list)
