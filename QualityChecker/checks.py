import logging
import os

from typing import List, Any, Union, Optional
from utils.databaseTools import run_sql
from utils.utils import read_file_content, to_flat_list

path = os.path.dirname(os.path.abspath(__file__))


def check_null_fields(dialect: str, schema: str, table: str, column: str, conn_dict: dict) -> List[List[int]]:
    """
    Проверяет, является ли столбец полностью пустым.

    Функция выполняет SQL-запрос для проверки наличия непустых значений в указанном столбце таблицы.
    Если столбец полностью пуст, возвращается [[1]]. В противном случае, возвращается [[0]].

    Параметры:
    ----------
    dialect : str
        Диалект базы данных (например, 'Vertica', 'Greenplum'), который используется для выполнения запроса.
    schema : str
        Имя схемы в базе данных.
    table : str
        Имя таблицы в базе данных.
    column : str
        Имя столбца, который необходимо проверить.
    conn_dict : dict
        Словарь с параметрами подключения к базе данных.

    Возвращает:
    -----------
    List[List[int]]
        [[1]] если столбец полностью пуст, [[0]] если в столбце есть непустые значения.
    """
    script = read_file_content(
        f'{path}/sql/DQ/{dialect.lower()}/check_not_nulls_columns.sql'
    ).format(table=table, schema=schema, column=column)
    
    result = run_sql(dialect, script, conn_dict)
    
    if not result:
        logging.warning(f'Весь столбец {column} пустой')
        return [[1]]
    else:
        logging.info(f'В колонке {column} есть непустые строки')
        return [[0]]


def max_length(dialect: str, schema: str, table: str, column: str, conn_dict: dict) -> List[List[int]]:
    """
    Проверяет, достигла ли указанная колонка максимальной длины.

    Функция выполняет SQL-запрос для проверки, достигла ли колонка максимальной длины. 
    Если максимальная длина достигнута, возвращается [[1]]. В противном случае, возвращается [[0]].

    Параметры:
    ----------
    dialect : str
        Диалект базы данных (например, 'Vertica', 'Greenplum'), который используется для выполнения запроса.
    schema : str
        Имя схемы в базе данных.
    table : str
        Имя таблицы в базе данных.
    column : str
        Имя столбца, который необходимо проверить.
    conn_dict : dict
        Словарь с параметрами подключения к базе данных.

    Возвращает:
    -----------
    List[List[int]]
        [[1]] если колонка достигла максимальной длины, [[0]] если нет.
    """
    script = read_file_content(
        f'{path}/sql/DQ/{dialect.lower()}/check_max_length.sql'
    ).format(table=table, schema=schema, column=column)
    
    result_cnt = run_sql(dialect, script, conn_dict)
    
    if result_cnt[0] == [1]:
        logging.warning(f'{column} достигла максимальной длины')
        return [[1]]
    else:
        logging.info(f'Колонка {column} не достигла максимальной длины')
        return [[0]]


def not_utf8(dialect: str, schema: str, table: str, column: str, conn_dict: dict) -> List[List[int]]:
    """
    Проверяет наличие не UTF-8 символов в указанной колонке таблицы.

    Функция выполняет SQL-запрос для проверки, содержатся ли в указанной колонке символы, которые не соответствуют
    кодировке UTF-8. Если такие символы найдены, возвращается [[1]]. Если их нет, возвращается [[0]].

    Параметры:
    ----------
    dialect : str
        Диалект базы данных (например, 'Vertica', 'Greenplum'), который используется для выполнения запроса.
    schema : str
        Имя схемы в базе данных.
    table : str
        Имя таблицы в базе данных.
    column : str
        Имя столбца, который необходимо проверить.
    conn_dict : dict
        Словарь с параметрами подключения к базе данных.

    Возвращает:
    -----------
    List[List[int]]
        [[1]] если в колонке есть не UTF-8 символы, [[0]] если их нет.
    """
    script = read_file_content(
        f'{path}/sql/DQ/{dialect.lower()}/check_not_utf8.sql'
    ).format(table=table, schema=schema, column=column)
    
    result_cnt = run_sql(dialect, script, conn_dict)
    
    if not result_cnt:
        logging.info(f'В поле {column} нет UTF-8 символов')
        return [[0]]
    else:
        logging.warning(f'В поле {column} есть не UTF-8 символы')
        return [[1]]


def check_max_tech_load_ts(dialect: str, schema: str, table: str, conn_dict: dict) -> Any:
    """
    Возвращает максимальное значение поля `tech_load_ts` в указанной таблице.

    Функция выполняет SQL-запрос для получения максимального значения поля `tech_load_ts` в таблице. 
    Возвращается первое значение из результата запроса.

    Параметры:
    ----------
    dialect : str
        Диалект базы данных (например, 'Vertica', 'Greenplum'), который используется для выполнения запроса.
    schema : str
        Имя схемы в базе данных.
    table : str
        Имя таблицы в базе данных.
    conn_dict : dict
        Словарь с параметрами подключения к базе данных.

    Возвращает:
    -----------
    Any
        Значение максимальной даты `tech_load_ts` для указанной таблицы.
    """
    script = read_file_content(
        f'{path}/sql/DQ/ansi/check_max_tech_load_ts.sql'
    ).format(table=table, schema=schema)
    
    result_cnt = run_sql(dialect, script, conn_dict)
    
    return result_cnt[0]


def check_row_count(dialect: str, schema: str, table: str, conn_dict: dict) -> List[int]:
    """
    Возвращает количество строк в указанной таблице.

    Функция выполняет SQL-запрос для подсчета количества строк в таблице. 
    Результат запроса возвращается в виде списка.

    Параметры:
    ----------
    dialect : str
        Диалект базы данных (например, 'Vertica', 'Greenplum'), который используется для выполнения запроса.
    schema : str
        Имя схемы в базе данных.
    table : str
        Имя таблицы в базе данных.
    conn_dict : dict
        Словарь с параметрами подключения к базе данных.

    Возвращает:
    -----------
    List[int]
        Список, содержащий количество строк в таблице.
    """
    check_row_count_path = f'{path}/sql/DQ/ansi/check_row_count.sql'
    check_row_count_script = read_file_content(check_row_count_path).format(table=table, schema=schema)
    result = to_flat_list(run_sql(dialect, check_row_count_script, conn_dict))
    return result


def check_pk_doubles(dialect: str, schema: str, table: str, conn_dict: dict) -> Optional[int]:
    """
    Проверяет наличие дублей по первичному ключу в указанной таблице.

    Функция выполняет последовательные SQL-запросы для определения наличия первичного ключа в таблице
    и проверки дублей по этому ключу. Если дублей нет, возвращается 0, если есть — количество дублей.
    Если первичного ключа нет, возвращается `None`.

    Параметры:
    ----------
    dialect : str
        Диалект базы данных (например, 'Vertica', 'Greenplum'), который используется для выполнения запроса.
    schema : str
        Имя схемы в базе данных.
    table : str
        Имя таблицы в базе данных.
    conn_dict : dict
        Словарь с параметрами подключения к базе данных.

    Возвращает:
    -----------
    Optional[int]
        Количество дублей по первичному ключу. Возвращает `None`, если первичного ключа нет.
    """
    select_pk_path = f'{path}/sql/work_with_meta/{dialect.lower()}/select_primary_key_columns.sql'
    select_pk_script = read_file_content(select_pk_path).format(table=table, schema_name=schema)

    pk_columns = run_sql(dialect, select_pk_script, conn_dict)

    if pk_columns:
        logging.info('Первичный ключ существует')
        pk_columns_list = to_flat_list(pk_columns)
        pk_columns_str = ', '.join(pk_columns_list)

        check_pk_double_path = f'{path}/sql/DQ/ansi/check_pk_doubles.sql'
        check_pk_script = read_file_content(check_pk_double_path).format(table=table, schema=schema, pk=pk_columns_str)

        result = to_flat_list(run_sql(dialect, check_pk_script, conn_dict))
        logging.info(check_pk_script)

        if result and result[0] != 0:
            logging.warning(f"""{result[0]} шт. дублей по ключу в {table}!!!!
                                Скрипт для проверки: 
                                {check_pk_script}""")
            return result[0]
        else:
            logging.info(f'В {table} нет дублей по ключу')
            return 0
    else:
        logging.warning('Первичного ключа нет')
        return None


def check_most_consistent_value(dialect: str, schema: str, table: str, column: str, conn_dict: dict) -> List[Any]:
    """
    Определяет самое часто встречающееся значение в указанном столбце таблицы.

    Функция выполняет SQL-запрос для поиска самого часто встречающегося значения в столбце.
    Результат запроса возвращается в виде списка.

    Параметры:
    ----------
    dialect : str
        Диалект базы данных (например, 'Vertica', 'Greenplum'), который используется для выполнения запроса.
    schema : str
        Имя схемы в базе данных.
    table : str
        Имя таблицы в базе данных.
    column : str
        Имя столбца в таблице, для которого нужно найти самое часто встречающееся значение.
    conn_dict : dict
        Словарь с параметрами подключения к базе данных.

    Возвращает:
    -----------
    List[Any]
        Список с результатом запроса, содержащим самое часто встречающееся значение в столбце.
    """
    script = read_file_content(
        f'{path}/sql/DQ/{dialect.lower()}/select_most_consistent_value.sql'
    ).format(table=table, schema=schema, column=column)

    result_cnt = run_sql(dialect, script, conn_dict)
    logging.warning(f'{column} {result_cnt[0]} ')
    return result_cnt


def check_columns_length_statistics(dialect: str, schema: str, table: str, column: str, conn_dict: dict) -> List[Any]:
    """
    Проверяет статистику длины значений в указанном столбце таблицы.

    Функция выполняет SQL-запрос для получения статистики по длине значений в столбце,
    включая максимальную, минимальную и среднюю длину. Возвращает результат в виде списка.

    Параметры:
    ----------
    dialect : str
        Диалект базы данных (например, 'Vertica', 'Greenplum'), который используется для выполнения запроса.
    schema : str
        Имя схемы в базе данных.
    table : str
        Имя таблицы в базе данных.
    column : str
        Имя столбца в таблице, для которого нужно получить статистику длины значений.
    conn_dict : dict
        Словарь с параметрами подключения к базе данных.

    Возвращает:
    -----------
    List[Any]
        Список с результатом запроса, содержащим статистику длины значений в столбце.
    """
    script = read_file_content(
        f'{path}/sql/DQ/{dialect.lower()}/select_columns_length_statistics.sql'
    ).format(table=table, schema=schema, column=column)

    result_cnt = run_sql(dialect, script, conn_dict)
    logging.warning(f'{column} {result_cnt[0]} ')
    return result_cnt


def check_insert_new_rows(dialect, schema, table, conn_dict):
    ods_schema = schema
    stg_schema = schema.replace('ODS_', 'STG_')
    if bool(run_sql(dialect, f'select 1 from {stg_schema}.{table} limit 1', conn_dict)):
        select_pk_path = f'{path}/sql/work_with_meta/{dialect.lower()}/select_primary_key_columns.sql'
        select_pk_script = read_file_content(select_pk_path).format(table=table, schema_name=stg_schema)
        pk_columns = run_sql(dialect, select_pk_script,
                                 conn_dict)
        pk_columns_list = to_flat_list(pk_columns)
        pk_columns_str = ', '.join([f'{col}' for col in pk_columns_list])
        if pk_columns_str == '':
            logging.warning(f'Нет первичного ключа')
        else:
            select_bc_path = f'{path}/sql/work_with_meta/{dialect.lower()}/select_business_columns.sql'
            select_bc_script = read_file_content(select_bc_path).format(table=table, schema_name=stg_schema)
            bc_columns = run_sql(dialect, select_bc_script, conn_dict)
            bc_columns_list = to_flat_list(bc_columns)
            # формирование скрипта для измеенившихся строк пр. false or (stg.col1 <=> ods.col1) = false
            u_compare = 'False'
            for col in bc_columns_list:
                u_compare = u_compare + f' or (ods.{col} <=> stg.{col}) = False'

            e_compare = 'True'
            for col in bc_columns_list:
                e_compare = e_compare + f' and (ods.{col} <=> stg.{col})'

            # формирование строчки для on в джойне пр. true and stg.pk1 = ods.pk1
            pk_join = 'true'
            for col in pk_columns_list:
                pk_join = pk_join + f' and ods.{col} = stg.{col}'

            if bool(run_sql(dialect, 
                    f"""select 1 from columns where table_schema = \'{ods_schema}\' and table_name = \'{table}\' 
                    and column_name = \'tech_is_deleted\'""", conn_dict)):
                check_insert_new_rows_path = f'{path}/sql/DQ/{dialect.lower()}/check_insert_new_rows_with_deleted.sql'
            else:
                check_insert_new_rows_path = f'{path}/sql/DQ/{dialect.lower()}/check_insert_new_rows_wo_deleted.sql'
            check_insert_new_rows_script = read_file_content(check_insert_new_rows_path).format(table=table,
                                                                                                ods_schema=ods_schema,
                                                                                                stg_schema=stg_schema,
                                                                                                pk_join=pk_join,
                                                                                                u_compare=u_compare,
                                                                                                e_compare=e_compare,
                                                                                                pk_columns_str=pk_columns_str)

            result = to_flat_list(run_sql(dialect, check_insert_new_rows_script, conn_dict))
            if result[0]:
                logging.warning(f'Инкремент Возможно хороший')
            else:
                logging.warning(f'Какая то хуйня')
                logging.info(check_insert_new_rows_script)

def check_insert_new_rows(dialect, schema, table, conn_dict):
    ods_schema = schema
    stg_schema = schema.replace('ODS_', 'STG_')
    if bool(run_sql(dialect, f'select 1 from {stg_schema}.{table} limit 1', conn_dict)):
        select_pk_path = f'{path}/sql/work_with_meta/{dialect.lower()}/select_primary_key_columns.sql'
        select_pk_script = read_file_content(select_pk_path).format(table=table, schema_name=stg_schema)
        pk_columns = run_sql(dialect, select_pk_script,
                                 conn_dict)
        pk_columns_list = to_flat_list(pk_columns)
        pk_columns_str = ', '.join([f'{col}' for col in pk_columns_list])
        if pk_columns_str == '':
            logging.warning(f'Нет первичного ключа')
        else:
            select_bc_path = f'{path}/sql/work_with_meta/{dialect.lower()}/select_business_columns.sql'
            select_bc_script = read_file_content(select_bc_path).format(table=table, schema_name=stg_schema)
            bc_columns = run_sql(dialect, select_bc_script, conn_dict)
            bc_columns_list = to_flat_list(bc_columns)
            # формирование скрипта для измеенившихся строк пр. false or (stg.col1 <=> ods.col1) = false
            u_compare = 'False'
            for col in bc_columns_list:
                u_compare = u_compare + f' or (ods.{col} <=> stg.{col}) = False'

            e_compare = 'True'
            for col in bc_columns_list:
                e_compare = e_compare + f' and (ods.{col} <=> stg.{col})'

            # формирование строчки для on в джойне пр. true and stg.pk1 = ods.pk1
            pk_join = 'true'
            for col in pk_columns_list:
                pk_join = pk_join + f' and ods.{col} = stg.{col}'

            if bool(run_sql(dialect, 
                    f"""select 1 from columns where table_schema = \'{ods_schema}\' and table_name = \'{table}\' 
                    and column_name = \'tech_is_deleted\'""", conn_dict)):
                check_insert_new_rows_path = f'{path}/sql/DQ/{dialect.lower()}/check_insert_new_rows_with_deleted.sql'
            else:
                check_insert_new_rows_path = f'{path}/sql/DQ/{dialect.lower()}/check_insert_new_rows_wo_deleted.sql'
            check_insert_new_rows_script = read_file_content(check_insert_new_rows_path).format(table=table,
                                                                                                ods_schema=ods_schema,
                                                                                                stg_schema=stg_schema,
                                                                                                pk_join=pk_join,
                                                                                                u_compare=u_compare,
                                                                                                e_compare=e_compare,
                                                                                                pk_columns_str=pk_columns_str)

            result = to_flat_list(run_sql(dialect, check_insert_new_rows_script, conn_dict))
            if result[0]:
                logging.warning(f'Инкремент Возможно хороший')
            else:
                logging.warning(f'Какая то хуйня')
                logging.info(check_insert_new_rows_script)

def check_segmentation(dialect: str, schema: str, table: str, conn_dict: dict) -> Union[str, int]:
    """
    Проверяет сегментацию данных в таблице по нодам, используя первичный ключ для анализа распределения данных.

    Функция выполняет проверку наличия данных в таблице и анализирует распределение данных по нодам
    на основе первичного ключа. Если таблица пуста, возвращает 'Пустая'. Если первичный ключ отсутствует, 
    выводит предупреждение. Возвращает процент уникальности сегментации, если проверка прошла успешно.

    Параметры:
    ----------
    dialect : str
        Диалект базы данных (например, 'Vertica', 'Greenplum'), который используется для выполнения запроса.
    schema : str
        Имя схемы в базе данных.
    table : str
        Имя таблицы в базе данных.
    conn_dict : dict
        Словарь с параметрами подключения к базе данных.

    Возвращает:
    -----------
    Union[str, int]
        Процент уникальности сегментации, если проверка прошла успешно, или строку 'Пустая', если таблица не содержит данных.
    """
    # Проверка наличия данных в таблице
    if bool(run_sql(dialect, f'select 1 from {schema}.{table} limit 1', conn_dict)):
        # Получение первичных ключей
        select_pk_path = f'{path}/sql/work_with_meta/{dialect.lower()}/select_primary_key_columns.sql'
        select_pk_script = read_file_content(select_pk_path).format(table=table, schema_name=schema)
        pk_columns = run_sql(dialect, select_pk_script, conn_dict)
        pk_columns_list = to_flat_list(pk_columns)
        pk_columns_str = ', '.join([f'{col}' for col in pk_columns_list])

        if pk_columns_str == '':
            logging.warning(f'Нет первичного ключа')
        else:
            # Проверка сегментации
            check_pk_double_path = f'{path}/sql/DQ/{dialect.lower()}/check_segmentation.sql'
            check_segmentation_script = read_file_content(check_pk_double_path).format(table=table, schema=schema, pk=pk_columns_str)

            result = to_flat_list(run_sql(dialect, check_segmentation_script, conn_dict))
            logging.warning(f'Уникальные проценты сегментации в нодах {result[0]}')
            return result[0]
    else:
        return 'Пустая'


def check_bussines_key_counts(dialect: str, schema: str, table: str, conn_dict: dict) -> Union[int, None]:
    """
    Проверяет количество уникальных бизнес-ключей в таблице.

    Функция выполняет SQL-запрос для получения списка первичных ключей таблицы и затем выполняет
    проверку количества уникальных значений бизнес-ключей, исключая поле `tech_load_ts` из списка ключей,
    если оно присутствует. Возвращает количество уникальных бизнес-ключей, если первичный ключ присутствует,
    или выводит предупреждение, если первичный ключ отсутствует.

    Параметры:
    ----------
    dialect : str
        Диалект базы данных (например, 'Vertica', 'Greenplum'), который используется для выполнения запроса.
    schema : str
        Имя схемы в базе данных.
    table : str
        Имя таблицы в базе данных.
    conn_dict : dict
        Словарь с параметрами подключения к базе данных.

    Возвращает:
    -----------
    Union[int, None]
        Количество уникальных бизнес-ключей, если первичный ключ присутствует, или `None`, если первичный ключ отсутствует.
    """
    select_pk_path = f'{path}/sql/work_with_meta/{dialect.lower()}/select_primary_key_columns.sql'
    select_pk_script = read_file_content(select_pk_path).format(table=table, schema_name=schema)

    pk_columns = run_sql(dialect, select_pk_script, conn_dict)
    if bool(pk_columns):
        logging.info('Первичный ключ есть')
        pk_columns_list = to_flat_list(pk_columns)
        pk_columns_str = ', '.join([f'{col}' for col in pk_columns_list])
        logging.info(pk_columns_str)

        # Удаление tech_load_ts из списка ключей, если оно присутствует
        pk_columns_str_wo_ts = pk_columns_str.replace(', tech_load_ts', '')
        logging.info(pk_columns_str_wo_ts)

        check_bussines_key_counts_path = f'{path}/sql/DQ/ansi/check_bussines_key_counts.sql'
        check_bussines_key_counts_script = read_file_content(check_bussines_key_counts_path).format(
            table=table, schema=schema, pk=pk_columns_str_wo_ts
        )

        result = to_flat_list(run_sql(dialect, check_bussines_key_counts_script, conn_dict))
        return result[0]
    else:
        logging.warning(f'Первичного ключа нет')
        return None
