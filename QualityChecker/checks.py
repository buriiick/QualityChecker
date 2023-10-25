import logging
import os

from utils.databaseTools import vertica_sql
from utils.utils import read_file_content, to_flat_list

path = os.path.join(os.path.abspath(os.path.dirname(__file__)))


def check_null_fields(schema, table, column, vertica_conn_dict):
    script = read_file_content(
        f'{path}/sql/DQ/check_not_nulls_columns.sql').format(
        table=table, schema=schema, column=column)
    result = vertica_sql(script, vertica_conn_dict)
    if not result:
        logging.warning(f'Весь столбец {column} пустой')
        return [[1]]
    else:
        logging.info(f'В колонке {column} есть непустые строки')
        return [[0]]


def max_length(schema, table, column, vertica_conn_dict):
    script = read_file_content(
        f'{path}/sql/DQ/check_max_length.sql').format(
        table=table, schema=schema, column=column)
    result_cnt = vertica_sql(script, vertica_conn_dict)
    if result_cnt[0] == [1]:
        logging.warning(f'{column} достигла максимальной длины')
        return [[1]]
    else:
        logging.info(f'Колонка {column} не достигла максимальной длины')
        return [[0]]


def check_max_tech_load_ts(schema, table, vertica_conn_dict):
    script = read_file_content(
        f'{path}/sql/DQ/check_max_tech_load_ts.sql').format(
        table=table, schema=schema)
    result_cnt = vertica_sql(script, vertica_conn_dict)
    return result_cnt[0]


def check_row_count(schema, table, vertica_conn_dict):
    check_row_count_path = f'{path}/sql/DQ/check_row_count.sql'
    check_row_count_script = (read_file_content(check_row_count_path).
                              format(table=table,
                                     schema=schema))
    result = to_flat_list(vertica_sql(check_row_count_script, vertica_conn_dict))
    return result


def check_pk_doubles(schema, table, vertica_conn_dict):
    select_pk_path = f'{path}/sql/work_with_meta/vertica/select_primary_key_columns.sql'
    select_pk_script = (read_file_content(select_pk_path).
                        format(table=table,
                               schema_name=schema))

    pk_columns = vertica_sql(select_pk_script,
                             vertica_conn_dict)
    if bool(pk_columns):
        logging.info('Первичный ключ есть')
        pk_columns_list = to_flat_list(pk_columns)
        pk_columns_str = ', '.join([f'{col}' for col in pk_columns_list])

        check_pk_double_path = f'{path}/sql/DQ/check_pk_doubles.sql'
        check_pk_script = read_file_content(check_pk_double_path).format(table=table, schema=schema, pk=pk_columns_str)

        result = to_flat_list(vertica_sql(check_pk_script, vertica_conn_dict))
        if bool(result) and result[0] != 0:
            logging.warning(f"""{result} шт дублей по ключу в {table}!!!!
                                    Скрипт для проверки: 
                                    {check_pk_script}""")
            return result[0]
        else:
            logging.info(f'В {table} нет дублей по ключу')
            return result[0]
    else:
        logging.warning(f'Первичного ключа нет')


def not_utf8(schema, table, column, vertica_conn_dict):
    script = read_file_content(
        f'{path}/sql/DQ/check_not_utf8.sql').format(
        table=table, schema=schema, column=column)
    result_cnt = vertica_sql(script, vertica_conn_dict)
    if not result_cnt:
        logging.info(f'В поле {column} нет UTF-8 символов')
        return [[0]]
    else:
        logging.warning(f'{column} В поле есть не UTF-8 символы')
        return [[1]]


def check_most_consistent_value(schema, table, column, vertica_conn_dict):
    script = read_file_content(
        f'{path}/sql/DQ/select_most_consistent_value.sql').format(
        table=table, schema=schema, column=column)
    result_cnt = vertica_sql(script, vertica_conn_dict)
    logging.warning(f'{column} {result_cnt[0]} ')
    return result_cnt


def check_columns_length_statistics(schema, table, column, vertica_conn_dict):
    script = read_file_content(
        f'{path}/sql/DQ/select_columns_length_statistics.sql').format(
        table=table, schema=schema, column=column)
    result_cnt = vertica_sql(script, vertica_conn_dict)
    logging.warning(f'{column} {result_cnt[0]} ')
    return result_cnt


def check_insert_new_rows(schema, table, vertica_conn_dict):
    ods_schema = schema
    stg_schema = schema.replace('ODS_', 'STG_')
    if bool(vertica_sql(f'select 1 from {stg_schema}.{table} limit 1', vertica_conn_dict)):
        select_pk_path = f'{path}/sql/work_with_meta/vertica/select_primary_key_columns.sql'
        select_pk_script = read_file_content(select_pk_path).format(table=table, schema_name=stg_schema)
        pk_columns = vertica_sql(select_pk_script,
                                 vertica_conn_dict)
        pk_columns_list = to_flat_list(pk_columns)
        pk_columns_str = ', '.join([f'{col}' for col in pk_columns_list])
        if pk_columns_str == '':
            logging.warning(f'Нет первичного ключа')
        else:
            select_bc_path = f'{path}/sql/work_with_meta/vertica/select_business_columns.sql'
            select_bc_script = read_file_content(select_bc_path).format(table=table, schema_name=stg_schema)
            bc_columns = vertica_sql(select_bc_script, vertica_conn_dict)
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

            if bool(vertica_sql(
                    f"""select 1 from columns where table_schema = \'{ods_schema}\' and table_name = \'{table}\' 
                    and column_name = \'tech_is_deleted\'""", vertica_conn_dict)):
                check_insert_new_rows_path = f'{path}/sql/DQ/check_insert_new_rows_with_deleted.sql'
            else:
                check_insert_new_rows_path = f'{path}/sql/DQ/check_insert_new_rows_wo_deleted.sql'
            check_insert_new_rows_script = read_file_content(check_insert_new_rows_path).format(table=table,
                                                                                                ods_schema=ods_schema,
                                                                                                stg_schema=stg_schema,
                                                                                                pk_join=pk_join,
                                                                                                u_compare=u_compare,
                                                                                                e_compare=e_compare,
                                                                                                pk_columns_str=pk_columns_str)

            result = to_flat_list(vertica_sql(check_insert_new_rows_script, vertica_conn_dict))
            if result[0]:
                logging.warning(f'Инкремент Возможно хороший')
            else:
                logging.warning(f'Какая то хуйня')
                print(check_insert_new_rows_script)

def check_insert_new_rows(schema, table, vertica_conn_dict):
    ods_schema = schema
    stg_schema = schema.replace('ODS_', 'STG_')
    if bool(vertica_sql(f'select 1 from {stg_schema}.{table} limit 1', vertica_conn_dict)):
        select_pk_path = f'{path}/sql/work_with_meta/vertica/select_primary_key_columns.sql'
        select_pk_script = read_file_content(select_pk_path).format(table=table, schema_name=stg_schema)
        pk_columns = vertica_sql(select_pk_script,
                                 vertica_conn_dict)
        pk_columns_list = to_flat_list(pk_columns)
        pk_columns_str = ', '.join([f'{col}' for col in pk_columns_list])
        if pk_columns_str == '':
            logging.warning(f'Нет первичного ключа')
        else:
            select_bc_path = f'{path}/sql/work_with_meta/vertica/select_business_columns.sql'
            select_bc_script = read_file_content(select_bc_path).format(table=table, schema_name=stg_schema)
            bc_columns = vertica_sql(select_bc_script, vertica_conn_dict)
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

            if bool(vertica_sql(
                    f"""select 1 from columns where table_schema = \'{ods_schema}\' and table_name = \'{table}\' 
                    and column_name = \'tech_is_deleted\'""", vertica_conn_dict)):
                check_insert_new_rows_path = f'{path}/sql/DQ/check_insert_new_rows_with_deleted.sql'
            else:
                check_insert_new_rows_path = f'{path}/sql/DQ/check_insert_new_rows_wo_deleted.sql'
            check_insert_new_rows_script = read_file_content(check_insert_new_rows_path).format(table=table,
                                                                                                ods_schema=ods_schema,
                                                                                                stg_schema=stg_schema,
                                                                                                pk_join=pk_join,
                                                                                                u_compare=u_compare,
                                                                                                e_compare=e_compare,
                                                                                                pk_columns_str=pk_columns_str)

            result = to_flat_list(vertica_sql(check_insert_new_rows_script, vertica_conn_dict))
            if result[0]:
                logging.warning(f'Инкремент Возможно хороший')
            else:
                logging.warning(f'Какая то хуйня')
                print(check_insert_new_rows_script)

def check_segmentation(schema, table, vertica_conn_dict):
    if bool(vertica_sql(f'select 1 from {schema}.{table} limit 1', vertica_conn_dict)):
        select_pk_path = f'{path}/sql/work_with_meta/vertica/select_primary_key_columns.sql'
        select_pk_script = read_file_content(select_pk_path).format(table=table, schema_name=schema)
        pk_columns = vertica_sql(select_pk_script,
                                 vertica_conn_dict)
        pk_columns_list = to_flat_list(pk_columns)
        pk_columns_str = ', '.join([f'{col}' for col in pk_columns_list])
        if pk_columns_str == '':
            logging.warning(f'Нет первичного ключа')
        else:
            check_pk_double_path = f'{path}/sql/DQ/check_segmentation.sql'
            check_segmentation_script = read_file_content(check_pk_double_path).format(table=table, schema=schema,
                                                                                       pk=pk_columns_str)

            result = to_flat_list(vertica_sql(check_segmentation_script, vertica_conn_dict))
            logging.warning(f'Уникальные проценты сегментации в нодах {result[0]}')
            return result[0]
    else:
        return 'Пустая'


"""def main_check(schema, table, all_columns_list, check_list, connection):
    check_type = 'all_col'
    for check in check_list:
        if check_type == 'table':
            exec(f'result = to_flat_list({check}({schema},{table}, {connection}))')
            exec(f'{check}_df.append({result}[0])')
        if check_type == 'all_col':
            for col in all_columns_list:
                cnt = 0
                print(f'result = to_flat_list({check}(\'{schema}\',\'{table}\',\'{col}\',{connection}))')
                result = eval(f'to_flat_list({check}(\'{schema}\',\'{table}\',\'{col}\',{connection}))')
                if result[0] == 1:
                    cnt = cnt + 1
                exec(f'count_{check}_df.appencd({cnt})')
                exec(f'{check}_df.append({result}[0])')
        if check_type == 'text_cols':
            for col in all_columns_list:
                if col in text_columns_list:
                    cnt = 0
                    exec(f'result = to_flat_list({check}({schema},{table},{col},{connection}))')
                    if result[0] == 1:
                        cnt = cnt + 1
                    exec(f'count_{check}_df.appencd({cnt})')
                    exec(f'{check}_df.append({result}[0])')"""
