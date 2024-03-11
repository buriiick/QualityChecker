"""
hghhcgfchgbgh


"""
import logging
import os
import time
import traceback

import pandas

from checks import max_length, check_pk_doubles, not_utf8, check_insert_new_rows, \
    check_most_consistent_value, check_columns_length_statistics, check_max_tech_load_ts, check_row_count, \
    check_null_fields, check_segmentation, check_bussines_key_counts  # , main_check
from conf import vertica_conn_dict
from utils.databaseTools import run_sql, select_columns
from utils.utils import to_flat_list, read_file_content

if __name__ == '__main__':
    print('Погнали')

# connection = cfg.connection
ENV = 'DEV'
connection = vertica_conn_dict[ENV]

"""check_list = ['max_length', 'check_pk_doubles', 'not_utf8', 'check_insert_new_rows',
              'check_most_consistent_value', 'check_columns_length_statistics', 'check_max_tech_load_ts',
              'check_row_count', 'check_null_fields']"""
check_list = ['check_most_consistent_value', 'check_null_fields']
check_type_list = ['all_cols']

# 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11....2, 3, 4, 5, 6, 7, 8, 9,
# Ниже перечислить номера проверок
dialect = 'Vertica'
checks = [2, 3, 5, 8, 10, 13, 1, 11, 12, 9, 14]
# checks = [10]
# 1. Дубли по ключам
# 2. Полностью пустые столбцы (NULL или '')
# 3. Текстовые поля длина которых достигла максимума
# 4. Наличие кривых символов (не utf-8)
# 5. Какая макс. tech_load_ts в ODS
# 6. Проверка корректности инкремента (тестируется)
# 7. Статистика самых часто встречающихся значений в поле и их доля от всех.
# 8. Статистика длин текстовых полей. varchar самого большого значения и максимальный.
# 9. Сегментация
# 10. Количество строк в STG.
# 11. Количество строк в ODS.
# 12. Кол-во уникальных ключей без tech_load_ts
# 13. Дубли по ключу в stg
# 14. Какая макс. tech_load_ts в ODS в STG


# -------------------------------------------------------------
# path = os.path.join(os.path.abspath(os.path.dirname(__file__)))
path = os.path.dirname(os.path.abspath(__file__))
sql_query = read_file_content(f'{path}/get_tables_sql_query.sql')

obj_list = run_sql(
    dialect,
    sql_query,
    connection)

empty_tables = []
b = time.strftime("%Y-%m-%d_%H-%M")
report_name = f'{ENV}_report'

print(obj_list)
for obj in obj_list:
    schema = obj[0]
    table = obj[1]
    all_columns_df = []
    unique = []
    check_not_null_fields_result_df = []
    check_stat_value_result_df = []

    print(f'Начало проверки таблицы  {schema}.{table}  select analyze_statistics(\'{schema}.{table}\')')
    print(time.strftime("%Y-%m-%d %H:%M"))
    run_sql(dialect, f'select analyze_statistics(\'{schema}.{table}\')', connection)

    schema_df = []
    table_df = []
    col_df = []
    col_schema_df = []
    col_table_df = []

    check_pk_doubles_df = []
    check_max_tech_load_ts_df = []
    count_not_utf8_cols_df = []
    count_null_cols_df = []
    count_max_length_df = []
    check_row_count_ods_df = []
    check_row_count_stg_df = []
    check_segmentation_df = []
    check_bussines_key_counts_df = []
    check_stg_pk_doubles_df = []
    check_max_tech_load_ts_stg_df = []

    max_length_df = []
    not_utf8_df = []
    stat_most_cons_val_df = []
    check_columns_length_statistics_result_df = []
    not_null_df = []

    if bool(run_sql(dialect, f'select 1 from {schema}.{table} limit 1', connection)):
        table_df.append(table)
        schema_df.append(schema)
        # Получаем все поля таблицы
        all_columns_list = select_columns(dialect, path, 'all', schema, table, connection)
        text_columns_list = select_columns(dialect, path, 'text', schema, table, connection)
        for col in all_columns_list:
            col_table_df.append(table)
            col_schema_df.append(schema)
            col_df.append(col)
        # main_check(schema, table, all_columns_list, check_list,  connection)
        if 1 in checks:
            print('1. Проверка дублей по ключу')
            check_pk_doubles_df.append(check_pk_doubles(dialect, schema, table, connection))

        if 2 in checks:
            print('2. Полностью пустые')
            cnt = 0
            for col in all_columns_list:
                result = to_flat_list(check_null_fields(dialect, schema, table, col, connection))
                not_null_df.append(result[0])
                if result[0] == 1:
                    cnt = cnt + 1
            count_null_cols_df.append(cnt)

        if 3 in checks:
            print('3. Проверка максимальных длин полей')
            cnt = 0
            for col in all_columns_list:
                if col in text_columns_list:
                    result = to_flat_list(max_length(dialect, schema, table, col, connection))
                    max_length_df.append(result[0])
                    if result[0] == 1:
                        cnt = cnt + 1
                else:
                    max_length_df.append('-')
            count_max_length_df.append(cnt)

        if 4 in checks:
            print('4. Проверка есть ли UTF-8 символы')
            cnt = 0
            for col in all_columns_list:
                result = to_flat_list(not_utf8(dialect, schema, table, col, connection))
                if col in text_columns_list:
                    not_utf8_df.append(result[0])
                else:
                    not_utf8_df.append('-')
                if result[0] == 1:
                    cnt = cnt + 1
            count_not_utf8_cols_df.append(cnt)

        if 5 in checks:
            print('5. Максимальная tech_load_ts ODS')
            check_max_tech_load_ts_df.append(check_max_tech_load_ts(dialect, schema, table, connection)[0])

        if 6 in checks:
            print('6.DRAFT Проверка корректности инкремента')
            try:
                check_insert_new_rows(dialect, schema, table, connection)
            except:
                print('Ошибка:\n', traceback.format_exc())
        print(schema)
        if 7 in checks:
            print('7. Самое часто встречающееся значание')
            for col in all_columns_list:
                stat_most_cons_val_df.append(
                    to_flat_list(check_most_consistent_value(dialect, schema, table, col, connection))[0])

        if 8 in checks:
            print('8. Статистика длины текстовых полей')
            for col in all_columns_list:
                if col in text_columns_list:
                    check_columns_length_statistics_result_df.append(
                        to_flat_list(check_columns_length_statistics(dialect, schema, table, col, connection))[0])
                else:
                    check_columns_length_statistics_result_df.append('-')
        if 9 in checks:
            print('9. Сегментация')
            check_segmentation_df.append(check_segmentation(dialect, schema, table, connection))

        if 10 in checks:
            print('10. Количество STG')
            stg_schema = schema.replace('ODS_', 'STG_')
            check_row_count_stg_df.append(check_row_count(dialect, stg_schema, table, connection)[0])

        if 11 in checks:
            print('11. Количество ODS')
            check_row_count_ods_df.append(check_row_count(dialect, schema, table, connection)[0])

        if 12 in checks:
            print('12. Количество бизнес ключей в ods')
            check_bussines_key_counts_df.append(check_bussines_key_counts(dialect, schema, table, connection))

        if 13 in checks:
            print('13. Дубли по ключу в stg')
            stg_schema = schema.replace('ODS_', 'STG_')

            check_stg_pk_doubles_df.append(check_pk_doubles(dialect, stg_schema, table, connection))

        if 14 in checks:
            print('14. Максимальная tech_load_ts STG')
            stg_schema = schema.replace('ODS_', 'STG_')
            check_max_tech_load_ts_stg_df.append(check_max_tech_load_ts(dialect, stg_schema, table, connection)[0])

        list1_all_options_dict = {
            'ods_pk_doubles': check_pk_doubles_df,
            'stg_pk_doubles': check_stg_pk_doubles_df,
            'ods_row_count': check_row_count_ods_df,
            'stg_row_count': check_row_count_stg_df,
            'bk_counts': check_bussines_key_counts_df,
            'max_ts_ods': check_max_tech_load_ts_df,
            'max_ts_stg': check_max_tech_load_ts_stg_df,
            'ods_null_fields': count_null_cols_df,
            'ods_max_length': count_max_length_df,
            'ods_not_utf8': count_not_utf8_cols_df,
            'segmentation': check_segmentation_df,
        }

        list2_all_options_dict = {
            'null_cols': not_null_df,
            'not_utf8': not_utf8_df,
            'max_length': max_length_df,
            'Consist': stat_most_cons_val_df,
            'length stat': check_columns_length_statistics_result_df
        }

        number_of_check_in_checks_variable_1 = {
            'ods_pk_doubles': 1,
            'stg_pk_doubles': 13,
            'ods_row_count': 11,
            'stg_row_count': 10,
            'bk_counts': 12,
            'max_ts_ods': 5,
            'max_ts_stg': 14,
            'ods_null_fields': 2,
            'ods_max_length': 3,
            'ods_not_utf8': 4,
            'segmentation': 9,
        }

        number_of_check_in_checks_variable_2 = {
            'null_cols': 2,
            'not_utf8': 4,
            'max_length': 3,
            'Consist': 7,
            'length stat': 8
        }

        list1_df_components_dict = {}
        list1_df_components_dict['schema'] = schema_df
        list1_df_components_dict['table'] = table_df
        list1_df_components_dict.update({key: list1_all_options_dict[key] for key in list1_all_options_dict if
                                         number_of_check_in_checks_variable_1.get(key, None) in checks})

        list2_df_components_dict = {}
        list2_df_components_dict['schema'] = col_schema_df
        list2_df_components_dict['table'] = col_table_df
        list2_df_components_dict['column'] = col_df
        list2_df_components_dict.update({key: list2_all_options_dict[key] for key in list2_all_options_dict if
                                         number_of_check_in_checks_variable_2.get(key, None) in checks})

        df_list1 = pandas.DataFrame(list1_df_components_dict)
        df_list2 = pandas.DataFrame(list2_df_components_dict)

        if os.path.isfile(f'{path}/reports/{report_name}_{b}.xlsx'):
            writer_sheet1 = pandas.read_excel(f'{path}/reports/{report_name}_{b}.xlsx', header=0, sheet_name='General')
            writer_sheet2 = pandas.read_excel(f'{path}/reports/{report_name}_{b}.xlsx', header=0, sheet_name='Detail')
            frame1 = [writer_sheet1, df_list1]
            df_result1 = pandas.concat(frame1)

            frame2 = [writer_sheet2, df_list2]
            df_result2 = pandas.concat(frame2)

            writer = pandas.ExcelWriter(f'{path}/reports/{report_name}_{b}.xlsx')
            df_result1.to_excel(writer, sheet_name='General', index=False)
            df_result2.to_excel(writer, sheet_name='Detail', index=False)
            writer.save()

        else:
            writer = pandas.ExcelWriter(f'{path}/reports/{report_name}_{b}.xlsx')

            df_list1.to_excel(writer, sheet_name='General', index=False)
            df_list2.to_excel(writer, sheet_name='Detail', index=False)
            writer.save()

    else:
        logging.warning(f'Таблица {schema}.{table} пустая')
        empty_tables.append(f'{schema}.{table}')
        print(empty_tables)

print(f'Check Results in `QualityChecker/reports/{report_name}_{b}.xlsx')
print(empty_tables)
print(b)
print(time.strftime("%Y-%m-%d_%H-%M"))

"""list1_all_options_dict = {
    'pk_doubles': check_pk_doubles_df,
    'max_ts_ods': check_max_tech_load_ts_df,
    'null_fields': count_null_cols_df,
    'max_length': count_max_length_df,
    'not_utf8': count_not_utf8_cols_df,
    'stg_row_count': check_row_count_stg_df,
    'ods_row_count': check_row_count_ods_df,
    'segmentation': check_segmentation_df}

list2_all_options_dict = {
    'null_cols': not_null_df,
    'not_utf8': not_utf8_df,
    'max_length': max_length_df,
    'Consist': stat_most_cons_val_df,
    'length stat': check_columns_length_statistics_result_df
}

number_of_check_in_checks_variable_1 = {
    'pk_doubles': 1,
    'max_ts_ods': 5,
    'null_fields': 2,
    'max_length': 3,
    'not_utf8': 4,
    'stg_row_count': 10,
    'ods_row_count': 11,
    'segmentation': 9
}

number_of_check_in_checks_variable_2 = {
    'null_cols': 2,
    'not_utf8': 4,
    'max_length': 3,
    'Consist': 7,
    'length stat': 8
}

list1_df_components_dict = {}
list1_df_components_dict['schema'] = schema_df
list1_df_components_dict['table'] = table_df
list1_df_components_dict.update({key: list1_all_options_dict[key] for key in list1_all_options_dict if number_of_check_in_checks_variable_1.get(key, None) in checks})

list2_df_components_dict = {}
list2_df_components_dict['schema'] = col_schema_df
list2_df_components_dict['table'] = col_table_df
list2_df_components_dict['column'] = col_df
list2_df_components_dict.update({key: list2_all_options_dict[key] for key in list2_all_options_dict if number_of_check_in_checks_variable_2.get(key, None) in checks})

df_list1 = pandas.DataFrame(list1_df_components_dict)
df_list2 = pandas.DataFrame(list2_df_components_dict)

writer = pandas.ExcelWriter(f'{path}/reports/{schema}_{b}.xlsx')
print(f'Check Results in `QualityChecker/reports/{schema}_{b}.xlsx`')

df_list1.to_excel(writer, sheet_name='General')
df_list2.to_excel(writer, sheet_name='Detail')
writer._save()"""
