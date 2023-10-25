import vertica_python
from utils.utils  import to_flat_list, read_file_content


def vertica_sql(sql_script: str, vertica_conn_dict: dict):
    """

    :param sql_script:
    :param vertica_conn_dict:
    :raise ValueError:
    :return:
    """
    with vertica_python.connect(**vertica_conn_dict) as connection:
        cur = connection.cursor()
        cur.execute(sql_script)
        result = cur.fetchall()
        return result


def select_columns(cur_path, col_type, dialect, schema, table, connection):
    dialect_path_dict = {'vertica': f'{cur_path}/sql/work_with_meta/vertica/select_all_columns.sql'}
    col_type_dict = {'all': 'true', 'text': 'data_type ilike \'%char%\''}
    columns = vertica_sql(read_file_content(dialect_path_dict[dialect]).format(table=table, schema_name=schema,
                                                                               where_clause=col_type_dict[col_type]),
                          connection)
    columns_list = to_flat_list(columns)
    return columns_list
