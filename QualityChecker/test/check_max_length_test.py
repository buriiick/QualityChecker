import pytest


from checks import max_length
from utils.databaseTools import run_sql
from conf import vertica_conn_dict 


conn_dict = vertica_conn_dict['DEV']

@pytest.fixture(scope="module")
def setup_database():
    create_table_script = """
    CREATE TABLE public.test_table (
        id SERIAL PRIMARY KEY,
        short_column VARCHAR(10),  -- Столбец, не превышающий длину
        long_column VARCHAR(10)    -- Столбец, превышающий длину
    );
    """
    
    insert_data_script = """
    INSERT INTO public.test_table (short_column, long_column) 
    VALUES 
    ('short', 'toolonggggg'), 
    ('ok', 'exceedslen');
    """
    
    drop_table_script = "DROP TABLE public.test_table;"
    
    run_sql('Vertica', create_table_script, conn_dict)
    
    run_sql('Vertica', insert_data_script, conn_dict)
    
    yield
    
    run_sql('Vertica', drop_table_script, conn_dict)


def test_max_length_column_reached(setup_database):
    result = max_length('Vertica', 'public', 'test_table', 'long_column', conn_dict)
    assert result == [[1]], "Expected the long_column to have reached its max length"

def test_max_length_column_not_reached(setup_database):
    result = max_length('Vertica', 'public', 'test_table', 'short_column', conn_dict)
    assert result == [[0]], "Expected the short_column not to have reached its max length"