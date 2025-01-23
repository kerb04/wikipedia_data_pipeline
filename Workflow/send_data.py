import os
from decouple import config
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Map from python datatypes to SQL datatypes
sql_datatypes = {
    'object': 'NVARCHAR',
    'int64': 'INT',
    'float64': 'FLOAT',
    'bool': 'BOOLEAN',
    'datetime64[ns]': 'DATE'
}

def send_to_snowflake():
    data_dir = os.path.abspath( os.path.join( os.path.dirname(os.path.abspath(__file__)), '..', 'Data') )
    transformed_data = os.path.join( data_dir, "transformed.csv" )
    
    df = pd.read_csv( transformed_data, parse_dates=[3] )
    df['DATE'] = df['DATE'].dt.date
    
    table_name = "wiki_daily_traffic"

    def get_column_metadata(df):
        
        updated_datatypes = [ sql_datatypes[str(x)] for x in df.dtypes ]
        column_names = [ x.upper() for x in df.columns.tolist() ]
        column_metadata = list( zip( column_names, updated_datatypes ) )
        column_metadata = ", ".join([f"{name} {datatype}" for name, datatype in column_metadata])
        return column_metadata
    
    con = snowflake.connector.connect(
        user=config('USER'),
        account=config('ACCOUNT'),
        password=config('PASSWORD'),
        warehouse='COMPUTE_WH',
        database='WIKIPEDIA_TRAFFIC_DATA',
        schema='WIKI_SCHEMA'
    )

    column_metadata = get_column_metadata(df)
    
    con.cursor().execute(f"CREATE OR REPLACE TABLE {table_name} ({column_metadata})")
    write_pandas(con, df, table_name.upper(), use_logical_type=True)
    con.close()    