import os
from decouple import config
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

sql_datatypes = {
    'object': 'NVARCHAR',
    'int64': 'INT',
    'float64': 'FLOAT',
    'bool': 'BOOLEAN',
    'datetime64[ns]': 'DATE'
}

def connect_to_snowflake():

    data_dir = os.path.abspath( os.path.join( os.path.dirname(os.path.abspath(__file__)), '..', 'Data') )
    
    tmp3 = os.path.join( data_dir, "transformed.csv" )
    
    df = pd.read_csv( tmp3, parse_dates=[3] )
    df['DATE'] = df['DATE'].dt.date
    
    table_name = "wiki_daily_traffic"

    def get_column_metadata(df):
        
        updated_datatypes = [ sql_datatypes[str(x)] for x in df.dtypes ]
        column_names = [ x.upper() for x in df.columns.tolist() ]
        column_metadata = list( zip( column_names, updated_datatypes ) )
        column_metadata = ", ".join([f"{name} {datatype}" for name, datatype in column_metadata])
        return column_metadata
    
    table_metadata = get_column_metadata(df)

    con = snowflake.connector.connect(
        user=config('USER'),
        account=config('ACCOUNT'),
        password=config('PASSWORD'),
        warehouse='COMPUTE_WH',
        database='WIKIPEDIA_TRAFFIC_DATA',
        schema='WIKI_SCHEMA'
    )
    
    con.cursor().execute(f"CREATE OR REPLACE TABLE {table_name} ({table_metadata})")
    write_pandas(con, df, table_name.upper(), use_logical_type=True)
    con.close()


    print( table_metadata )
    print(df['DATE'].head())
    print(df['DATE'].dtype)

    

connect_to_snowflake()