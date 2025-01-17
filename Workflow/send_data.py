import os
import pandas as pd
import snowflake.connector
from decouple import config

sql_datatypes = {
    'object': 'VARCHAR',
    'int64': 'INT',
    'float64': 'FLOAT',
    'bool': 'BOOLEAN',
    'datetime64[ns]': 'DATETIME'
}

def connect_to_snowflake():

    data_dir = os.path.abspath( os.path.join( os.path.dirname(os.path.abspath(__file__)), '..', 'Data') )
    
    tmp3 = os.path.join( data_dir, "transformed.csv" )
    
    df = pd.read_csv( tmp3, parse_dates=[3] )

    def get_column_metadata(df):
        
        updated_datatypes = [ sql_datatypes[str(x)] for x in df.dtypes ]
        column_names = [ x.upper() for x in df.columns.tolist() ]
        column_metadata = list( zip( column_names, updated_datatypes ) )

        return column_metadata
    
    con = snowflake.connector.connect(
        user=config('USER'),
        account=config('ACCOUNT'),
        password=config('PASSWORD'),
        warehouse='COMPUTE_WH',
        database='WIKIPEDIA_TRAFFIC_DATA',
        schema='WIKI_SCHEMA'
    )

    # column_metadata = get_column_metadata(df)
    # print( column_metadata )

    con.close()

connect_to_snowflake()