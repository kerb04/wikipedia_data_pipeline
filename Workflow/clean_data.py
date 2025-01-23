import os
import pandas as pd 

# Drop csv indexes for rows that have them
def drop_indexes(df):

    def shift_last_column(row):
        # If the last column is empty (row has no index), shift the row to the right
        if pd.isna(row.iloc[-1]):
            row = row.shift(1)

        return row

    df = df.apply(shift_last_column, axis=1)
    return df.drop(df.columns[0], axis=1)

def drop_null(df):
    df = df.dropna()
    return df

def drop_duplicates(df):
    df = df.drop_duplicates( subset=["article", "date"] )
    df = df.reset_index( drop=True )
    return df

def format_data(df):
    # Replace underscores with spaces for readability
    df['article'] = df['article'].str.replace('_', ' ')
    # Capitalize column names
    df.columns = df.columns.str.upper()
    # Ensure dates have consistent format
    df['DATE'] = df['DATE'].str.replace('/', '-')

    df = df.sort_values(by=['DATE', 'RANK'], ascending=[True, True])

    return df

def clean_data():
    data_dir = os.path.abspath( os.path.join( os.path.dirname(os.path.abspath(__file__)), '..', 'Data') )
    
    raw_data = os.path.join( data_dir, "pageviews.csv" )
    clean_data = os.path.join( data_dir, "cleaned.csv" )
    
    df = pd.read_csv( raw_data, low_memory=False )
    df = drop_indexes( df )
    df = drop_null( df )
    df = drop_duplicates( df )
    df = format_data( df )

    df.to_csv( clean_data, index=False )