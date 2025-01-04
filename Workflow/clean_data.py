import os
import pandas as pd 

from IPython.display import display

def drop_null(df):
    df = df.dropna()
    return df

def drop_duplicates(df):
    df = df.drop_duplicates( subset=["article", "date"] )
    df = df.reset_index( drop=True )
    return df

def format_data(df):
    df['article'] = df['article'].str.replace('_', ' ')
    return df

def clean_data():
    data_dir = os.path.abspath( os.path.join( os.path.dirname(os.path.abspath(__file__)), '..', 'Data') )
    
    tmp = os.path.join( data_dir, "pageviews.csv" )
    tmp2 = os.path.join( data_dir, "cleaned.csv" )

    if os.path.exists( tmp2 ):
        os.remove( tmp2 )
        print(f"File { tmp2 } has been deleted.")
    
    df = pd.read_csv( tmp )

    df = drop_null( df )
    df = drop_duplicates( df )
    df = format_data( df )

    df.to_csv( tmp2, index=False )

clean_data()