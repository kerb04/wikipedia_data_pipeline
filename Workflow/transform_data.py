import os
import pandas as pd

def add_previous_rank(df):
    df['DATE'] = pd.to_datetime(df['DATE'])
    df['PREV_DATE'] = df['DATE'] - pd.Timedelta(days=1)

    # Create temporary datframe to look up past article rankings
    prev_day = df[['DATE', 'ARTICLE', 'RANK']]
    prev_day = prev_day.rename(columns={'DATE': 'PREV_DATE', 'RANK': 'PREV_RANK'})

    df = df.merge(prev_day, on=['PREV_DATE', 'ARTICLE'], how='left')
    df = df.drop(columns=['PREV_DATE'])
    
    return df

def transform_data():
    data_dir = os.path.abspath( os.path.join( os.path.dirname(os.path.abspath(__file__)), '..', 'Data') )
    
    clean_data = os.path.join( data_dir, "cleaned.csv" )
    transformed_data = os.path.join( data_dir, "transformed.csv" )
    
    df = pd.read_csv( clean_data )

    df = add_previous_rank( df )
    df['CHANGE_IN_RANK'] = df['PREV_RANK'] - df['RANK']
    df['NEW_ENTRY_FLAG'] = df['PREV_RANK'].isna()

    df.to_csv( transformed_data, index=False )