import os
import pandas as pd

def transform_data():
    data_dir = os.path.abspath( os.path.join( os.path.dirname(os.path.abspath(__file__)), '..', 'Data') )
    
    tmp2 = os.path.join( data_dir, "cleaned.csv" )
    tmp3 = os.path.join( data_dir, "transformed.csv" )
    
    df = pd.read_csv( tmp2 )

    df['date'] = pd.to_datetime(df['date'])
    df['prev_date'] = df['date'] - pd.Timedelta(days=1)
    prev_day = df[['date', 'article', 'rank']]
    prev_day = prev_day.rename(columns={'date': 'prev_date', 'rank': 'prev_rank'})
    df = df.merge(prev_day, on=['prev_date', 'article'], how='left')
    df['change_in_rank'] = df['rank'] - df['prev_rank']
    df['new_entry_flag'] = df['prev_rank'].isna()
    df = df.drop(columns=['prev_date'])

    df.to_csv( tmp3, index=False )

transform_data()