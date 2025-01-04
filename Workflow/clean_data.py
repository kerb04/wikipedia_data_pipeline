import os
import pandas as pd 

def create_table():
    
    # Get data directory
    data_dir = os.path.abspath( os.path.join( os.path.dirname(os.path.abspath(__file__)), '..', 'Data') )
    
    wiki_df = pd.read_csv( data_dir + "/pageviews.csv" )

