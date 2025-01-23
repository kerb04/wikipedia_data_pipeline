from kaggle.api.kaggle_api_extended import KaggleApi
import os

def download_data():
    # Parent directory
    parent = os.path.join( os.path.dirname(os.path.abspath(__file__)), '..', 'Data')
    data_dir = os.path.abspath( parent )

    tmp = os.path.join( data_dir, "pageviews.csv" )
    if os.path.exists( tmp ):
        os.remove( tmp )
        print(f"File { tmp } has been deleted.")

    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files('vladtasca/wikipedia-pageviews', path=data_dir, unzip=True)