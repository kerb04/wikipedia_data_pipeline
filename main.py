from Workflow.download_data import download_data
from Workflow.clean_data import clean_data
from Workflow.transform_data import transform_data
from Workflow.send_data import send_to_snowflake

def main():
    try:
        download_data()
    except Exception as e:
        print(f"Error downloading data: {e}")
        return

    try:
        clean_data()
    except Exception as e:
        print(f"Error cleaning data: {e}")
        return

    try:
        transform_data()
    except Exception as e:
        print(f"Error transforming data: {e}")
        return

    try:
        send_to_snowflake()
    except Exception as e:
        print(f"Error sending data to Snowflake: {e}")

if __name__ == "__main__":
    main()