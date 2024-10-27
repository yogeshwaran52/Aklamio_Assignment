import os
import pandas as pd
import json
from sqlalchemy import create_engine
import argparse
import logging

# Logger Config
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

def create_db_database():
    # Creating Postgres database
    logging.info("Creating database connection.")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT")

    database = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
    logging.info("Database connection established.")

    return database

def load_json_data(file_path):
    # Load JSON data
    logging.info(f"Loading JSON data from {file_path}.")
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            data.append(json.loads(line))
    logging.info(f"Loaded {len(data)} records from JSON.")

    return pd.DataFrame(data)

def clean_data(df):
    # Cleaning the data base on the field event_type
    # Checking if the field event_type is nul, empty or with Value "EMPTY_VALUE"
    logging.info("Cleaning the data.")
    df['invalid_data_reason'] = None
    invalid_event_type = df['event_type'].isnull() | (df['event_type'] == "") | (df['event_type'] == "EMPTY_VALUE")
    df.loc[invalid_event_type, 'invalid_data_reason'] = 'Invalid event_type (null, empty, or "EMPTY_VALUE")'

    cleaned_data = df[~invalid_event_type].copy()
    failed_data = df[invalid_event_type].copy()

    # Converting 'fired_at' to datetime in cleaned_data
    cleaned_data['fired_at'] = pd.to_datetime(cleaned_data['fired_at'], format='%m/%d/%Y, %H:%M:%S', errors='coerce')
    date_conversion_errors = cleaned_data['fired_at'].isnull()

    failed_data = pd.concat([failed_data,
                             cleaned_data[date_conversion_errors].assign(invalid_data_reason='Invalid date format')])

    cleaned_data = cleaned_data[~date_conversion_errors]

    logging.info(f"Cleaned data: {len(cleaned_data)} valid rows; {len(failed_data)} failed rows.")
    return cleaned_data, failed_data

def calculated_fields(df):
    # Adding a new field to floor the hour for calculation
    # logging.info("Adding Hour field for metrics.")
    df['hour'] = df['fired_at'].dt.floor('h')
    return df

def calculate_metrics(df):
    # Calculating all Metrics
    logging.info("Calculations of Metrics started")
    page_loads_df = df[df['event_type'] == 'ReferralPageLoad']
    clicks_df = df[df['event_type'] == 'ReferralRecommendClick']

    page_loads = page_loads_df.groupby(['customer_id', 'hour']).size().reset_index(name='page_loads')
    clicks = clicks_df.groupby(['customer_id', 'hour']).size().reset_index(name='clicks')
    unique_user_clicks = clicks_df.groupby(['customer_id', 'hour'])['user_id'].nunique().reset_index(name='unique_user_clicks')

    # Joining all DF to one DF
    result = pd.merge(page_loads, clicks, on=['customer_id', 'hour'], how='outer').fillna(0)
    result = pd.merge(result, unique_user_clicks, on=['customer_id', 'hour'], how='outer').fillna(0)

    # Calculating click_through_rate
    result['click_through_rate'] = result['clicks'] / result['page_loads']
    result.fillna(0, inplace=True)

    logging.info("Metrics calculated successfully.")
    return result

def insert_into_db(df, table_name, engine):
    # Inserting into table in Postgres
    logging.info(f"Inserting data into '{table_name}' table.")
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logging.info(f"Data inserted into '{table_name}' table.")

def process_full_data(file_path):
    # This function is used to process the data as a full file
    database = create_db_database()
    df = load_json_data(file_path)
    cleaned_data, failed_data = clean_data(df)
    cleaned_data = cleaned_data.drop_duplicates()
    logging.info(f"Cleaned data: {len(cleaned_data)} valid rows; {len(failed_data)} failed rows.")
    insert_into_db(cleaned_data, 'cleaned_data', database)
    insert_into_db(failed_data, 'failed_data', database)
    logging.info("Adding Hour field for metrics.")
    processed_data = calculated_fields(cleaned_data)
    result = calculate_metrics(processed_data)
    insert_into_db(result, 'hourly_aggregates', database)

def process_streaming_data(file_path):
    # This function is used to process the data as a line by line
    database = create_db_database()
    metrics = {}
    failed_data = []
    total_lines = 0

    with open(file_path, 'r') as file:
        for line in file:
            total_lines += 1
            try:
                data = json.loads(line)
                df = pd.DataFrame([data])
                cleaned_data, failed_row = clean_data(df)

                if not cleaned_data.empty:
                    cleaned_data = calculated_fields(cleaned_data)
                    for _, row in cleaned_data.iterrows():
                        hour = row['hour']
                        customer_id = row['customer_id']
                        event_type = row['event_type']
                        user_id = row['user_id']

                        if (customer_id, hour) not in metrics:
                            metrics[(customer_id, hour)] = {
                                'page_loads': 0,
                                'clicks': 0,
                                'unique_user_clicks': set()
                            }

                        if event_type == 'ReferralPageLoad':
                            metrics[(customer_id, hour)]['page_loads'] += 1
                        elif event_type == 'ReferralRecommendClick':
                            metrics[(customer_id, hour)]['clicks'] += 1
                            metrics[(customer_id, hour)]['unique_user_clicks'].add(user_id)

                if not failed_row.empty:
                    failed_data.extend(failed_row.to_dict('records'))

                # Log progress every 100 lines processed
                if total_lines % 100 == 0:
                    logging.info(f"Processed {total_lines} lines.")

            except Exception as e:
                failed_data.append({'line': line, 'reason': str(e)})
                logging.error(f"Failed to process line: {line.strip()} - Reason: {str(e)}")

    # Prepare the final metrics DataFrame
    result = []
    for (customer_id, hour), metric in metrics.items():
        result.append({
            'customer_id': customer_id,
            'hour': hour,
            'page_loads': metric['page_loads'],
            'clicks': metric['clicks'],
            'unique_user_clicks': len(metric['unique_user_clicks']),
            'click_through_rate': metric['clicks'] / metric['page_loads'] if metric['page_loads'] > 0 else 0
        })

    # Insert into PostgreSQL
    insert_into_db(pd.DataFrame(result), 'hourly_aggregates', database)
    insert_into_db(pd.DataFrame(failed_data), 'failed_data', database)

    logging.info("Streaming data processing completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process JSON data either by batch or stream.")
    parser.add_argument("mode", choices=["batch", "stream"], help="Mode of processing: 'batch' or 'stream'")
    args = parser.parse_args()

    json_file_path = 'aklamio_challenge.json'

    if args.mode == "batch":
        logging.info("Batch processing started.")
        process_full_data(json_file_path)
        logging.info("Batch processing completed.")
    elif args.mode == "stream":
        logging.info("Streaming processing started.")
        process_streaming_data(json_file_path)
        logging.info("Streaming processing completed.")
