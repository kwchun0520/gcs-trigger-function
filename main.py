import functions_framework
from loguru import logger
from typing import Dict
import google.auth
from datetime import datetime,timezone
from src.gcs_trigger_function.gbq import write_to_table, move_delta_to_table, get_table
from src.gcs_trigger_function.gcs import download_file
from cloudevents.http import CloudEvent


##create a function that will be triggered by a GCS event
@functions_framework.cloud_event
def main(cloud_event:CloudEvent) -> Dict[str,str]:

    data:dict = cloud_event.data
    logger.info(f"Received data: {data}")

    file_path:str = data["name"]
    bucket_name:str = data["bucket"]

    if not file_path.endswith(".csv"):
        logger.warning(f"File {file_path} is not a CSV file")
        return {"response":"No action taken"}

    logger.info(f"Processing file: {file_path}")
    dataset_name,table_name,_ = file_path.split("/")
    _, project = google.auth.default()

    if not get_table(project=project, dataset_name=dataset_name, table_name=table_name):
        logger.warning(f"Table {project}.{dataset_name}.{table_name} does not exist")
        return {"response":"No action taken"}
    
    datetime_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Current datetime: {datetime_str}")
    data = download_file(bucket_name=bucket_name, file_path=file_path)
    updated_data = list(map(lambda d: {**d, "last_update":datetime_str}, data))

    delta = f"{project}.{dataset_name}.{table_name}_delta"
    table = f"{project}.{dataset_name}.{table_name}"

    try:
        write_to_table(data=updated_data, table=delta)
    except Exception as e:
        logger.error(e)
        return {"response":"An error occurred"}
    
    move_delta_to_table(delta=delta, datetime_str=datetime_str, table=table)


    return {"response":"File processed successfully"}


if __name__ == '__main__':

    attributes = {
        "type": "com.example.sampletype1",
        "source": "https://example.com/event-producer",
    }
    data = {"name": "my_dataset/my_table/test.csv", "bucket": "my-new-project-bucket-1234"}
    event = CloudEvent(attributes, data)
    main(event)