import functions_framework

##create a function that will be triggered by a GCS event
@functions_framework.cloud_event
def main(cloud_event) -> str:
    data = cloud_event.data
    print(f"data: {data}")
    return "Done"