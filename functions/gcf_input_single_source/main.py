import logging
import traceback
import tempfile
import os
import re
import io

from google.cloud import documentai_v1 as documentai
from google.cloud import storage

from urllib.parse import urlparse


def main_run(event, context):
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))

    logging.debug('Event ID: {}'.format(context.event_id))
    logging.debug('Event type: {}'.format(context.event_type))
    logging.debug('Bucket: {}'.format(event['bucket']))
    logging.debug('File: {}'.format(event['name']))
    logging.debug('Metageneration: {}'.format(event['metageneration']))
    logging.debug('Created: {}'.format(event['timeCreated']))
    logging.debug('Updated: {}'.format(event['updated']))

    output_bucket = os.environ.get('OUTPUT_URI', 'ERROR: Specified environment variable is not set.')
    if output_bucket in "ERROR":
        logging.fatal('OUTPUT_URI variable is not set exit program')
        return
    logging.debug('OUTPUT_URI: {}'.format(output_bucket))
    print('output_bucket: {}'.format(output_bucket))

    processor = os.environ.get('PROCESSOR', 'ERROR: Specified environment variable is not set.')
    if processor in "ERROR":
        logging.fatal('processor variable is not set exit program')
        return

    logging.debug('PROCESSOR: {}'.format(processor))
    print('processor: {}'.format(processor)) 

    project_id = get_env()
    print(project_id)

#    process(processor,"eu", processor,  "gs://" + event['bucket'] +"/" + event['name'],  event['name'] , "out", output_bucket)
    batch_process_documents(project_id, 'eu', processor,         "gs://" + event['bucket'] +"/" + event['name'],  event['name'], output_bucket)

    return 'OK'


def get_env():
    

    print(os.environ)
    if 'GCP_PROJECT' in os.environ:
       return os.environ['GCP_PROJECT']

    import google.auth

    _,project_id = google.auth.default()
    print(project_id)
    return project_id



#Synchronous processing
def process(
    project_id: str, location: str, processor_id: str, gcs_input_uri: str, gcs_output_uri : str, gcs_output_uri_prefix, OUTPUT_JSON_URI,  timeout: int = 300,
):

    # Instantiates a client
    opts = {"api_endpoint": f"{location}-documentai.googleapis.com"}
    client = documentai.DocumentProcessorServiceClient(client_options=opts)

    # The full resource name of the processor, e.g.:
    # projects/project-id/locations/location/processor/processor-id
    # You must create new processors in the Cloud Console first
    name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"

    uri = urlparse(gcs_input_uri)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(uri.hostname)

    blob = bucket.get_blob(uri.path[1:])
    image_content = blob.download_as_bytes()
    
    # Read the file into memory
    document = {"content": image_content, "mime_type": "application/pdf"}
    # Configure the process request
    request = {"name": name, "document": document}
    
    # Recognizes text entities in the PDF document
    result = client.process_document(request=request, timeout=timeout)
    
    document = result.document

    print("Document processing complete.")

    # Read the text recognition output from the processor
    text = document.text
    print("The document contains the following text (first 100 charactes):")
    print(text[:100])
    print(document)
    # Read the detected page split from the processor




#Asynchronous processing
def batch_process_documents(
    project_id,
    location,
    processor_id,
    gcs_input_uri,
    gcs_output_uri,
    gcs_output_uri_prefix,
    timeout: int = 300,
):

    # You must set the api_endpoint if you use a location other than 'us', e.g.:
    opts = {}
    if location == "eu":
        opts = {"api_endpoint": "eu-documentai.googleapis.com"}

    client = documentai.DocumentProcessorServiceClient(client_options=opts)

    destination_uri = f"gs://{gcs_output_uri}/{gcs_output_uri_prefix}/"

    gcs_documents = documentai.GcsDocuments(
        documents=[{"gcs_uri": gcs_input_uri, "mime_type": "application/pdf"}]
    )

    # 'mime_type' can be 'application/pdf', 'image/tiff',
    # and 'image/gif', or 'application/json'
    input_config = documentai.BatchDocumentsInputConfig(gcs_documents=gcs_documents)

    # Where to write results
    output_config = documentai.DocumentOutputConfig(
        gcs_output_config={"gcs_uri": destination_uri}
    )

    # Location can be 'us' or 'eu'
    name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"
    request = documentai.types.document_processor_service.BatchProcessRequest(
        name=name,
        input_documents=input_config,
        document_output_config=output_config,
    )

    operation = client.batch_process_documents(request)

    print('operation: {}'.format(operation))
    
    print("end")


#Asynchronous processing
def batch_process_documents_beta13(
    project_id,
    location,
    processor_id,
    gcs_input_uri,
    gcs_output_uri_prefix,
    OUTPUT_JSON_URI,
    timeout: int = 540,
):

    from google.cloud import documentai_v1beta3 as documentai

    opts = {"api_endpoint": f"{location}-documentai.googleapis.com"}
    print('api_endpoint: {}'.format(opts))

    client = documentai.DocumentProcessorServiceClient(client_options=opts)
    print('client: {}'.format(client))

    destination_uri = f"gs://{OUTPUT_JSON_URI}/{gcs_output_uri_prefix}/"
    print('destination_uri: {}'.format(destination_uri))

    # 'mime_type' can be 'application/pdf', 'image/tiff',
    # and 'image/gif', or 'application/json'
    input_config = documentai.types.document_processor_service.BatchProcessRequest.BatchInputConfig(
        gcs_source=gcs_input_uri, mime_type="application/pdf"
    )
    print('input_config: {}'.format(input_config))

    # Where to write results
    output_config = documentai.types.document_processor_service.BatchProcessRequest.BatchOutputConfig(
        gcs_destination=destination_uri
    )
    print('output_config: {}'.format(output_config))

    # Location can be 'us' or 'eu'
    name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"
    print('processor name: {}'.format(name))

    request = documentai.types.document_processor_service.BatchProcessRequest(
        name=name,
        input_configs=[input_config],
        output_config=output_config,
    )
    print('request: {}'.format(request))

    operation = client.batch_process_documents(request)
    print('operation: {}'.format(operation))
    # Wait for the operation to finish
    operation.result(timeout=timeout)
    print('operation end: {}'.format(operation))
    
    print("end")
