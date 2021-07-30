import logging
import traceback
import tempfile
import os
import re
import io

from google.cloud import documentai_v1 as documentai
from google.cloud import storage

from urllib.parse import urlparse

import pandas_gbq
import pandas as pd

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
    
    bqTableName = os.environ.get('BQ_TABLENAME', 'ERROR: Specified environment variable is not set.')
    if bqTableName in "ERROR":
        logging.fatal('inputPdfPath variable is not set exit program')
        return

    logging.debug('PROCESSOR: {}'.format(processor))
    print('processor: {}'.format(processor)) 



#    process(processor,"eu", processor,  "gs://" + event['bucket'] +"/" + event['name'],  event['name'] , "out", output_bucket)
#    batch_process_documents(project_id, 'eu', processor,         "gs://" + event['bucket'] +"/" + event['name'],  event['name'], output_bucket)
    gcs_input_uri = "gs://" + event['bucket'] +"/" + event['name']
    if gcs_input_uri.lower().endswith(".pdf"):
        project_id = get_env()
        print(project_id)

        doc_type = getDocumentType(gcs_input_uri)
        if doc_type == "invoices":
            document = process(    project_id, 'eu', processor, gcs_input_uri)

            df = getDF(document    ,  gcs_input_uri, doc_type)

            print("Start Insert BQ : " + bqTableName)
            print("json")
            #df.to_gbq(bqTableName,project_id,if_exists='append')

            pandas_gbq.to_gbq(df, bqTableName, if_exists='append')
            print("Insert BQ done in : " + bqTableName)
        else:
            print("Unsuported document type: " + doc_type + " uri:"+ gcs_input_uri)
    else:
        print("Unsuported extention: " + gcs_input_uri)            

    return 'OK'

# TODO: Implement document type classification
def getDocumentType(gcs_input_uri):
    return "invoices"

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
    project_id: str, location: str, processor_id: str, gcs_input_uri: str,   
    timeout: int = 300,
):

    # Instantiates a client
    # You must set the api_endpoint if you use a location other than 'us', e.g.:
    opts = {}
    if location == "eu":
        opts = {"api_endpoint": "eu-documentai.googleapis.com"}
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
    request = {"name": name, "raw_document": document}
    
    # Recognizes text entities in the PDF document
    result = client.process_document(request=request, timeout=timeout)
    
    document = result.document

    print("Document processing complete.")
    return document


def getDF(document, name, doc_type):
    lst = [[]]
    lst.pop()
    
    for entity in document.entities:        
        if entity.normalized_value.text != "":
            val = entity.normalized_value.text
            #print(f'{entity.type_} {entity.mention_text} - normalized_value: {val} - {entity.confidence} ' )        
        else:
            val = entity.mention_text
            #print(f'{entity.type_} {val} - {entity.confidence} ' )        

        lst.append(["entity", val, entity.type_, entity.confidence, name , doc_type]) 
        


    # Read the text recognition output from the processor
    for page in document.pages:
        for form_field in page.form_fields:
            field_name = get_text(form_field.field_name, document)
            field_value = get_text(form_field.field_value, document)
            field_type = field_name

            #print(f"Extracted key value pair: \t{field_name}, {field_value}")
            
            lst.append(["key_value",field_value, field_type, form_field.field_name.confidence, name , doc_type])

    df = pd.DataFrame(lst
                          ,columns =['key', 'value', 'type', 'confidence', 'file', 'doc_type']
                         )

    
    return  df



# Extract shards from the text field
def get_text(doc_element: dict, document: dict):
    """
    Document AI identifies form fields by their offsets
    in document text. This function converts offsets
    to text snippets.
    """
    response = ""
    # If a text segment spans several lines, it will
    # be stored in different text segments.
    for segment in doc_element.text_anchor.text_segments:
        start_index = (
            int(segment.start_index)
            if segment in doc_element.text_anchor.text_segments
            else 0
        )
        end_index = int(segment.end_index)
        response += document.text[start_index:end_index]
    return response


#Synchronous processing
def batch_process_documents(
    project_id,
    location,
    processor_id,
    gcs_input_uri,
    gcs_output_uri,
    gcs_output_uri_prefix,
    timeout: int = 540,
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

    print('operation name: {}'.format(operation.name, operation.response))
    print('operation response: {}'.format(operation.response))    
    # Wait for the operation to finish
    res = operation.result(timeout=timeout)
    print('operation end: {} / {}'.format(res, operation.error))
    
    print("end")

