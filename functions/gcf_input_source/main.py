import logging
import traceback
import tempfile
import os
import re
import io

from google.cloud import documentai_v1beta3 as documentai
from google.cloud import storage

from PyPDF2 import PdfFileWriter, PdfFileReader
from urllib.parse import urlparse

def try_catch_log(wrapped_func):
  def wrapper(*args, **kwargs):
    try:
      response = wrapped_func(*args, **kwargs)
    except Exception:
      # Replace new lines with spaces so as to prevent several entries which
      # would trigger several errors.
      error_message = traceback.format_exc().replace('\n', '  ')
      logging.error(error_message)
      return 'Error'
    return response
  return wrapper

#@try_catch_log
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

    batch_process_documents(project_id, 'eu', processor,  
        "gs://" + event['bucket'] +"/" + event['name'],  event['name'], output_bucket)

    return 'OK'


def get_env():
    

    print(os.environ)
    if 'GCP_PROJECT' in os.environ:
       return os.environ['GCP_PROJECT']

    import google.auth

    _,project_id = google.auth.default()
    print(project_id)
    return project_id


def split_pdf(inputpdf, start_page, end_page, uri, gcs_output_uri : str, gcs_output_uri_prefix :str):
    storage_client = storage.Client()

    with io.StringIO() as stream:

        print("numPages: {}".format( inputpdf.numPages))

        output = PdfFileWriter()
        for i in range (start_page , end_page+1):
            output.addPage(inputpdf.getPage(i))
            print("add page {}".format(i))

        file = uri.path[:-4] +"-page-{}-to-{}.pdf".format( start_page, end_page )
        print(file)

        buf = io.BytesIO()
        output.write(buf)
        data =buf.getvalue()
        outputBlob =  gcs_output_uri_prefix  + file
        print("Start write:"+outputBlob)
        bucket = storage_client.get_bucket(urlparse(gcs_output_uri).hostname)

        bucket.blob(outputBlob).upload_from_string(data, content_type='application/pdf')

        stream.truncate(0)
        
    print("split finish")

def pages_split(text: str, document: dict, uri, gcs_output_uri : str, gcs_output_uri_prefix :str ):
    """
    Document AI identifies possible page splits
    in document. This function converts page splits
    to text snippets and prints it.    
    """
    for i, entity in enumerate(document.entities):
        confidence = entity.confidence
        text_entity = ''
        for segment in entity.text_anchor.text_segments:
            start = segment.start_index
            end = segment.end_index
            text_entity += text[start:end]
            
        pages = [p.page for p in entity.page_anchor.page_refs]
        print(f"*** Entity number: {i}, Split Confidence: {confidence} ***")
        print(f"*** Pages numbers: {[p for p in pages]} ***\nText snippet: {text_entity[:100]}")
        print("type: " + entity.type_)
        start_page= pages[0]
        end_page = pages[len(pages)-1]
        print(start_page)
        print(end_page)
        
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(uri.hostname)
        blob = bucket.get_blob(uri.path[1:])

        inputpdf=  PdfFileReader(
            io.BytesIO(blob.download_as_bytes())
            ,strict=False) 
        
        split_pdf(inputpdf, start_page, end_page, uri,gcs_output_uri, gcs_output_uri_prefix + "/" + entity.type_)
         

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
    print("\nThe processor detected the following page split entities:")
    pages_split(text, document, uri, gcs_output_uri, gcs_output_uri_prefix)


#Asynchronous processing
def batch_process_documents(
    project_id,
    location,
    processor_id,
    gcs_input_uri,
    gcs_output_uri_prefix,
    OUTPUT_JSON_URI,
    timeout: int = 300,
):

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
    
    
    print("end")
