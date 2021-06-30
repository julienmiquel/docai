import logging
import traceback
import tempfile
import os
import re
import io

from google.cloud import documentai_v1 as documentai
from google.cloud import storage

from PyPDF2 import PdfFileWriter, PdfFileReader
from urllib.parse import urlparse
from urllib.parse import urlunparse

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

    inputPdfPath = os.environ.get('INPUT_PDF_PATH', 'ERROR: Specified environment variable is not set.')
    if inputPdfPath in "ERROR":
        logging.fatal('inputPdfPath variable is not set exit program')
        return

    bqTableName = os.environ.get('BQ_TABLENAME', 'ERROR: Specified environment variable is not set.')
    if bqTableName in "ERROR":
        logging.fatal('inputPdfPath variable is not set exit program')
        return
        
    pdffilename = event['name']
    print(f"PDF file name: {pdffilename}")
    #url = urlparse("gs://"+inputPdfPath+"/"+pdffilename[0])
    #url_parts = list(urlparse("gs://"+inputPdfPath))
    #url_parts[2] = pdffilename
    #pdf = urlunparse(url_parts)

    pdf = "gs://"+inputPdfPath + "/"+pdffilename[0]
    print("pdffilename: "+ pdf)
    process(  
        "gs://" + event['bucket'] +"/" + event['name'] ,pdf , bqTableName)

    return 'OK'


def get_env():
    print(os.environ)
    if 'GCP_PROJECT' in os.environ:
       return os.environ['GCP_PROJECT']

    import google.auth

    _,project_id = google.auth.default()
    print(project_id)
    return project_id

def getDF(document, name):
    lst = [[]]
    lst.pop()
    
    for entity in document.entities:        
        if entity.normalized_value.text != "":
            val = entity.normalized_value.text
            print(f'{entity.type_} {entity.mention_text} - normalized_value: {val} - {entity.confidence} ' )        
        else:
            val = entity.mention_text
            print(f'{entity.type_} {val} - {entity.confidence} ' )        

        lst.append([entity.type_, val, entity.type_, entity.confidence, name ]) 
        


    # Read the text recognition output from the processor
    for page in document.pages:
        for form_field in page.form_fields:
            field_name = get_text(form_field.field_name, document)
            field_value = get_text(form_field.field_value, document)
            field_type = field_name

            print(f"Extracted key value pair: \t{field_name}, {field_value}")
            
            lst.append([field_name,field_value, field_type, form_field.field_name.confidence, name ])

    df = pd.DataFrame(lst
                          ,columns =['key', 'value', 'type', 'confidence', 'file']
                         )

    
    return  df

def process(
    gcs_input_uri: str,  pdffilename : str, bqTableName : str ):
    if ".json" in gcs_input_uri:
        print("gcs_input_uri:"+gcs_input_uri)
        uri = urlparse(gcs_input_uri)

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(uri.hostname)
        print("bucket:"+bucket.name)
        blob = bucket.get_blob(uri.path[1:])

        uriPdf = urlparse(pdffilename)
        print("getbucket: " + uriPdf.hostname)
        #bucketPdf = storage_client.get_bucket(uriPdf.hostname)
        
        print("uripdf: "+uriPdf.path[1:])
        #blobPdf = bucketPdf.get_blob(uriPdf.path[1:])
        #print(blobPdf)
        #if blobPdf.exists(): 
        #    print("Input PDF exist" )
        #else:
        #    print("ERROR Input PDF DOES NOT exist: " + uriPdf)                
        #    return

        #blob_as_bytes = blob.download_as_bytes()

           # Results are written to GCS. Use a regex to find
        # output files
        match = re.match(r"gs://([^/]+)/(.+)", gcs_input_uri)
        output_bucket = match.group(1)
        prefix = match.group(2)
        print(output_bucket)
        print(prefix)

        project_id = get_env()
        print(project_id)
        
        bucket = storage_client.get_bucket(output_bucket)
        blob_list = list(bucket.list_blobs(prefix=prefix))        

        for i, blob in enumerate(blob_list):
            # If JSON file, download the contents of this blob as a bytes object.
            if ".json" in blob.name:
                blob_as_bytes = blob.download_as_bytes()

                document = documentai.types.Document.from_json(blob_as_bytes)
                print(f"Fetched file {i + 1}")
                df = getDF(document, pdffilename)

                table_id = bqTableName 
                pandas_gbq.to_gbq(df, table_id, if_exists='append')
            else:
                print(f"Skipping non-supported file type {blob.name}")
                            
                

        
    else:
        print("Input file not supported:" + gcs_input_uri)

    

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