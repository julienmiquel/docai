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
from urllib.parse import urlunparse


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

    processor = os.environ.get('PROCESSOR', 'ERROR: Specified environment variable is not set.')
    if processor in "ERROR":
        logging.fatal('processor variable is not set exit program')
        return

    inputPdfPath = os.environ.get('INPUT_PDF_PATH', 'ERROR: Specified environment variable is not set.')
    if inputPdfPath in "ERROR":
        logging.fatal('inputPdfPath variable is not set exit program')
        return

    bqTableName = os.environ.get('BQ_TABLENAME', 'ERROR: Specified environment variable is not set.')
    if bqTableName in "ERROR":
        logging.fatal('inputPdfPath variable is not set exit program')
        return

    pdffilename = event['name'].split("/")
    #url = urlparse("gs://"+inputPdfPath+"/"+pdffilename[0])
    #url_parts = list(urlparse("gs://"+inputPdfPath))
    #url_parts[2] = pdffilename
    #pdf = urlunparse(url_parts)

    pdf = "gs://"+inputPdfPath + "/"+pdffilename[0]
    print("pdffilename: "+ pdf)
    process(  
        "gs://" + event['bucket'] +"/" + event['name'],output_bucket, "" ,pdf, bqTableName)

    return 'OK'



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
        outputBlob = gcs_output_uri_prefix  + file

        print("Start write bucket:"+ gcs_output_uri)
        
        bucket = storage_client.get_bucket(gcs_output_uri)

        print("Start write:"+ outputBlob)
        bucket.blob(outputBlob).upload_from_string(data, content_type='application/pdf')

        stream.truncate(0)
    
    print("split finish")
    return outputBlob


def pages_split( document: dict, uri, gcs_output_uri : str, gcs_output_uri_prefix :str , gcs_input_uri : str, table_id : str):
    """
    Document AI identifies possible page splits
    in document. This function converts page splits
    to text snippets and prints it.    
    """

    rows_to_insert = []

    for i, entity in enumerate(document.entities):
        confidence = entity.confidence
        text_entity = ''
        for segment in entity.text_anchor.text_segments:
            start = segment.start_index
            end = segment.end_index
            text_entity += document.text[start:end]
            
        pages = [p.page for p in entity.page_anchor.page_refs]
        print(f"*** Entity number: {i}, Split Confidence: {confidence} ***")
        print(f"*** Pages numbers: {[p for p in pages]} ***\nText snippet: {text_entity[:10]}")
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

        outputBlob = split_pdf(inputpdf, start_page, end_page, uri,gcs_output_uri, gcs_output_uri_prefix + entity.type_)

        row = {u"type": entity.type_ , u"input": uri.geturl(), u"output_split" : outputBlob, u"text" : text_entity}
        rows_to_insert.append(row)
        print(row)
    
    bqInsert(rows_to_insert, table_id)

         
def bqInsert(rows_to_insert, table_id):
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))         

#Synchronous processing
def process(
    gcs_input_uri: str, gcs_output_uri : str, gcs_output_uri_prefix, pdffilename, table_id
):
    if ".json" in gcs_input_uri:
        print("gcs_input_uri:"+gcs_input_uri)
        uri = urlparse(gcs_input_uri)

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(uri.hostname)
        print("bucket:"+bucket.name)
        blob = bucket.get_blob(uri.path[1:])

        uriPdf = urlparse(pdffilename)
        print("getbucket: " + uriPdf.hostname)
        bucketPdf = storage_client.get_bucket(uriPdf.hostname)
        
        print("uripdf: "+uriPdf.path[1:])
        blobPdf = bucketPdf.get_blob(uriPdf.path[1:])
        if blobPdf.exists(): 
            print("Input PDF exist" )
        else:
            print("ERROR Input PDF DOES NOT exist: " + uriPdf)                
            return

        blob_as_bytes = blob.download_as_bytes()

        document = documentai.types.Document.from_json(blob_as_bytes)

        print("Document processing complete.")

        # Read the detected page split from the processor
        print("\nThe processor detected the following page split entities:")
        pages_split( document, uriPdf, gcs_output_uri, gcs_output_uri_prefix, gcs_input_uri, table_id)
    else:
        print("Input file not supported:" + gcs_input_uri)

    

