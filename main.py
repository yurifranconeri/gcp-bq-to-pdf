import json
import logging
import sys
import time
import asyncio
import uuid

from concurrent.futures import ThreadPoolExecutor

import bq_utils
import pdf_utils
import storage_utils

logging.basicConfig(level=logging.INFO)

# Environment variables
BUCKET_NAME = "results-pdfs"
BIGQUERY_TABLE = "resolute-return-427518-g1.teste.tableteste"
MAX_WORKERS = 8

def process():
    """Processes BigQuery results, generating and uploading PDFs."""
    
    logging.info(f"Getting data")
    pages = bq_utils.get_results_pages(BIGQUERY_TABLE)
    
    logging.info(f"Generating and uploading pdfs")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:  # Adjust max_workers as needed
        for page in pages:
            executor.submit(generate_pdfs_and_upload, page)

def generate_pdfs_and_upload(pages):
    """Processes pages of data, generating and uploading PDFs."""
    asyncio.run(process_results(pages))

async def process_results(rows):
    """Processes rows of data, generating and uploading PDFs."""
    tasks = []

    for row in rows:
        pdf_content=generate_pdf(row)
        tasks.append(upload_pdf_async(uuid.uuid4().hex, pdf_content))

    await asyncio.gather(*tasks)

def generate_pdf(row):
    pdf_content = pdf_utils.render_pdf(row)
    return pdf_content

async def upload_pdf_async(pdf_name, pdf_content):
    await storage_utils.upload(BUCKET_NAME, pdf_name, pdf_content)

if __name__ == "__main__":
    try:
        start_time = time.time()
        logging.info(f"Start Processing")
        process()
        total_time = time.time() - start_time
        logging.info(f"Total processing time: {total_time:.2f} seconds")
    except Exception as err:
        logging.error(f"Job failed: {str(err)}")
        sys.exit(1)