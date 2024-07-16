from google.cloud import storage

storage_client = storage.Client()

async def upload(bucket_name, name, data):
    """
    Uploads a PDF to Cloud Storage.

    Args:
        bucket_name: The name of the Cloud Storage bucket.
        name: The name of the PDF file.
        data: The PDF content as a byte string.
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f'{name}.pdf')
    blob.upload_from_string(data, content_type='application/pdf')