from google.cloud import bigquery

client = bigquery.Client()

def get_results_pages(table_name, rows_size=5000):
    """
    Performs a query on the specified BigQuery table.

    Args:
        table_name: The fully qualified name of the table to query.
        rows_size: The number of rows to fetch per page.

    Returns:
        A list of pages from the query result.
    """
    query = f"SELECT * FROM `{table_name}`"
    query_job = client.query(query)
    return query_job.result(page_size=rows_size).pages
