from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="../gcp-key.json"
# Construct a BigQuery client object.
client = bigquery.Client()

query = """
    SELECT *,
	Target_paid_late from ML.PREDICT(MODEL `machine-learning-qlik-sense.SAP.paid_time_auto_ml`, (SELECT
  CountryCode,
	CustomerID,
	InvoiceDate,
	InvoiceAmount,	
	Disputed, 	
	PaperlessBill,
	Target_paid_late
	FROM
`machine-learning-qlik-sense.SAP.paid_late_financial`)) limit 1
"""
query_job = client.query(query)  # Make an API request.

print("The query data:")
for row in query_job:
    # Row values can be accessed by field name or index.
    print("row={}".format(row))