import boto3
import pandas as pd
import os
import io
import openpyxl

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    xlsx_key = event['Records'][0]['s3']['object']['key']

    if not xlsx_key.endswith('.xlsx'):
        return

    # Get the file from S3
    response = s3.get_object(Bucket=bucket, Key=xlsx_key)
    excel_bytes = response['Body'].read()

    # Load workbook
    excel_io = io.BytesIO(excel_bytes)
    xls = pd.ExcelFile(excel_io)

    for sheet in xls.sheet_names:
        df = xls.parse(sheet)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        csv_key = f"converted/{os.path.splitext(xlsx_key)[0]}_{sheet}.csv"
        s3.put_object(Bucket=bucket, Key=csv_key, Body=csv_buffer.getvalue())
