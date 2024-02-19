from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from supabase import create_client, Client
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import traceback
import tempfile
import json
import os

pd.options.mode.use_inf_as_na = True

GG_API_SERVICE = None


def get_coupon(supabase, table_name, sys_run_date):
    print("table_name: ", table_name, " sys_run_date: ", sys_run_date)
    data = (
        supabase.table(table_name)
        .select("*")
        .eq("sys_run_date", sys_run_date)
        .execute()
    )
    df = pd.json_normalize(data.model_dump()["data"])
    print(df.shape)
    return df


def get_credential(credentials_file="cred.json"):
    scopes = ["https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_file(credentials_file, scopes=scopes)
    return credentials


def get_service():
    global GG_API_SERVICE
    if GG_API_SERVICE is not None:
        return GG_API_SERVICE

    GG_API_SERVICE = build("drive", "v3", credentials=get_credential())

    return GG_API_SERVICE


def delete_old_files(drive_service, name):
    results = (
        drive_service.files().list(q=f"name='{name}'", fields="files(id)").execute()
    )
    items = results.get("files", [])
    print(items)

    for item in items:
        drive_service.files().delete(fileId=item["id"]).execute()
    # if items:
    #     return items[0]["id"]
    # else:
    #     return None


def upload_file(drive_service, file_path, folder_id):
    file_metadata = {"name": os.path.basename(file_path)}
    if folder_id:
        file_metadata["parents"] = [folder_id]
    media = MediaFileUpload(
        file_path,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    file = (
        drive_service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute()
    )
    print("File ID:", file.get("id"))


def main():
    coupon_tables = [
        "coupon_data2",
        "coupon_dealnews",
        "coupon_dealsofamerica",
        "coupon_sales_gazer",
    ]

    current_date = datetime.now()
    sys_run_date = current_date.strftime("%Y-%m-%d")
    folder_id = "1WX1HTUT8RjiJbTyf1SQIGN3eKoklXO71"

    # excel_writer = pd.ExcelWriter("coupons_data.xlsx", mode="w")
    with tempfile.TemporaryDirectory() as download_dir:
        file_name = f"{sys_run_date}.xlsx"
        file_path = os.path.join(download_dir, file_name)
        with pd.ExcelWriter(file_path) as excel_writer:
            for table_name in coupon_tables:
                coupons = get_coupon(supabase, table_name, sys_run_date)
                coupons.to_excel(excel_writer, table_name, index=False)

        drive_service = get_service()
        delete_old_files(drive_service, file_name)
        upload_file(drive_service, file_path, folder_id)


SUPABASE_URL = "https://sxoqzllwkjfluhskqlfl.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InN4b3F6bGx3a2pmbHVoc2txbGZsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDIyODE1MTcsImV4cCI6MjAxNzg1NzUxN30.FInynnvuqN8JeonrHa9pTXuQXMp9tE4LO0g5gj0adYE"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


if __name__ == "__main__":
    main()
