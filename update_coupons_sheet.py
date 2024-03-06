from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
from supabase import create_client, Client
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import traceback
import tempfile
import json
import os
import sys
import re
import io


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


def clean_brand_name(brand_name):
    temp_list1 = (
        brand_name.replace("https://", "")
        .replace("http://", "")
        .replace("www.", "")
        .split(".")
    )

    return temp_list1[0]


def filter_coupon_data2(df: pd.DataFrame, ip_sellers: list):
    df_copy = df.copy()
    df_copy["total_discount"] = (
        df_copy["total_discount"].str.replace("%", "").astype("float")
    )
    filter_df = df[df_copy["total_discount"] >= 40]
    return filter_df


def filter_product_dealnews(df, ip_sellers):
    df_copy = df[
        (df["type"] == "Product Discounts ")
        & (df.seller.apply(lambda x: True if x not in ip_sellers else False))
    ].copy()

    df_copy["original_price"] = (
        df_copy["original_price"]
        .str.replace("$", "")
        .str.replace(",", "")
        .replace("", np.NaN)
        .astype("float")
    )

    df_copy["min_sales"] = df_copy.sales.apply(
        lambda x: min(
            [float(item.replace(",", "")) for item in re.findall(r"\$(\S*)", x)]
            + [np.nan]
        )
    )

    df_copy["sales_percent"] = 1 - df_copy["min_sales"] / df_copy["original_price"]

    chosen_series = (df_copy["sales_percent"] >= 0.4) & (
        df_copy["original_price"] <= 100
    )
    filtered_df = df.loc[chosen_series[chosen_series].index]
    return filtered_df


def filter_store_event_dealnews(df, ip_sellers):
    df_copy = df[
        (df["type"] == "Store Sales & Events ")
        & (df.seller.apply(lambda x: True if x not in ip_sellers else False))
    ].copy()

    df_copy["max_sales"] = df_copy.sales.apply(
        lambda x: max(
            [float(item.replace(",", "")) for item in re.findall(r"(\S*)%", x)]
            + [np.nan]
        )
    )

    chosen_series = df_copy["max_sales"] >= 40
    filtered_df = df.loc[chosen_series[chosen_series].index]
    return filtered_df


def filter_coupon_dealnews(df: pd.DataFrame, ip_sellers: list):
    product_filtered_df = filter_product_dealnews(df, ip_sellers)
    store_event_df = filter_store_event_dealnews(df, ip_sellers)
    filtered_df = pd.concat([product_filtered_df, store_event_df])
    return filtered_df


def filter_coupon_dealsofamerica(df: pd.DataFrame, ip_sellers: list):
    df_copy = df[
        (df.seller.apply(lambda x: True if x not in ip_sellers else False))
    ].copy()

    df_copy["original_price"] = (
        df_copy["original_price"]
        .str.replace("$", "")
        .str.replace(",", "")
        .replace("", np.NaN)
        .astype("float")
    )

    df_copy["sales"] = (
        df_copy["sales"]
        .str.replace("$", "")
        .str.replace(",", "")
        .str.replace("FREE", "1000")
        .replace("", np.NaN)
        .astype("float")
    )

    df_copy["sales_percent"] = 1 - df_copy["sales"] / df_copy["original_price"]
    # df_copy.to_csv("test_data/product_discount.csv")

    chosen_series = (df_copy["sales_percent"] >= 0.4) & (
        df_copy["original_price"] <= 100
    )
    filtered_df = df.loc[chosen_series[chosen_series].index]
    return filtered_df


def filter_coupon_sales_gazer(df: pd.DataFrame, ip_sellers: list):
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


def get_csv_content(drive_service, file_id):
    result = (
        drive_service.files()
        .export_media(fileId=file_id, mimeType="text/csv")
        .execute()
    )
    file = io.BytesIO(result)
    df = pd.read_csv(file)

    return df


def delete_old_files(drive_service, name):
    results = (
        drive_service.files().list(q=f"name='{name}'", fields="files(id)").execute()
    )
    items = results.get("files", [])
    print(items)

    for item in items:
        drive_service.files().delete(fileId=item["id"]).execute()


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
    seller_names_in_table = {
        "coupon_data2": "store",
        "others": "seller",
    }

    ip_sellers = [
        "ebay",
        "amazon",
        "woot an amazon company",
        "lenovo",
        "dell technologies",
        "dell",
        "samsung",
    ]

    current_date = datetime.now()
    sys_run_date = current_date.strftime("%Y-%m-%d")
    folder_id = "1WX1HTUT8RjiJbTyf1SQIGN3eKoklXO71"
    IP_list_file_ID = "18y0gy6uzeOV_eqCpVbJlsgkoNTGWgihWJVGVxj4eBiE"

    DRIVER_SERVICE = get_service()

    IP_list_df = get_csv_content(DRIVER_SERVICE, IP_list_file_ID)
    col = IP_list_df.columns[0]
    IP_list = IP_list_df[col].values.tolist()
    IP_list = set(
        [
            seller.replace(" ", "").lower() if isinstance(seller, str) else ""
            for seller in IP_list
        ]
    )
    # print(IP_list)
    # print("samsclub" in IP_list)

    with tempfile.TemporaryDirectory() as download_dir:
        file_name = f"{sys_run_date}.xlsx"
        file_path = os.path.join(download_dir, file_name)
        with pd.ExcelWriter(file_path) as excel_writer:
            for table_name in coupon_tables:
                try:
                    coupons = get_coupon(supabase, table_name, sys_run_date)
                    if len(coupons) == 0:
                        continue

                    seller_name_default = "seller"
                    seller_name = seller_names_in_table.get(
                        table_name, seller_name_default
                    )
                    seller_list = coupons[seller_name].values
                    seller_list = set(
                        [
                            (
                                seller.replace(" ", "").lower()
                                if isinstance(seller, str)
                                else ""
                            )
                            for seller in seller_list
                        ]
                    )

                    seller_ip = seller_list - (seller_list - IP_list)
                    # print("seller_list: ", seller_list)
                    print("seller_ip: ", seller_ip)
                    coupons = coupons[
                        coupons[seller_name].apply(
                            lambda x: x.replace(" ", "").lower() not in seller_ip
                        )
                    ]

                    filter_function = getattr(
                        sys.modules[__name__], f"filter_{table_name}"
                    )
                    filtered_df = filter_function(coupons, ip_sellers)
                    filtered_df.to_csv(f"test_data/{table_name}.csv", index=False)
                    filtered_df.to_excel(excel_writer, table_name, index=False)
                except Exception as e:
                    print(e)
                    traceback.print_exc()

            DRIVER_SERVICE2 = get_service()
        delete_old_files(DRIVER_SERVICE2, file_name)
        upload_file(DRIVER_SERVICE2, file_path, folder_id)


SUPABASE_URL = "https://sxoqzllwkjfluhskqlfl.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InN4b3F6bGx3a2pmbHVoc2txbGZsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDIyODE1MTcsImV4cCI6MjAxNzg1NzUxN30.FInynnvuqN8JeonrHa9pTXuQXMp9tE4LO0g5gj0adYE"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


if __name__ == "__main__":
    main()
