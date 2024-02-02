from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import WebDriverException
from selenium import webdriver
from selenium.common import exceptions as selenium_exceptions
import chromedriver_autoinstaller
from apify_client import ApifyClient
from google_img_source_search import ReverseImageSearcher
import json
import psycopg2
from supabase import create_client, Client
from datetime import datetime, timezone, timedelta
from fuzzywuzzy import fuzz
from decimal import Decimal
from json import JSONEncoder
import os
import asyncio
import pandas as pd
import numpy as np
import time
import math

chromedriver_autoinstaller.install()


class EmptyElement:
    pass


def find_element_if_exist(parent_element, target_return, *args, **kwargs):
    try:
        child_element = parent_element.find_element(*args, **kwargs)
        if target_return is None:
            return child_element
        else:
            return getattr(child_element, target_return)
    except selenium_exceptions.NoSuchElementException as e:
        return None


def transform_data(json_object):
    data = pd.json_normalize(json_object)

    extensions = [
        "au",
        "com",
        # "Ruelala",
        "com (xpath)",
        "com (japan)",
        "ca",
        # "DermStore",
        "de",
        # "ryan's pet",
        "net",
        "mx",
        "com (canada)",
        # "The Happy Planner",
        # "itcosmetics",
        "eu",
        "it",
        "us",
        "fr",
        "com (france)",
        "ca (chapters)",
        "com (search page)",
        "com (germany)",
        "es",
        # "Bang good",
        # "Luxplus",
        # "Jackson's Art",
        "com (australia)",
        # "Morphe",
        "com (spain)",
        # "drschollsshoes",
        "jp",
        "co",
        "com (gb)",
        "com (italy)",
        # "Penzeys",
        # "brandinteriors",
        # "Parts Town",
        "com (Canada)",
        # "Rogue",
        "com (us)",
        # "UFC Store",
        # "Beauty The Shop",
        # "Honey",
    ]

    def clean_seller(seller_name):
        for extension in extensions:
            seller_name = seller_name.replace(f".{extension}", "")
        return seller_name.replace("-", " ")

    data["seller"] = data["seller"].apply(lambda content: clean_seller(content))

    return data


def insert_new_data(data: pd.DataFrame, table):
    SUPABASE_URL = "https://sxoqzllwkjfluhskqlfl.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InN4b3F6bGx3a2pmbHVoc2txbGZsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDIyODE1MTcsImV4cCI6MjAxNzg1NzUxN30.FInynnvuqN8JeonrHa9pTXuQXMp9tE4LO0g5gj0adYE"
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    today = datetime.utcnow()
    today = today + timedelta(hours=7)
    today = today.strftime("%Y-%m-%d")
    print("Today: ", today)
    data["sys_run_date"] = today

    try:
        print("len data: ", len(data))
        size = 10000
        for i in range(0, len(data), size):
            json_list = (
                data.loc[i : i + size - 1,]
                .copy()
                .replace({np.nan: None})
                .to_dict(orient="records")
            )

            # json_list = data.to_dict(orient="records")
            # Insert the rows into the database using executemany
            response = supabase.table(table).insert(json_list).execute()

            if hasattr(response, "error") and response.error is not None:
                print(f"Error inserting rows: {response.error}")

        print(f"Row inserted successfully")

        data, count = supabase.table(table).delete().lt("sys_run_date", today).execute()
        print("Deleted old data with len: ", data, count)

    except Exception as e:
        print(f"Error with row: {e}")
        # Optionally, break or continue based on your preference


def crawl_data():
    chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument("--headless=new")
    driver = webdriver.Chrome(options=chrome_options)

    driver.get("https://salesgazer.com/customer/login/")
    time.sleep(4)

    user_name = "utytracking@gmail.com"
    password = "i9D3$KD7Ga.aXTC"

    user_name_input = driver.find_element(By.XPATH, '//*[@id="loginusername"]')
    user_name_input.send_keys(user_name)

    password_input = driver.find_element(By.XPATH, '//*[@id="loginpassword"]')
    password_input.send_keys(password)

    submit_button = driver.find_element(
        By.XPATH, "/html/body/div[2]/div/div[1]/form/div[2]/div/div[5]/button"
    )
    submit_button.click()
    time.sleep(5)

    mail_count = float(
        driver.find_element(
            By.XPATH, '//*[@id="inbox-toolbar-toggle-multiple"]/div[3]/span[2]'
        ).text
    )

    max_page = math.ceil(mail_count / 200.0)
    print("Mail count: ", mail_count)
    print("Max page: ", max_page)

    coupons_data = []

    for page_number in range(1, max_page + 1):
        page_link = f"https://salesgazer.com/mailbox/?p={page_number}&per_page=200&q="
        print("Page: ", page_number)
        print("Link: ", page_link)

        driver.get(page_link)
        time.sleep(5)

        table_content = driver.find_element(By.CSS_SELECTOR, ".rowlink")
        mails = table_content.find_elements(By.CSS_SELECTOR, "tr:not(.ad)")

        print(len(mails))

        for mail in mails:
            seller = mail.find_element(By.CSS_SELECTOR, "td.table-inbox-name").text
            content = mail.find_element(By.CSS_SELECTOR, "td.table-inbox-message")
            time_coupon = mail.find_element(By.CSS_SELECTOR, "td.table-inbox-time").text
            title = content.text
            link = content.find_element(By.CSS_SELECTOR, "a").get_attribute("href")

            coupon_data = {}
            coupon_data["seller"] = seller
            coupon_data["title"] = title
            coupon_data["link"] = link
            coupon_data["time_coupon"] = time_coupon
            coupons_data.append(coupon_data)
    return coupons_data


def main():
    coupons_data = crawl_data()
    clean_data = transform_data(coupons_data)
    insert_new_data(clean_data, "coupon_sales_gazer")


if __name__ == "__main__":
    main()
