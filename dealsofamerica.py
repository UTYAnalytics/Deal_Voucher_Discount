from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.common import exceptions as selenium_exceptions
from supabase import create_client, Client
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import time
from selenium.webdriver.chrome.service import Service

chrome_driver_path = r"chromedriver-win32\chromedriver.exe"
service = Service(executable_path=chrome_driver_path)

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

    data["other_benefic"] = data["other_benefic"].apply(
        lambda content: content.split(" + ")[-1] if content is not None else None
    )
    data["other_benefic"] = data["other_benefic"].apply(
        lambda content: (
            content if content is not None and content.find("Shipping") != -1 else None
        )
    )

    data["time_coupon"] = data["time_coupon"].str.replace("Posted at ", "")

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

        # data, count = supabase.table(table).delete().lt("sys_run_date", today).execute()
        # print("Deleted old data with len: ", data, count)

    except Exception as e:
        print(f"Error with row: {e}")
        # Optionally, break or continue based on your preference


def crawl_data():
    chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument("--headless=new")
    driver = webdriver.Chrome(service=service,options=chrome_options)

    # driver.get("https://www.dealsofamerica.com/walmart-deals.php")
    driver.get("https://www.dealsofamerica.com")
    time.sleep(4)

    # wait = WebDriverWait(driver, 10)  # Adjust the timeout value as needed

    seller_link_container = driver.find_element(
        By.XPATH, "/html/body/header/div/div[1]/nav/ul/li[2]/ul/li[2]/ul/li[3]/ul"
    )

    seller_link_elements = seller_link_container.find_elements(By.CSS_SELECTOR, "a")
    seller_links = [element.get_attribute("href") for element in seller_link_elements]
    print("len seller: ", len(seller_links))
    coupons_data = []

    for link in seller_links:
        seller_name = link.replace("-deals.php", "").split("/")[-1].replace("-", " ")
        print("Link: ", link)
        print("Seller: ", seller_name)

        driver.get(link)
        time.sleep(5)

        while True:
            coupon_div_list = driver.find_elements(By.CSS_SELECTOR, "section.deal.row")
            print("len coupons list: ", len(coupon_div_list))

            for coupon_div in coupon_div_list:
                coupon_data = {}
                # print(coupon_div.get_attribute("outerHTML"))
                # print(coupon_div.text)
                # coupon_data["type"] = groups_heading[i]
                # print(coupon.text)

                title = find_element_if_exist(
                    coupon_div,
                    "text",
                    By.CSS_SELECTOR,
                    "section > header > div.title > a",
                )
                link = find_element_if_exist(
                    coupon_div, None, By.CSS_SELECTOR, "section > footer > a"
                ).get_attribute("href")

                sales_price = find_element_if_exist(
                    coupon_div,
                    "text",
                    By.CSS_SELECTOR,
                    "div > div > span.our-price",
                )
                original_price = find_element_if_exist(
                    coupon_div, "text", By.CSS_SELECTOR, "div > div > span.list-price"
                )
                other_benefic = find_element_if_exist(
                    coupon_div, "text", By.CSS_SELECTOR, "ul > li > span.cpriceb"
                )
                # other_benefic = " + ".join(other_benefic.split(" + ")[1:])

                time_coupon = find_element_if_exist(
                    coupon_div,
                    "text",
                    By.CSS_SELECTOR,
                    "section > header > div.store_time_div > time",
                )
                # print(time_coupon)
                coupon_data["title"] = title
                coupon_data["link"] = link
                coupon_data["sales"] = sales_price
                coupon_data["original_price"] = original_price
                coupon_data["other_benefic"] = other_benefic
                coupon_data["time_coupon"] = time_coupon
                coupon_data["seller"] = seller_name

                # print(coupon_data)

                # print("title: ", title_element.text)
                # print("link: ", link)
                # print("sales_price: ", sales_price)
                # print("original_price: ", original_price)
                # print("other_benefic: ", other_benefic)
                # print()
                # print()

                coupons_data.append(coupon_data)
            print("Len coupons data: ", len(coupons_data))

            next_page = find_element_if_exist(
                driver, None, By.XPATH, '//*[@id="deals-container"]/footer/ul/li/a'
            )
            if next_page is None:
                print("There is no next page. Returning")
                break

            print("Go to next page.")
            next_page.click()
            time.sleep(5)
    return coupons_data


def main():
    coupons_data = crawl_data()
    clean_data = transform_data(coupons_data)
    insert_new_data(clean_data, "coupon_dealsofamerica")


if __name__ == "__main__":
    main()
