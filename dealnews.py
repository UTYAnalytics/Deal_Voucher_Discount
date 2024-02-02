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

    data["time_coupon"] = data["time_coupon"].apply(
        lambda string: string.split("\u00b7 ")[-1]
    )
    data["seller"] = data["seller"].apply(
        lambda string: string.replace("-", " ").lower()
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

        # data, count = supabase.table(data).delete().lt("sys_run_date", today).execute()
        # print("Deleted old data with len: ", count)

    except Exception as e:
        print(f"Error with row: {e}")
        # Optionally, break or continue based on your preference


def crawl_data():
    chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument("--headless=new")
    driver = webdriver.Chrome(service=service, options=chrome_options)

    order = "?group=type&sort=time"
    #
    driver.get("https://www.dealnews.com/s321/Walmart/")
    time.sleep(4)
    accept_cookie_button = driver.find_element(
        By.XPATH, '//*[@id="privacy_banner"]/div/div[2]/ul/li/div[2]/button'
    )
    accept_cookie_button.click()

    sellers_div = driver.find_element(By.XPATH, '//*[@id="nav-menu"]/ul/li[2]/div')
    link_elements = sellers_div.find_elements(By.TAG_NAME, "a")
    links = [ele.get_attribute("href") for ele in link_elements]
    print(links)
    coupons_data = []

    for link in links:
        seller = link.split("/")[-2]
        print("Link: ", link)
        print("Seller: ", seller)
        if seller in ["Amazon", "Walmart", "online-stores"]:
            continue

        driver.get(link + order)
        time.sleep(4)

        count_element = driver.find_element(By.CSS_SELECTOR, "div.count")
        count = count_element.text.split(" ")[0]
        if int(count) == 0:
            continue
        # Adjust the timeout value as needed

        while 1:
            try:
                more_button_container = driver.find_element(
                    By.CSS_SELECTOR, ".dynamic-grid-pager:not(.hidden)"
                )
                more_button = more_button_container.find_element(
                    By.CSS_SELECTOR, ".btn-hero.btn-positive.btn-block.pager-more"
                )
                more_button.click()
                print("clicked more")
                time.sleep(2)
            except selenium_exceptions.NoSuchElementException as e:
                print("There is no more")
                break

        time.sleep(5)
        section_headings = driver.find_elements(
            By.CSS_SELECTOR, ".override-anchor-color"
        )
        groups_heading = [heading.text.split("(")[0] for heading in section_headings]
        print(groups_heading)

        group_content_list = driver.find_elements(By.CSS_SELECTOR, ".footprint-group")
        print(len(group_content_list))

        for i in range(len(group_content_list)):
            group_content = group_content_list[i]
            coupon_div_list = group_content.find_elements(
                By.CSS_SELECTOR, ".content-card-initial"
            )
            print("len coupons list: ", len(coupon_div_list))

            for coupon_div in coupon_div_list:
                coupon_data = {}
                coupon_data["type"] = groups_heading[i]
                # print(coupon.text)

                time_coupon = find_element_if_exist(
                    coupon_div,
                    "text",
                    By.CSS_SELECTOR,
                    "div.key-attribute.limit-height.limit-height-large-1.limit-height-small-1",
                )
                title_element = find_element_if_exist(
                    coupon_div, None, By.CSS_SELECTOR, "a.title-link"
                )
                link = title_element.get_attribute("href")
                sales_element = find_element_if_exist(
                    coupon_div, None, By.CSS_SELECTOR, ".callout-group"
                )
                if sales_element is None:
                    continue
                sales_price = find_element_if_exist(
                    sales_element,
                    "text",
                    By.CSS_SELECTOR,
                    ".callout.limit-height.limit-height-large-1.limit-height-small-1",
                )

                original_price = find_element_if_exist(
                    sales_element, "text", By.CSS_SELECTOR, ".callout-comparison"
                )
                if original_price is not None:
                    sales_price = sales_price.replace(original_price, "")
                    original_price = original_price.strip()

                sales_price = sales_price

                other_benefic = find_element_if_exist(
                    sales_element, "text", By.CSS_SELECTOR, ".secondary-callout"
                )

                coupon_data["title"] = title_element.text
                coupon_data["link"] = link
                coupon_data["sales"] = sales_price
                coupon_data["original_price"] = original_price
                coupon_data["other_benefic"] = other_benefic
                coupon_data["time_coupon"] = time_coupon
                coupon_data["seller"] = seller

                # print(coupon_data)

                # print("title: ", title_element.text)
                # print("link: ", link)
                # print("sales_price: ", sales_price)
                # print("original_price: ", original_price)
                # print("other_benefic: ", other_benefic)
                # print()
                # print()

                coupons_data.append(coupon_data)
    return coupons_data


def main():
    coupons_data = crawl_data()
    clean_data = transform_data(coupons_data)
    insert_new_data(clean_data, "coupon_dealnews")


if __name__ == "__main__":
    main()
