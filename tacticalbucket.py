from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common import exceptions as selenium_exceptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import chromedriver_autoinstaller
from supabase import create_client, Client
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import time

# from pyvirtualdisplay import Display

# display = Display(visible=0, size=(800, 600))
# display.start()

chromedriver_autoinstaller.install()


class EmptyElement:
    pass


def find_element_if_exist(parent_element, target_return, *args, **kwargs):
    try:
        child_element = parent_element.find_element(*args, **kwargs)
        if target_return is None:
            return child_element
        else:
            return child_element.get_attribute(target_return)
    except selenium_exceptions.NoSuchElementException as e:
        return None


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
    chrome_options.add_argument("--headless=new")
    driver = webdriver.Chrome(options=chrome_options)

    username = "uty.tra@thebargainvillage.com"
    password = "2292186@Vy"

    driver.get("https://www.tacticalbucket.com/dashboard/discount-list/")
    time.sleep(7)
    username_input = driver.find_element(By.XPATH, '//*[@id="id_username"]')
    username_input.send_keys(username)
    time.sleep(4)

    password_input = driver.find_element(By.XPATH, '//*[@id="id_password"]')
    password_input.send_keys(password)
    time.sleep(3)

    sign_in_button = driver.find_element(
        By.XPATH, "/html/body/div/div[1]/form/div[4]/button"
    )
    sign_in_button.click()
    time.sleep(25)

    wait = WebDriverWait(driver, 20)
    table_element = wait.until(
        EC.visibility_of_element_located((By.XPATH, '//*[@id="discount_datatable"]'))
    )

    table_header = table_element.find_element(By.CSS_SELECTOR, "thead")
    header_element = table_header.find_elements(By.CSS_SELECTOR, "th")
    headers = [item.get_attribute("innerText") for item in header_element]
    print(headers)

    coupons_data = []

    page_count = 0
    row_count = 0
    while True:
        page_count += 1
        print(f"Page {page_count}")

        table_element = wait.until(
            EC.visibility_of_element_located(
                (By.XPATH, '//*[@id="discount_datatable"]')
            )
        )
        body_element = table_element.find_element(By.CSS_SELECTOR, "tbody")
        rows = body_element.find_elements(By.CSS_SELECTOR, "tr")
        for row in rows:
            row_count += 1
            print("Row: ", row_count)

            row_data = row.find_elements(By.CSS_SELECTOR, "td")
            coupon_data = []

            for cell_data in row_data:
                try:
                    link_element = cell_data.find_element(By.CSS_SELECTOR, "a")

                    link = link_element.get_attribute("href")
                    if "api" in link:
                        # print("Open new tab to get redirect URL")
                        ActionChains(driver).key_down(Keys.CONTROL).click(
                            link_element
                        ).key_up(Keys.CONTROL).perform()
                        time.sleep(1.5)

                        # After opening the link in a new tab, you can switch to the new tab
                        # Get window handles to switch between tabs
                        driver.switch_to.window(driver.window_handles[-1])
                        time.sleep(5)
                        # Update new href
                        link = driver.current_url

                        driver.close()
                        driver.switch_to.window(driver.window_handles[0])

                    coupon_data.append(link)
                    # print(link)

                except selenium_exceptions.NoSuchElementException:
                    text = cell_data.get_attribute("innerText")
                    coupon_data.append(text)
                    # print(text)

            coupons_data.append(coupon_data)
            # print(coupon_data)
            # break

        # Open next page
        try:
            next_button = driver.find_element(
                By.CSS_SELECTOR, "#discount_datatable_next:not(.disabled)"
            )
            next_button.click()
            print("Next page")
            time.sleep(8.2)
        except:
            print("There is no next page")
            break

    print("Len coupons: ", len(coupons_data))
    data = pd.DataFrame(coupons_data, columns=headers)
    # time.sleep(100)

    headers = [item.lower().replace(" ", "_") for item in headers]

    data = pd.DataFrame(coupons_data, columns=headers)
    data = data.replace("✔", True).replace("✘", False).replace("-", None)

    return data


def main():
    coupons_data = crawl_data()
    insert_new_data(coupons_data, "coupon_data2")


if __name__ == "__main__":
    main()
