import os
import time

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

colors = ["green", "yellow"]


def setup_driver():
    options = webdriver.ChromeOptions()
    download_dir = os.path.expanduser("~/Downloads/nyc_taxi_data")
    os.makedirs(download_dir, exist_ok=True)  # Create the directory if it doesn't exist
    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )


def download_green_taxi_data():
    driver = setup_driver()

    # Navigate to the NYC TLC Trip Record Data page
    url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    driver.get(url)
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (By.XPATH, "//div[contains(@class, 'faq-questions')]")
        )
    )
    time.sleep(3)  # Wait for the page to load

    # Click the "Expand All" button
    expand_all_button = driver.find_element(By.CLASS_NAME, "faq-expand-all")
    expand_all_button.click()
    time.sleep(2)  # Wait for all sections to expand

    # Click the button to expand the section for the specified year
    for year in range(2024, 2014, -1):
        print(f"Downloading data for {year}...")

        # # Find the section for the specified year
        # button = driver.find_element(By.XPATH, f"//div[@data-answer='faq{year}']")
        # button.click()  # Click to expand the year section
        # time.sleep(1)  # Wait for the section to expand

        # Find the taxi trip records link
        links = driver.find_elements(By.TAG_NAME, "a")
        for link in links:
            href = link.get_attribute("href")
            for color in colors:
                if (
                    href
                    and f"{color}_tripdata" in href
                    and f"{year}" in href
                    and href.endswith(".parquet")
                ):
                    try:
                        print(f"Clicking and downloading: {href}")
                        # Scroll to the element
                        driver.execute_script(
                            "arguments[0].scrollIntoView(true);", link
                        )
                        # Wait until the element is clickable
                        WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable(link)
                        )

                        # Use JavaScript to click as a fallback
                        driver.execute_script("arguments[0].click();", link)

                        time.sleep(10)  # Wait for the download to initiate
                        break  # Exit after the first match
                    except Exception as e:
                        print(f"Failed to click the link: {e}")

    print("Downloads should be complete. Keeping browser open for you to review.")

    # Wait indefinitely until you manually close the browser
    input("Press Enter to close the browser...")

    driver.quit()


if __name__ == "__main__":
    download_green_taxi_data()
