'''
xpath change will break the code
change years if neccesary : change location for a different location
'''


import os
import time
import glob
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


def search_months(years=[2014, 2015, 2016, 2017, 2018, 2019]):
    '''change years if required'''

    months = range(1, 13)
    searcher = []

    for year in years:
        for month in months:
            searcher.append(str(year)+"-"+str(month))

    return searcher


def scrape_website(link_to_search):
    ''' Make request to wunderground '''

    DELAY = 10

    driver = webdriver.Chrome()
    driver.get(link_to_search)
    count = 0

    if count < 3:
        try:

            daily_history = '''//li[@class="calendar-day current-month history"]'''

            element = WebDriverWait(driver, DELAY).until(
                EC.presence_of_element_located(
                    (By.XPATH, daily_history)))

            daily_element = element.find_elements_by_xpath(daily_history)

            history = [i.text for i in daily_element]
            # print(history)
            time.sleep(1)

            return history

        except Exception as exception_chrome:

            count = count+1
            print(exception_chrome)
    else:
        print("data not available")


def scrape_weather_data_monthly(location, searcher):
    '''document requests from the search '''

    main_df = pd.DataFrame()

    for dates in searcher:
        k = []
        print(dates)
        try:
            weather_df = scrape_website(
                link_to_search=f'https://www.wunderground.com/calendar/us/ny/new-york-city/{location}/date/{dates}?cm_ven=localwx_calendar')

            for i in weather_df:
                k.append(i.split("Actual")[0].split())
            k = pd.DataFrame(k)
            k['date'] = dates
            print(k.shape, dates)
            main_df = pd.concat([main_df, k], ignore_index=True)

        except Exception as unavailable_data:
            print(unavailable_data, dates, "weather data is not available")

    return main_df


def weather_dataframe(proxy_df):
    '''creates weather dataframe '''

    main_df = proxy_df
    main_df.columns = ['date_number', 'weather_1',
                       'weather_2', 'weather_3', 'date']
    main_df['true_date'] = main_df['date'] + \
        '-'+main_df['date_number'].astype(str)
    main_df['weather_2'].fillna(" ", inplace=True)
    main_df['true_weather'] = main_df['weather_1']+' '+main_df['weather_2']

    return main_df


def scrape_weather_data_monthly(location, searcher):
    '''document requests from the search '''

    main_df = pd.DataFrame()
    dummy_count = 0
    for dates in searcher:
        k = []
        print(dates)
        if dummy_count <= 3:
            try:
                weather_df = scrape_website(
                    link_to_search=f'https://www.wunderground.com/calendar/us/ny/new-york-city/{location}/date/{dates}?cm_ven=localwx_calendar')

                for i in weather_df:
                    k.append(i.split("Actual")[0].split())
                k = pd.DataFrame(k)
                k['date'] = dates
                print(k.shape, dates)
                main_df = pd.concat([main_df, k], ignore_index=True)

            except Exception as scrape_exception:
                dummy_count = dummy_count+1
                print(scrape_exception, dates, "weather data is not available")
        else:
            print("future weather data")

    return main_df


def main_monthly(location):
    '''scrapes the weather data monthly'''

    search_years = [2014, 2015, 2016, 2017, 2018, 2019, 2020]
    # search_years=['2019'] : for getting only one specific year
    date_codes = search_months(years=search_years)
    weather = scrape_weather_data_monthly(
        location=location, searcher=date_codes)
    weather_df = weather_dataframe(proxy_df=weather)
    weather_df = weather_df[['true_date', "true_weather"]]
    print(weather_df.shape)
    weather_df.to_csv(f"weather_data_pull_for_{location}.csv", index=False)
    print(f'file is saved as ,weather_data_pull_for_{location}.csv')
    print("job done")

    return weather


def get_dates_for_daily_scrape(last_scrape_date, location):
    '''function to scrape the weather data daily'''

    search_years = [2014, 2015, 2016, 2017, 2018, 2019, 2020]
    date_codes = search_months(years=search_years)
    refresh_dates = date_codes[date_codes.index(
        last_scrape_date):date_codes.index(last_scrape_date)+2]

    weather = scrape_weather_data_monthly(
        location=location, searcher=refresh_dates)
    weather_df = weather_dataframe(proxy_df=weather)
    weather_df = weather_df[['true_date', "true_weather"]]
    weather_df = weather_df.drop_duplicates(subset={'true_date'})
    weather_df['location'] = location
    return weather_df


def main_daily(location):
    '''main function which appends to the existing weather data of a particular station'''

    main_df = pd.read_csv(f"weather_data_pull_for_{location}.csv")
    last_scrape = main_df['true_date'].iloc[-1][:-3]
    recent_scrape_data = get_dates_for_daily_scrape(last_scrape, location)

    main_df = pd.concat([main_df, recent_scrape_data], ignore_index=False)
    weather_df = main_df.drop_duplicates(subset={'true_date'})
    weather_df['location'] = location
    weather_df.to_csv(f"weather_data_pull_for_{location}.csv", index=False)

    return weather_df


def upload_weather_to_S3(location):
    '''upload to s3 :dependency - aws cli has to be configured'''
    files = glob.glob(f"weather_data_pull_for_{location}.csv")

    if f"weather_data_pull_for_{location}.csv" in files:
        os.system(
            f'aws s3 cp  weather_data_pull_for_{location}.csv s3://nextorbitweather')
        print("file uploaded to S3")


def KLGA_weather_pull():
    '''klga specific function'''
    location = 'KLGA'
    files = glob.glob(f"weather_data_pull_for_{location}.csv")

    if "weather_data_pull_for_KLGA.csv" not in files:
        main = main_monthly(location="KLGA")

    updated_weather_data = main_daily(location="KLGA")

    upload_weather_to_S3(location)


if __name__ == '__main__':
    KLGA_weather_pull()


'''thing to do weather data daily'''
