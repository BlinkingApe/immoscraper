'''
Webscraper for German real estate site
immoscout24 - https://www.immobilienscout24.de/
'''


# module imports
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os

# paste complete urls from the immoscout search page
# apartments to buy search url
wohnung_url = 'https://www.immobilienscout24.de/Suche/S-T/Wohnung-Kauf/Polygonsuche/ot%7BxHuwih@yg@mb@uLeBaUfJaVsOr@NkQajAj@qNxs@qiA%60OaGjUfJ%60S_Gxf@j_A%60Uc@bUeBv%5BiK%7CkAkmAni@%60%7DBqsCjuAaLhC_MzK/-/80,00-/EURO--200000,00?enteredFrom=result_list#/'

# houses to buy search url
haus_url = wohnung_url.replace('Wohnung', 'Haus')

# apartments to rent search url
rent_url = 'https://www.immobilienscout24.de/Suche/S-T/Wohnung-Miete/Polygonsuche/yxwxHgynh@k@e@jDO;_dyxHcmih@g%7D@oFw%5B%7DSaNvBm%5B_NaP~Fk_@%7BSkKy%60@aCkp@%60Ukb@dw@e%7C@%60Fq@%60%5DvQx%60@zp@%60POvRtPpx@aW~FzET%7CLkHvkAhBhh@aOhZ/-/80,00-/EURO--1000,00?enteredFrom=result_list#/'

# get current date#
current_datetime = datetime.now().strftime('%Y_%m_%d')


def main():

    # column names for the pandas DataFrame
    columns = ['city', 'price', 'house_size', 'land_size', 'rooms', 'location', 'commission', 'listing_url']

    # initialize DataFrames with column names
    df_immo = pd.DataFrame(columns=columns)

    # fill DataFrame with data from web crawler
    df_immo = df_immo.assign(**get_data())

    # write the uncleaned raw data
    write_raw(df_immo)

    # clean the data
    df_clean = clean_df(df_immo)

    # write the cleaned data
    write_clean(df_clean)


# prepare BeautifulSoup object for data extraction
def make_soup(url):

    # generate HTTP requests for search
    try:
        req = requests.get(url)
    except Exception:
        print("make_soup:Problem opening the website")
        raise SystemExit(0)
    # extract from site with BeautifulSoup
    try:
        return BeautifulSoup(req.text, "html.parser")
    except Exception:
        print("make_soup:Problem reading from BeautifulSoup")
        raise SystemExit(0)


# crawl for data and output a pandas dataframe
def get_data():

    search_url_list = [haus_url, wohnung_url, rent_url]

    # initialize lists for page data
    city = []
    price = []
    house_size = []
    land_size = []
    rooms = []
    location = []
    commission = []
    listing_url = []

    # initialize a dictionary for the page data lists
    page_data = {}

    for url in search_url_list:

        # initialize list for search page links
        link_list = []
        # split the complete url so we can link easily to multiple pages
        url_parts = url.split('S-T/', 2)
        # get total number of pages of search results
        soup = make_soup(url)

        try:
            number_pages = max([int(n["value"]) for n in soup.find_all("option")])
        # in case of only a single page of results
        except Exception:
            number_pages = 1

        # generate a link for each page of search results
        for i in range(1, number_pages + 1):
            link_list.append(url_parts[0] + 'P-' + str(i) + '/' +
                             url_parts[1])
        link_count = 1

        # get data from each page of results
        for link in link_list:
            try:
                # make BeautifulSoup object for data extraction
                soup = make_soup(link)
                # make a list of listing results
                results = soup.find_all("div", {"class":
                                                "result-list-entry__data"})
            except Exception:
                print("get_data:Problem with BeautifulSoup")
                raise SystemExit(0)
            # print which page is being searched
            print("Crawling: " + link + " (link " + str(link_count) +
                  " of " + str(len(link_list)) + ")")
            link_count += 1

            # step through each listing in the search page, appending data
            for i in range(0, len(results)):

                # add location data to list
                try:
                    city.append(results[i].find("div",
                                                {"class": "result-list-entry__address"})
                                .get_text().strip().split(',', 2)[-1])
                except Exception:
                    city.append(None)

                # add price data to list
                try:
                    price.append(results[i].find_all("dd")[0].get_text().strip())
                except Exception:
                    price.append(None)

                # add house size data to list
                try:
                    house_size.append(results[i].find_all("dd")[1]
                                      .get_text().strip())
                except Exception:
                    house_size.append(None)

                # add land size data to list
                try:
                    land_size.append(results[i].find_all("dd")[3]
                                     .get_text().strip())
                except Exception:
                    land_size.append(None)

                # add number of rooms to list
                try:
                    rooms.append(results[i].find_all("dd")[2].get_text().strip())
                except Exception:
                    rooms.append(None)

                # add location data to list
                try:
                    location.append(results[i].find("div",
                                                    {"class": "result-list-entry__address"})
                                    .get_text().strip())
                except Exception:
                    location.append(None)

                # add commission data to list
                try:
                    commission.append(results[i].find("div", {"class":
                                                              "result-list-entry__secondary-criteria-container"})
                                      .get_text().strip().startswith('Provisions'))
                except Exception:
                    commission.append(None)

                # add url link to list
                try:
                    listing_url.append(url_parts[0][:32] + results[i]
                                       .find('a')['href'])
                except Exception:
                    listing_url.append(None)

    # invert commission values
    commission = [not i for i in commission]

    # fill page data dictionary with lists
    page_data = {'city': city,
                 'price': price,
                 'house_size': house_size,
                 'land_size': land_size,
                 'rooms': rooms,
                 'location': location,
                 'commission': commission,
                 'listing_url': listing_url}

    return page_data


# export raw data
def write_raw(df):

    # set path to raw csv save location
    raw_path = os.path.join(os.getcwd(), "Results", "Raw")

    # make the directory if it doesn't exist
    if not os.path.isdir(raw_path):
        os.makedirs(raw_path)

    # write raw csv
    raw_path_write = os.path.join(raw_path,
                                  "raw_" + current_datetime + ".csv")

    df.to_csv(raw_path_write, sep=";", index=False)


# clean data
def clean_df(df):

    # replace 'NaN' values with 'none'
    df = df.fillna(value='none')

    # delete duplicate entries
    df = df.drop_duplicates()

    # delete unnecesary characters
    df['price'] = [x.strip().replace('€', '') for x in df['price']]
    df['price'] = [x.strip().replace('.', '') for x in df['price']]
    df['house_size'] = [x.strip().replace("m²", "") for x in df['house_size']]
    df['land_size'] = [x.strip().replace("m²", "") for x in df['land_size']]
    df['house_size'] = [x.strip().replace(".", "") for x in df['house_size']]
    df['land_size'] = [x.strip().replace(".", "") for x in df['land_size']]

    # handle half room cases
    df['rooms'] = [x.strip().split('.', 1)[1] for x in df['rooms']]

    return df


# export cleaned data
def write_clean(df):

    # delete old new listings CSV, if exists
    if os.path.isfile(os.getcwd() + '/' + 'New_listings.csv'):
        os.remove(os.getcwd() + '/' + 'New_listings.csv')

    # define path for clean CSV
    clean_path = os.path.join(os.getcwd(), "Results", "Clean")

    # make the path if it doesn't exist
    if not os.path.isdir(clean_path):
        os.makedirs(clean_path)

    # write as clean CSV
    clean_path_write = os.path.join(clean_path,
                                    "clean_" + current_datetime + ".csv")

    df.to_csv(clean_path_write, sep=";", index=False)

    # check if an old current CSV exists
    if os.path.isfile(os.getcwd() + '/' + 'Current_immoscout.csv'):
        # load old current CSV as dataframe
        old_current_df = pd.read_csv(os.getcwd() + '/' + 'Current_immoscout.csv', sep=';')

        # make dataframe showing only new listings
        new_listings_df = pd.concat([df, old_current_df])
        new_listings_df.drop_duplicates(subset='listing_url', keep=False, inplace=True)

        # write new listings CSV, if new listings exist
        if not new_listings_df.empty:
            new_listings_path_write = os.path.join(os.getcwd(), "New_listings.csv")
            new_listings_df.to_csv(new_listings_path_write, sep=';', index=False)
            print('New listings!')
        else:
            print('No new listings')
    # write as current CSV
    current_path_write = os.path.join(os.getcwd(),
                                      "Current_immoscout.csv")

    df.to_csv(current_path_write, sep=";", index=False)


# Driver code
if __name__ == '__main__':
    main()
