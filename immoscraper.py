'''
Webscraper for German immo sites

TODO:
    # Use slice instead of declaring two variables for url
    # Update a current market csv from individual crawls
    # add columns 'available' and 'distance_from'
    # rearrange columns
'''

# module imports

import requests
from bs4 import BeautifulSoup
from pandas import DataFrame
from datetime import datetime
import os

# define top level url
domain = 'https://www.immobilienscout24.de/Suche/S-T/'

# define search specific url
search_url = 'Haus-Kauf/Fahrzeitsuche/Duisburg/47058/-219844/2393066/-/-/45/-/-/-/-/-/-/14,15,17,21,24,25,119,122,126/3,4?enteredFrom=result_list'

# get current date#
current_datetime = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')


def main():

    # column names for the pandas DataFrame
    columns = ['commission', 'location',
               'price', 'house_size',
               'land_size', 'rooms',
               'listing_url']

    # initialize DataFrame with column names
    df_immo = DataFrame(columns=columns)

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
        print("Fehler beim Oeffnen der Website")

    # extract from site with BeautifulSoup
    try:
        return BeautifulSoup(req.text, "lxml")
    except Exception:
        print("Fehler beim Einlesen in BeautifulSoup")


# crawl for data and output a pandas dataframe
def get_data():

    # initialize
    link_list = []
    commission = []
    location = []
    price = []
    house_size = []
    land_size = []
    rooms = []
    listing_url = []
    page_data = {}

    # get total number of pages of search results
    try:
        soup = make_soup(domain + search_url)
        number_pages = max([int(n["value"]) for n
                            in soup.find_all("option")])
    except Exception:
        print("Problem with BeautifulSoup")

    # generate a link for each page of search results
    for i in range(1, number_pages + 1):
        link_list.append(domain + 'P-' + str(i) + '/' +
                         search_url)
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
            print("Problem with BeautifulSoup")

        print("Crawling: " + link + " (link " + str(link_count) +
              " of " + str(len(link_list)) + ")")
        link_count += 1
        # step through each listing in the search page, appending data
        for i in range(0, len(results)):
            try:
                commission.append(results[i].find("div",
                                  {"class": "result-list-entry__secondary-criteria-container"})
                                  .get_text().strip().startswith('Provisions'))
            except Exception:
                commission.append(None)
            try:
                location.append(results[i].find("div",
                                {"class": "result-list-entry__address"})
                                .get_text().strip())
            except Exception:
                location.append(None)
            try:
                price.append(results[i].find_all("dd")[0].get_text().strip())
            except Exception:
                price.append(None)
            try:
                house_size.append(results[i].find_all("dd")[1]
                                  .get_text().strip())
            except Exception:
                house_size.append(None)
            try:
                land_size.append(results[i].find_all("dd")[3]
                                 .get_text().strip())
            except Exception:
                land_size.append(None)
            try:
                rooms.append(results[i].find_all("dd")[2].get_text().strip())
            except Exception:
                rooms.append(None)
            try:
                listing_url.append(domain[:32] + results[i].find('a')['href'])
            except Exception:
                listing_url.append(None)

    commission = [not i for i in commission]

    page_data = {"commission": commission,
                 "location": location,
                 "price": price,
                 "house_size": house_size,
                 "land_size": land_size,
                 "rooms": rooms,
                 "listing_url": listing_url}

    return page_data


# export raw data
def write_raw(df):

    raw_path = os.path.join(os.getcwd(), "Results", "Raw")
    if not os.path.isdir(raw_path):
        os.makedirs(raw_path)

    raw_path_write = os.path.join(raw_path,
                                  "raw_" + current_datetime + ".csv")

    df.to_csv(raw_path_write, sep=";", index=False)


# clean data
def clean_df(df):

    df = df.dropna(axis=0)
    df = df.drop_duplicates()
    df['price'] = [x.strip().replace('€', '') for x in df['price']]
    df['price'] = [x.strip().replace('.', '') for x in df['price']]
    df['house_size'] = [x.strip().replace("m²", "") for x in df['house_size']]
    df['land_size'] = [x.strip().replace("m²", "") for x in df['land_size']]
    df['house_size'] = [x.strip().replace(".", "") for x in df['house_size']]
    df['land_size'] = [x.strip().replace(".", "") for x in df['land_size']]
    df['rooms'] = [x.strip().split('.', 1)[1] for x in df['rooms']]

    return df


# export cleaned data
def write_clean(df):
    clean_path = os.path.join(os.getcwd(), "Results", "Clean")
    if not os.path.isdir(clean_path):
        os.makedirs(clean_path)

    clean_path_write = os.path.join(clean_path,
                                    "clean_" + current_datetime + ".csv")

    df.to_csv(clean_path_write, sep=";", index=False)


# Driver code
if __name__ == '__main__':
    main()
