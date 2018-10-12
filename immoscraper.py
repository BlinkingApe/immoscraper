'''
Webscraper for German immo sites

TODO:
    # Use slice instead of declaring two variables for url
    # Update a current market csv from individual crawls
    # add columns 'available' and 'distance_from'
    # split location into 'city' and 'district'
    # rearrange columns
'''

# module imports
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
from shutil import copyfile


# paste complete url from the immoscout search page
complete_url_from_immoscout = 'https://www.immobilienscout24.de/Suche/S-T/Haus-Kauf/Fahrzeitsuche/Duisburg/47058/-219844/2393066/-/-/45/-/-/-/-/-/-/14,15,17,21,24,25,119,122,126/3,4?enteredFrom=result_list'

# split the url for later convinience
url_parts = complete_url_from_immoscout.split('S-T/', 2)

# get current date#
current_datetime = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')


def main():

    # column names for the pandas DataFrame
    columns = ['city', 'price', 'house_size', 'land_size', 'rooms',
               'travel', 'location', 'commission', 'available', 'listing_url']

    # initialize DataFrames with column names
    df_immo = pd.DataFrame(columns=columns)
    #df_previous = pd.DataFrame(pd.read_csv())

    # fill DataFrame with data from web crawler
    df_immo = df_immo.assign(**get_data())

    # write the uncleaned raw data
    #write_raw(df_immo)

    # clean the data
    df_clean = clean_df(df_immo)

    # write the cleaned data
    #write_clean(df_clean)

    # update the current market DataFrame
    #df_update = update_df(df_clean)

    # backup previous version of current csv and overwrite

    #write_update(df_update)


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

    # initialize lists for page data
    city = []
    price = []
    house_size = []
    land_size = []
    rooms = []
    travel = []
    location = []
    commission = []
    listing_url = []
    available = []

    # initialize list for search page links
    link_list = []

    # initialize a dictionary for the page data lists
    page_data = {}

    # get total number of pages of search results
    soup = make_soup(complete_url_from_immoscout)
    number_pages = max([int(n["value"]) for n in soup.find_all("option")])
    
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
            print("Problem with BeautifulSoup")

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

            # add travel time data to list
            try:
                travel.append(results[i].find("div",
                              {"class": "float-left"}).get_text().strip()[:2])
            except Exception:
                travel.append(None)
            
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
                listing_url.append(url_parts[0][:32] + results[i].find('a')['href'])
            except Exception:
                listing_url.append(None)

            # add availablilty data to list
            available.append(True)

    # invert commission values, so that a true value signifies a commission exists
    commission = [not i for i in commission]

    # fill page data dictionary with lists
    page_data = {'city': city,
                 'price': price,
                 'house_size': house_size,
                 'land_size': land_size,
                 'rooms': rooms,
                 'travel': travel,
                 'location': location,
                 'commission': commission,
                 'listing_url': listing_url,
                 'available': available}

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


# update the current market DataFrame and write a copy
def write_update(df):
    update_path = os.path.join(os.getcwd(), "Results", "Current")
    if not os.path.isdir(update_path()):
        os.makedirs(update_path)
    backup_path = os.path.join(update_path, "Backup")

    update_path_write = os.path.join(update_path,
                                     "updated_" + current_datetime + ".csv")
    df.to_csv(update_path_write, sep=";", index=False)


Driver code
#if __name__ == '__main__':
#    main()
