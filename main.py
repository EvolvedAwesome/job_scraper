import pandas as pd
from pandas.core.frame import DataFrame
import requests
from bs4 import BeautifulSoup
from enum import Enum
from math import ceil
import re
import tqdm
import asyncio
import aiohttp

# incl_all, incl_exact, incl_at_least_one, excl_words, incl_title, salary_lower, salary_higher, employment_hours, contract_type, results_pp
# https://www.adzuna.com.au/search?adv=1&qwd={incl_all}&qph={incl_exact}&qor={incl_at_least_one}&qxl=excl_words&qtl=in_title&sf=5000&st=140000&cty=permanent&cti=full_time&w=Australia&pp=50&sb=date&sd=down

query_contents = {
    'adv' : 1, # Enables advanced search options
    'qwd' : None, # Incl all
    'qph' : None, # Incl exact
    'qor' : None, # Incl at least once
    'qxl' : None, # Exclude
    'qtl' : None, # Incl in title
    'sf' : None, # Starting salary (5k jumps)
    'st' : None, # Finishing salary
    'cty' : None, # permanent or contract
    'cti' : None, # part_time or full_time
    'w' : 'Australia', # Location
    'pp' : 50, # Per page
    'sb' : 'date', # Order
    'sd' : 'down', # Asc or desc order
    'page' : 1
}

def search_query(query_dict):
    # If a query contents is not none, then we add it to the search.
    uri = 'https://www.adzuna.com.au/search?'
    for query_tuple in query_dict.items():
        if query_tuple[1] is not None:
            uri += f"{query_tuple[0]}={query_tuple[1]}&"
    return uri

def listing_query(listing_n):
    return f"https://www.adzuna.com.au/details/{listing_n}"

def get_query_soup(uri):
    r = requests.get(uri)
    return BeautifulSoup(r.content, "html.parser")

def get_pandas_parse(uri):
    return pd.read_html(uri)[0]

def get_n_jobs(soup):
    return int(soup.find("div", class_="ui-search-heading").find("span").string)

def get_query_data(query_dict):
    q_uri = search_query(query_dict)
    soup = get_query_soup(q_uri)

    n_pages = ceil(get_n_jobs(soup) / 50)

    get_data_vars = lambda soup: [int(child.attrs.get('data-aid', None)) for child in soup.find("div", class_="ui-search-results").find_all("div", attrs={"data-aid": re.compile(r"\d+")})]

    job_listings = get_data_vars(soup)
    for page_n in range(2, n_pages+1):
        query_dict['page'] = page_n
        q_uri = search_query(query_dict)
        soup = get_query_soup(q_uri)
        job_listings += get_data_vars(soup)

    query_dict['page'] = 1

    return job_listings

def listing_uri_from_code(listing_n):
    return f"https://www.adzuna.com.au/details/{listing_n}"

async def process_listing_data(listing_dict, listing_n):
    async with aiohttp.ClientSession() as session:
        resp = await session.get(listing_uri_from_code(listing_n))
        content = await resp.text()
        soup = BeautifulSoup(content, 'html.parser')

        # Collect the data 
        data_dict = {}
        data_dict['title'] = soup.find('h1').string
        data_dict['description'] = soup.find('section', attrs={'class':'text-sm'}).string

        # Arbitary column data 
        table = soup.find('table')
        for trow in table.find_all('tr'):
            # Some of these are company links
            data_dict[trow.find('th').string.replace(":","").strip()] = trow.find('td').get_text().strip()

        # Add a URL
        data_dict['url'] = listing_uri_from_code(listing_n)
        # Add to column
        listing_dict[listing_n] = data_dict

async def main(listings_dict):
    await asyncio.gather(*[process_listing_data(listings_dict, listing_n) for listing_n in listings_dict.keys()])
    df = pd.DataFrame.from_dict(listings_dict, orient='index')
    df.to_csv('test.csv')

query_contents['qtl'] = "Aboriginal"
#listings = get_query_data(query_contents)

listings = [2733540927, 2733463950, 2732787092, 2732781858, 2732670804, 2732058130, 2732057772, 2732057776, 2732057799, 2732057764, 2732057757, 2732057706, 2731940946, 2731688999, 2729066384, 2729066393, 2728361537, 2728087520, 2727027138, 2727027158, 2727027156, 2727027148, 2726342128, 2726173846, 2726056482, 2725986758, 2725986721, 2725986697, 2725986715, 2725849213, 2724723195, 2723193938, 2723193957, 2723192215, 2722462324, 2722450285, 2722250447, 2722250444, 2722250464, 2722250401, 2722250315, 2722000651, 2721267815, 2720994881]

listings_dict = { l:{} for l in listings }



# class: ui-search-heading -> span
# class: ui-search-results

loop = asyncio.get_event_loop()
loop.run_until_complete(main(listings_dict))
loop.close()