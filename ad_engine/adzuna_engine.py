import pandas as pd
from pandas.core.frame import DataFrame
import requests
from bs4 import BeautifulSoup
from math import ceil
import re
import asyncio
import aiohttp

class AdzunaEngine:
    def __init__(self):
        self.query_contents = {
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

    def search_query(self, query_contents):
        # If a query contents is not none, then we add it to the search.
        uri = 'https://www.adzuna.com.au/search?'
        for query_tuple in query_contents.items():
            if query_tuple[1] is not None:
                uri += f"{query_tuple[0]}={query_tuple[1]}&"
        return uri

    def get_query_soup(self, uri):
        r = requests.get(uri)
        return BeautifulSoup(r.content, "html.parser")

    def get_n_jobs(self, soup):
        return int(soup.find("div", class_="ui-search-heading").find("span").string)

    def get_query_pages(self):
        q_uri = self.search_query(self.query_contents)
        soup = self.get_query_soup(q_uri)

        if "No results found" in soup.get_text():
            return 0

        n_pages = ceil(self.get_n_jobs(soup) / 50)
        if n_pages > 20: # 1000 records upper limit
            return 20

        return n_pages

    async def process_ad_pages(self, listing_list, page_n):
        async with aiohttp.ClientSession() as session:
            this_query = self.query_contents.copy()
            this_query['page'] = page_n
            resp = await session.get(self.search_query(this_query))
            content = await resp.text()
            soup = BeautifulSoup(content, 'html.parser')

            get_data_vars = lambda soup: [int(child.attrs.get('data-aid', None)) for child in soup.find("div", class_="ui-search-results").find_all("div", attrs={"data-aid": re.compile(r"\d+")})]
            listing_list += get_data_vars(soup)

    def listing_uri_from_code(self, listing_n):
        return f"https://www.adzuna.com.au/details/{listing_n}"

    async def process_listing_data(self, listing_dict, listing_n):
        async with aiohttp.ClientSession() as session:
            resp = await session.get(self.listing_uri_from_code(listing_n))
            content = await resp.text()
            soup = BeautifulSoup(content, 'html.parser')

            # Collect the data 
            data_dict = {}
            data_dict['title'] = soup.find('h1').string
            data_dict['description'] = soup.find('section', attrs={'class':'text-sm'}).get_text()

            # Arbitary column data 
            table = soup.find('table')
            for trow in table.find_all('tr'):
                # Some of these are company links
                data_dict[trow.find('th').string.replace(":","").strip()] = trow.find('td').get_text().strip()

            # Add a URL
            data_dict['url'] = self.listing_uri_from_code(listing_n)
            # Add to column
            listing_dict[listing_n] = data_dict

    async def collate_data(self, listings_dict, listing_list, n_pages):
        await asyncio.gather(*[self.process_ad_pages(listing_list, n) for n in range(1, n_pages+1)])
        
        listings_dict = { l:{} for l in listing_list }
        await asyncio.gather(*[self.process_listing_data(listings_dict, listing_n) for listing_n in listings_dict.keys()])
        
        df = pd.DataFrame.from_dict(listings_dict, orient='index')
        return df