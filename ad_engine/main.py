import pandas as pd
from pandas.core.frame import DataFrame
import requests
from bs4 import BeautifulSoup
from math import ceil
import re
import asyncio
import aiohttp
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from typing import Optional
import io

# incl_all, incl_exact, incl_at_least_one, excl_words, incl_title, salary_lower, salary_higher, employment_hours, contract_type, results_pp
# https://www.adzuna.com.au/search?adv=1&qwd={incl_all}&qph={incl_exact}&qor={incl_at_least_one}&qxl=excl_words&qtl=in_title&sf=5000&st=140000&cty=permanent&cti=full_time&w=Australia&pp=50&sb=date&sd=down

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

app = FastAPI()

@app.get("/adzuna/incl_title_terms")
async def search_incl_title_terms(terms: str):
    # Generate a new adzuna_engine instance
    adzuna_engine = AdzunaEngine()
    adzuna_engine.query_contents['qtl'] = terms

    # Pagination
    n_pages = adzuna_engine.get_query_pages()

    if n_pages == 0:
        return {"status" : False, 
        "message" : "No jobs found for that query"}

    # Data structures
    listing_list = [] 
    listings_dict = {}

    # Run the queries
    output_df = await adzuna_engine.collate_data(listings_dict, listing_list, n_pages)

    # Return a data stream
    response = StreamingResponse(io.StringIO(output_df.to_csv(index=False)), media_type="text/csv")
    # Edit the headers so its a download
    response.headers["Content-Disposition"] = "attachment; filename=export.csv"

    return response 


