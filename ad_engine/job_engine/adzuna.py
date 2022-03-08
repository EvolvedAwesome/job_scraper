import asyncio
from typing import Dict, List, Union
from bs4 import BeautifulSoup
import re

from engine import Scraper_Engine

# incl_all, incl_exact, incl_at_least_one, excl_words, incl_title, salary_lower, salary_higher, employment_hours, contract_type, results_pp
# https://www.adzuna.com.au/search?adv=1&qwd={incl_all}&qph={incl_exact}&qor={incl_at_least_one}&qxl=excl_words&qtl=in_title&sf=5000&st=140000&cty=permanent&cti=full_time&w=Australia&pp=50&sb=date&sd=down

class AdzunaEngine(Scraper_Engine):
    def __init__(self):
        self.api_url = 'https://www.adzuna.com.au/search?'
        self.listing_url_template = "https://www.adzuna.com.au/details/{listing_code}"
        self.job_number_per_page = 50
        self.max_number_jobs = 500
        self.page_query_option = "page"
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
            'pp' : int(self.job_number_per_page), # Per page
            'sb' : 'date', # Order
            'sd' : 'down', # Asc or desc order
            'page' : 1
        }
        # Make sure we call the post init method
        self.__post_init__()

    def get_number_jobs(self, soup: BeautifulSoup) -> int:
        return int(float(soup.find("div", class_="ui-search-heading").find("span").string.replace(',','')))

    def check_if_no_results(self, soup) -> bool:
        if "No results found" in soup.get_text():
            return True 
        return False
    
    def get_listing_codes(self, soup: BeautifulSoup) -> List[str]:
        return [int(child.attrs.get('data-aid', None)) for child in soup.find("div", class_="ui-search-results").find_all("div", attrs={"data-aid": re.compile(r"\d+")})]

    def get_job_data(self, soup: BeautifulSoup, listing_code: Union[str, int]) -> Dict[str, str]:
        data_dict = {}
        data_dict['title'] = soup.find('h1').string
        try:
            data_dict['description'] = soup.find('section', class_='text-sm').text
        except AttributeError as e:
            print(AttributeError, soup)
            raise AttributeError

        # Arbitary column data 
        table = soup.find('table')
        for trow in table.find_all('tr'):
            # Some of these are company links
            data_dict[trow.find('th').string.replace(":","").strip()] = trow.find('td').get_text().strip()

        # Add a URL
        data_dict['url'] = self.get_listing_uri(listing_code)
        return data_dict

async def __test_main():
    async with AdzunaEngine() as ae:
        ae.query_contents['qtl'] = "Aboriginal"
        number_pages = await ae.get_number_of_pages()
        df = await ae.collate_data(number_pages)
        print(df)

if __name__ == "__main__":
    asyncio.run(__test_main())