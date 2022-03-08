import asyncio
import aiohttp
from dataclasses import dataclass
from math import ceil
from typing import Callable, Dict, Optional, List, Union
from bs4 import BeautifulSoup
import pandas as pd

from aiohttp import ClientSession

class PageNotFoundException(Exception):
    pass

@dataclass
class Scraper_Engine:
    """Prototype class of the scraper engine which should be extended
    to individual sites.

    Prototype methods:
        verify_page_contents: Checks with exceptions if there are failures
        check_if_no_jobs: Checks if there are no jobs on the page and returns true if so
        get_job_data: Get the job contents
        get_listing_codes: Take a soup and return the listing codes

    """
    api_url: str
    listing_url_template: str
    query_contents: str
    page_query_option: str
    job_number_per_page: int = 50
    max_number_jobs: int = 500
    headers: Optional[str] = None
    client_session: Optional[ClientSession] = None
    modify_query_for_page: Optional[Callable] = None
    verify_page_contents: Optional[Callable] = None
    check_if_no_results: Optional[Callable] = None

    def __post_init__(self):
        """Called after the dataclass init method.
        """
        if not self.headers:
            self.headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.97 Safari/537.36"}
        # Using int will truncate the result, so we will alwasy round down.
        self._max_pages = int(self.max_number_jobs/self.job_number_per_page)

        assert self.get_listing_codes, "You need to define the implementation of self.get_listing_codes"
        assert self.get_job_data, "You need to define the implementation of self.get_job_data"

    async def __aenter__(self):
        if not self.client_session:
            self.client_session = aiohttp.ClientSession(headers=self.headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client_session:
            await self.client_session.close()

    def get_query_uri(self, query_contents: List[str]) -> str:
        """If a query contents is not none, 
        then we add it to the search.
        """
        uri = self.api_url
        for query_tuple in query_contents.items():
            if query_tuple[1] is not None:
                uri += f"{query_tuple[0]}={query_tuple[1]}&"
        return uri
    
    def get_listing_uri(self, listing_code) -> str:
        return self.listing_url_template.format(listing_code=listing_code)

    async def get_number_of_pages(self) -> int:
        response = await self.client_session.get(self.get_query_uri(self.query_contents))
        assert response.status == 200, "Request response code is not 200."
        soup = BeautifulSoup(await response.text(), 'html.parser')

        if response.status == 404:
            raise PageNotFoundException("Can't load the listings page, check the API url")

        if self.verify_page_contents: 
            self.verify_page_contents(response, soup)

        if self.check_if_no_results: 
            if self.check_if_no_results(soup):
                return 0

        n_pages = ceil(self.get_number_jobs(soup) / self.job_number_per_page)

        if n_pages > self._max_pages:
            return self._max_pages
        
        return n_pages
    
    def modify_query_for_page(self, query_contents, page_number, query_option):
        query_contents[query_option] = page_number 
        return query_contents

    async def process_listing_page(self, page_n: Union[str, int], query_option: str) -> List[str]:
        """Process a single job listing page and return 
        a list of job listing codes (listing_code).
        """
        modified_query = self.query_contents.copy()
        modified_query = self.modify_query_for_page(self, modified_query, page_n, query_option)

        response = await self.client_session.get(self.get_query_uri(modified_query))
        soup = BeautifulSoup(await response.text(), "html.parser")

        if self.verify_page_contents: 
            self.verify_page_contents(response, soup)

        return self.get_listing_codes(soup)

    async def process_job_listing(self, listing_code: str) -> Dict[str, str]:
        response = await self.client_session.get(self.get_listing_uri(listing_code))
        soup = BeautifulSoup(await response.text(), "html.parser")

        # Non fatal 404 error
        if response.status == 404:
            return

        if self.verify_page_contents: 
            self.verify_page_contents(response, soup)

        return self.get_job_data(soup, listing_code)

    async def collate_data(self, number_pages: int) -> pd.DataFrame:
        job_listings = await asyncio.gather(*[self.process_listing_page(page_n, self.page_query_option) for page_n in range(1, number_pages+1)])
        
        listing_codes = [l for page_listings in job_listings for l in page_listings if l]
        listing_data = await asyncio.gather(*[self.process_job_listing(listing_code) for listing_code in listing_codes])

        return pd.DataFrame(listing_data)