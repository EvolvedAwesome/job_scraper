from attr import attr
import pandas as pd
from pandas.core.frame import DataFrame
import requests
from bs4 import BeautifulSoup
from math import ceil
import re
import asyncio
import aiohttp

# Define an exception for a captcha appearing
class CaptchaException(Exception):
    pass

PAGE_UPPER_LIMIT = 100
# jobs?as_and=dvd&as_phr&as_any&as_not&as_ttl&as_cmp&jt=all&st&salary&radius=50&l&fromage=any&limit=10&sort&psf=advsrch&from=advancedsearch&

class IndeedEngine:
    def __init__(self):
        # Test
        self.pages_re = re.compile("^Page ([0-9]*) of ([0-9]*) jobs")
        self.searchq = "Aboriginal Politics"
        self.headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.97 Safari/537.36"}

    def get_query_string(self):
        return f"https://au.indeed.com/jobs?q={self.searchq}"

    def sync_query_indeed(self):
        return requests.get(self.get_query_string())

    def get_query_soup(self):
        content = self.sync_query_indeed().text
        return BeautifulSoup(content, 'html.parser')

    def get_n_jobs(self, soup):
        print(soup)
        pages_text = soup.find('div', id="searchCountPages").get_text().replace(",", "").strip()
        return int(self.pages_re.match(pages_text).group(2))

    def generate_n_page_uri(self, page_n):
        # This indeed CMS seems to start at pg1 -> page=0, pg2 -> page=10, pg3 -> page=20
        # So page = (page_n - 1)*10
        base_uri = self.get_query_string()
        return base_uri + f"&start={int((page_n-1)*10)}"

    def get_page_job_ids(self, soup):
        jobs = soup.find_all('a', id=re.compile('^job_'))
        return [j['data-jk'] for j in jobs]

    def get_query_pages(self):
        soup = self.get_query_soup()

        if "did not match any jobs" in soup.get_text():
            return 0
        elif "hCaptcha" in soup.get_text():
            raise CaptchaException("hCaptcha block present on page.")

        n_pages = ceil(self.get_n_jobs(soup) / 10)
        
        if n_pages > PAGE_UPPER_LIMIT: # 1000 records upper limit
            return PAGE_UPPER_LIMIT 

        return n_pages

    async def process_ad_pages(self, listing_list, page_n):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            resp = await session.get(self.generate_n_page_uri(page_n))
            content = await resp.text()
            soup = BeautifulSoup(content, 'html.parser')

            listing_list += self.get_page_job_ids(soup) 

    def listing_uri_from_code(self, listing_code):
        return f"https://au.indeed.com/viewjob?jk={listing_code}"

    async def process_listing_data(self, listing_dict, listing_code):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            resp = await session.get(self.listing_uri_from_code(listing_code))
            content = await resp.text()
            soup = BeautifulSoup(content, 'html.parser')

            # Collect the data 
            data_dict = {}
            data_dict['title'] = soup.find('h1', attrs={'class':'jobsearch-JobInfoHeader-title'}).string
            data_dict['description'] = soup.find('div', id="jobDescriptionText").get_text()
            #data_dict['company'] = soup.find('a', attrs={'href', re.compile("^https://au\.indeed\.com/cmp/.*")}).get_text()

            # Add a URL
            data_dict['url'] = self.listing_uri_from_code(listing_code)
            # Add to column
            listing_dict[listing_code] = data_dict

    async def collate_data(self, listings_dict, listing_list, n_pages):
        await asyncio.gather(*[self.process_ad_pages(listing_list, n) for n in range(1, n_pages+1)])
        
        listings_dict = { l:{} for l in listing_list }
        await asyncio.gather(*[self.process_listing_data(listings_dict, listing_n) for listing_n in listings_dict.keys()])
        
        df = pd.DataFrame.from_dict(listings_dict, orient='index')
        return df

if __name__ == "__main__":
    ie = IndeedEngine()

    listings_dict = {}
    listing_list = []
    n_pages = ie.get_query_pages()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(ie.collate_data(listings_dict, listing_list, n_pages))
    loop.close()