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

PAGE_UPPER_LIMIT = 10 
# jobs?as_and=dvd&as_phr&as_any&as_not&as_ttl&as_cmp&jt=all&st&salary&radius=50&l&fromage=any&limit=10&sort&psf=advsrch&from=advancedsearch&

#jobs?as_and=all_these&as_phr=exact_this&as_any=at_least_one&as_not=none_of&as_ttl=title_search&as_cmp=from_company&jt=fulltime&st=&salary=&radius=50&l=&fromage=any&limit=50&sort=&psf=advsrch&from=advancedsearch

class IndeedEngine:
    def __init__(self):
        # Test
        self.pages_re = re.compile("^Page ([0-9]*) of ([0-9]*) jobs")
        self.headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.97 Safari/537.36"}
        self.query_contents = {
            'psf':'advsrch', # Specify an advanced search
            'from':'advancedsearch', # Specify an advanced search
            'as_and': None, # all of these terms
            'as_phr': None, # exactly these terms
            'as_any': None, # at least once of these
            'as_not': None, # none of these
            'as_ttl': None, # title search
            'as_cmp': None, # From company
            'jt': None, # "JobType" - fulltime, parttime, casual, contract
            'limit': 50, # Max page limit
            'start': 0, # n to start reading jobs from
        }

    def get_query_string(self, query_contents):
        uri = "https://au.indeed.com/jobs?"
        for query_tuple in query_contents.items():
            if query_tuple[1] is not None:
                uri += f"{query_tuple[0]}={query_tuple[1]}&"
        return uri

    def sync_query_indeed(self):
        return requests.get(self.get_query_string(self.query_contents))

    def get_query_soup(self):
        content = self.sync_query_indeed().text
        return BeautifulSoup(content, 'html.parser')

    def get_n_jobs(self, soup):
        pages_text = soup.find('div', id="searchCountPages").get_text().replace(",", "").strip()
        return int(self.pages_re.match(pages_text).group(2))

    def generate_n_page_uri(self, page_n):
        self.query_contents['start'] = int(self.query_contents['limit'])*page_n
        return self.get_query_string(self.query_contents)

    def get_page_job_ids(self, soup):
        jobs = soup.find_all('a', id=re.compile('^job_'))
        return [j['data-jk'] for j in jobs]

    def get_query_pages(self):
        soup = self.get_query_soup()

        if "did not match any jobs" in soup.get_text():
            return 0
        elif "hCaptcha" in soup.get_text():
            raise CaptchaException("hCaptcha block present on page.")

        n_pages = ceil(self.get_n_jobs(soup) / int(self.query_contents['limit']))

        if n_pages > PAGE_UPPER_LIMIT: # 1000 records upper limit
            return PAGE_UPPER_LIMIT

        return n_pages

    async def process_ad_pages(self, listing_list, page_n):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            resp = await session.get(self.generate_n_page_uri(page_n))
            content = await resp.text()
            soup = BeautifulSoup(content, 'html.parser')

            if "hCaptcha" in soup.get_text():
                raise CaptchaException("hCaptcha block present on page.")

            listing_list += self.get_page_job_ids(soup)

    def listing_uri_from_code(self, listing_code):
        return f"https://au.indeed.com/viewjob?jk={listing_code}"

    async def process_listing_data(self, listing_dict, listing_code):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            resp = await session.get(self.listing_uri_from_code(listing_code))
            content = await resp.text()
            soup = BeautifulSoup(content, 'html.parser')
        
            # Run the hcaptcha check every page we access
            if "hCaptcha" in soup.get_text():
                raise CaptchaException("hCaptcha block present on page.")

            if "404" in soup.get_text():
                return

            # Collect the data
            data_dict = {}

            data_dict['title'] = soup.find('h1', attrs={'class':'jobsearch-JobInfoHeader-title'}).string

            data_dict['description'] = soup.find('div', id="jobDescriptionText").get_text()

            # Employer information (name + location)
            r = soup.find('div', class_='jobsearch-CompanyInfoContainer')
            if r is not None:
                data_dict['employer'] = s.text if (s := r.find("a")) is not None else r.find('div', 'jobsearch-InlineCompanyRating').text
                data_dict['location'] = ' '.join([f.text for f in r.find('div', 'jobsearch-JobInfoHeader-subtitle').find_all('div', attrs={'class': None})])
            else:
                data_dict['employer'] = None
                data_dict['location'] = None
            
            # Position details
            data_dict['employment_type'] = ""
            data_dict['salary'] = ""

            if (r := soup.find('div', class_='jobsearch-JobMetadataHeader-item')) is not None:
                pdetails = r.find_all('span')

                if len(pdetails) == 1:
                    content = pdetails[0].text
                    if "$" in content: 
                        data_dict['salary'] = pdetails[0].text
                    else:
                        data_dict['employment_type'] = pdetails[0].text
                elif len(pdetails) == 2:
                    data_dict['employment_type'] = pdetails[1].text.replace("-",'').strip()
                    data_dict['salary'] = pdetails[0].text

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
    ie.query_contents['as_ttl'] = "Aboriginal"

    listings_dict = {}
    listing_list = []
    n_pages = ie.get_query_pages()

    loop = asyncio.get_event_loop()
    df = loop.run_until_complete(ie.collate_data(listings_dict, listing_list, n_pages))

    # Output to CSV for testing/development
    print(df)
    df.to_csv("docco.csv")

    loop.close()
