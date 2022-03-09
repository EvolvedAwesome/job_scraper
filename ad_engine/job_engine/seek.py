import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional, Union
from bs4 import BeautifulSoup
import re
import json 

from job_engine.engine import Scraper_Engine

@dataclass
class JobData:
    id: str
    listingDate: str
    title: str
    short_description: str
    bulletpoints: List[str]
    company: str
    location: str
    area: str
    contract_type: str
    category: str
    subcategory: str
    salary: str
    description: Optional[str] = None
    url: Optional[str] = None

    def as_dict(self):
        """Return a dataframe happy version of the object.
        """
        return {
            "listingDate": self.listingDate,
            "title": self.title,
            "short_description": self.short_description,
            "bulletpoints": self.bulletpoints,
            "company": self.company,
            "location": self.location,
            "area": self.area,
            "contract_type": self.contract_type,
            "category": self.category,
            "subcategory": self.subcategory,
            "salary": self.salary,
            "description": self.description,
            "url": self.url,
        }


class SeekEngine(Scraper_Engine):
    def __init__(self, search_term):
        self.api_url = 'https://www.seek.com.au/{search_term}-jobs?'
        self.listing_url_template = "https://www.seek.com.au/job/{listing_code}"
        self.job_number_per_page = 22 
        self.max_number_jobs = 200 
        self.page_query_option = "page"
        self.search_term = search_term
        self.query_contents = {
            'page' : 1
        }
        self.listing_href_regex = re.compile("\/job\/([0-9]+)")
        self.listing_data = {} 
        self.find_data_automation_tag = lambda soup, tag: soup.find('div', attrs={"data-automation": str(tag)})
        self.find_data_automation_script = lambda soup, tag: soup.find('script', attrs={"data-automation": str(tag)})
        # Make sure we call the post init method
        self.__post_init__()
    
    def get_query_uri(self, query_contents: List[str]) -> str:
        """If a query contents is not none, 
        then we add it to the search.
        """
        uri = self.api_url.format(search_term=self.search_term.replace(" ", "-"))
        for query_tuple in query_contents.items():
            if query_tuple[1] is not None:
                uri += f"{query_tuple[0]}={query_tuple[1]}&"
        return uri

    def get_number_jobs(self, soup: BeautifulSoup) -> int:
        return int(float(soup.find("strong", attrs={'data-automation': 'totalJobsCount'}).string.replace(',','')))

    def check_if_no_results(self, soup) -> bool:
        if "Sorry, we couldn't find anything." in soup.get_text():
            return True 
        return False

    def get_listing_codes(self, soup: BeautifulSoup) -> List[str]:
        listing_string = ''.join([line.strip().replace("window.SEEK_REDUX_DATA = ", "").replace("undefined","null")[:-1]
            for line in self.find_data_automation_script(soup, "server-state").string.split("\n") 
            if "SEEK_REDUX_DATA" in line])
        json_data = json.loads(listing_string) 
        jobs_list = json_data['results']['results']['jobs']
        self.listing_data.update({
            job['id'] : JobData( 
                id = job['id'],
                listingDate = job['listingDate'],
                title = job['title'],
                short_description = job['teaser'],
                bulletpoints = ' '.join(job['bulletPoints']),
                company = job['advertiser']['description'],
                location = job['location'],
                area = job['area'],
                contract_type = job['workType'],
                category = job['classification']['description'],
                subcategory = job['subClassification']['description'],
                salary = job['salary']) for job in jobs_list})

        return [job.id for job in self.listing_data.values()]

    def get_job_data(self, soup: BeautifulSoup, listing_code: Union[str, int]) -> Dict[str, str]:
        self.listing_data[listing_code].description = self.find_data_automation_tag(soup, 'jobAdDetails').get_text().strip()
        self.listing_data[listing_code].url = self.get_listing_uri(listing_code)

        return self.listing_data[listing_code].as_dict()

async def __test_main():
    async with SeekEngine("aboriginal") as se:
        se.query_contents['qtl'] = "Aboriginal"
        number_pages = await se.get_number_of_pages()
        df = await se.collate_data(number_pages)
        df.to_csv("test_output.csv")

if __name__ == "__main__":
    asyncio.run(__test_main())