import re
import asyncio

from engine import Scraper_Engine

# Define an exception for a captcha appearing
class CaptchaException(Exception):
    pass

# jobs?as_and=dvd&as_phr&as_any&as_not&as_ttl&as_cmp&jt=all&st&salary&radius=50&l&fromage=any&limit=10&sort&psf=advsrch&from=advancedsearch&

# jobs?as_and=all_these&as_phr=exact_this&as_any=at_least_one&as_not=none_of&as_ttl=title_search&as_cmp=from_company&jt=fulltime&st=&salary=&radius=50&l=&fromage=any&limit=50&sort=&psf=advsrch&from=advancedsearch

class IndeedEngine(Scraper_Engine):
    def __init__(self):
        # Test
        self.pages_re = re.compile("^Page ([0-9]*) of ([0-9]*) jobs")
        self.headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.97 Safari/537.36"}
        self.api_url = "https://au.indeed.com/jobs?"
        self.listing_url_template = "https://au.indeed.com/viewjob?jk={listing_code}"
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
        self.page_query_option = "start"
        self.__post_init__()

    def get_number_jobs(self, soup):
        pages_text = soup.find('div', id="searchCountPages").get_text().replace(",", "").strip()
        return int(self.pages_re.match(pages_text).group(2))

    def modify_query_for_page(self, query_contents, page_number, query_option):
        query_contents[query_option] = int(self.query_contents['limit'])*page_number
        return query_contents

    def verify_page_contents(self, response, soup):
        if "hCaptcha" in soup.get_text():
            raise CaptchaException("hCaptcha block present on page.")
    
    def check_if_no_jobs(self, soup):
        if "did not match any jobs" in soup.get_text():
            return True 

    def get_listing_codes(self, soup):
        jobs = soup.find_all('a', id=re.compile('^job_'))
        return [j['data-jk'] for j in jobs]

    def get_job_data(self, soup, listing_code):
        # Collect the data
        data_dict = {}

        title = soup.find('h1', attrs={'class':'jobsearch-JobInfoHeader-title'})
        if title is not None:
            data_dict['title'] = title.string
        else:
            data_dict['title'] = ''

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
        data_dict['url'] = self.get_listing_uri(listing_code)
        return data_dict

async def __test_main__():
    async with IndeedEngine() as ie:
        ie.query_contents['as_ttl'] = "Aboriginal"
        number_pages = await ie.get_number_of_pages()
        df = await ie.collate_data(number_pages)
        print(df)

if __name__ == "__main__":
    asyncio.run(__test_main__())