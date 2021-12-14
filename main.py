import pandas as pd
from pandas.core.frame import DataFrame
import requests
from bs4 import BeautifulSoup
from enum import Enum
from math import ceil
import re
import tqdm

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

def get_listing_data(listing_n):
    uri = listing_query(listing_n)
    df = get_pandas_parse(uri)
    df['listing_n'] = listing_n
    return df

def get_listings_df(listing_list):
    df = get_listing_data(listing_list[0])
    for listing in tqdm.tqdm(listing_list[1:]):
        df2 = get_listing_data(listing)
        df = df.append(df2).fillna("")

    df.reset_index(inplace=True)
    #df.set_index('listing_n')
    df = df.pivot(index='listing_n', columns=0, values=1).fillna("")
    return df

query_contents['qtl'] = "Aboriginal"
#listings = get_query_data(query_contents)

#print(get_listing_data(2733540927))
df = get_listings_df([2733540927, 2733463950, 2732787092, 2732781858, 2732670804, 2732058130, 2732057772, 2732057776, 2732057799, 2732057764, 2732057757, 2732057706, 2731940946, 2731688999, 2729066384, 2729066393, 2728361537, 2728087520, 2727027138, 2727027158, 2727027156, 2727027148, 2726342128, 2726173846, 2726056482, 2725986758, 2725986721, 2725986697, 2725986715, 2725849213, 2724723195, 2723193938, 2723193957, 2723192215, 2722462324, 2722450285, 2722250447, 2722250444, 2722250464, 2722250401, 2722250315, 2722000651, 2721267815, 2720994881])# 2719912139, 2719912156, 2719912150, 2719912136, 2719720516, 2719719425, 2718577353, 2718577378, 2718577416, 2718577357, 2718577399, 2718577294, 2718577229, 2718496538, 2718393064, 2717566616, 2717521001, 2716536654, 2716242737, 2716241597, 2716241218, 2715923020, 2715413094, 2715312024, 2715197465, 2715197515, 2715197312, 2715197102, 2715196174, 2715040742, 2714597093, 2714351758, 2714267882, 2713284105, 2713284101, 2713284107, 2713239737, 2713239706, 2713238609, 2712987490, 2712985724, 2711692939, 2710921256, 2710910177, 2710029488, 2708006352, 2707271433, 2706612028, 2704760626, 2703569352, 2703569347, 2703569321, 2703495368, 2703495370, 2703451327, 2702926918, 2702194763, 2702194559, 2702194596, 2702194453, 2702191227, 2702187223, 2702186986, 2702183315, 2702183100, 2702182808, 2702181960, 2702179941, 2702171723, 2700721443, 2700403213, 2700171223, 2698865986, 2698412535, 2698411300, 2696044422, 2695231590, 2695231476, 2695089303, 2695051561, 2694956105, 2694956151, 2694956075, 2694955932, 2692812856, 2692315381, 2692044907, 2692044833, 2692044818, 2692044855, 2692044882, 2691895071, 2691607333, 2691522895, 2689728003, 2689728009, 2689727894, 2689613763, 2688814865, 2687261715, 2686556093, 2685595575, 2683039833, 2683005737, 2683005707, 2680843401, 2680568048, 2679076756, 2678992945, 2678095272, 2677521517, 2674833736, 2674564990, 2674564724, 2674207269, 2673915570, 2671347994, 2670751353, 2670661873, 2670661422, 2670306773, 2665695018, 2661749250, 2661581759, 2659823758, 2655610715, 2655033999, 2654293687, 2653475747, 2653405718, 2651155825, 2650806500, 2649553316, 2647769902, 2647300460, 2639144549, 2634953875, 2632807430, 2632193202, 2625104518, 2623221375, 2623148672, 2622358879, 2612892988, 2602493103, 2602316529, 2598678583, 2598678475, 2598361666, 2584253107, 2577940884, 2575748188, 2556440912, 2556422230, 2541210502, 2536001052, 2521255777, 2503035372, 2496918180, 2496918142, 2496918120, 2463002407, 2455579313, 2424966471, 2412737595, 2344117412, 2293388172, 2281884456, 2281884411, 2281884406, 2177340960, 2146291006, 2075394982, 1947708424, 1613931035, 1311747937, 778187304]))
url_from_n = lambda n: f"https://www.adzuna.com.au/details/{n}"
df['url'] = df.index.map(url_from_n)
df.to_csv('data.csv')
# class: ui-search-heading -> span
# class: ui-search-results