from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from typing import Optional, Literal, Union
import io
from datetime import date

from adzuna_engine import AdzunaEngine
from indeed_engine import IndeedEngine
from indeed_engine import CaptchaException

app = FastAPI()

@app.get("/export")
async def adz_search_incl_title_terms(
        site: Literal['adzuna', 'indeed'] = 'adzuna',
        focus: Literal['title', 'all'] =  'title',
        terms: str = 'Aboriginal Politics',
        exact: Literal['true', 'false'] = 'false'
):
    if site == 'adzuna':
        # Generate a new adzuna_engine instance
        adzuna_engine = AdzunaEngine()

        if focus == 'title':
            # This is where we define how the search terms are matched to the jobs in the adzuna database
            adzuna_engine.query_contents['qtl'] = terms
        else:
            # This is where we define how the search terms are matched to the jobs in the adzuna database
            if exact == 'true':
                # This is where we define how the search terms are matched to the jobs in the adzuna database
                adzuna_engine.query_contents['qph'] = terms
            else:
                # This is where we define how the search terms are matched to the jobs in the adzuna database
                adzuna_engine.query_contents['qor'] = terms

        # Pagination
        n_pages = adzuna_engine.get_query_pages()

        # No captcha systems to speak of here.
        if n_pages == 0:
            return {
                "status" : False,
                "message" : "No jobs found for that query"
            }

        # Data structures
        listing_list = []
        listings_dict = {}

        # Run the queries
        output_df = await adzuna_engine.collate_data(listings_dict, listing_list, n_pages)

    if site == 'indeed':
        # Generate a new adzuna_engine instance
        indeed_engine = IndeedEngine()

        if focus == 'title':
            # This is where we define how the search terms are matched to the jobs in the adzuna database
            indeed_engine.query_contents['as_ttl'] = terms
        else:
            # This is where we define how the search terms are matched to the jobs in the adzuna database
            if exact == 'true':
                # This is where we define how the search terms are matched to the jobs in the adzuna database
                indeed_engine.query_contents['as_and'] = terms
            else:
                # This is where we define how the search terms are matched to the jobs in the adzuna database
                indeed_engine.query_contents['as_any'] = terms


        # Pagination
        try:
            n_pages = indeed_engine.get_query_pages()
        except CaptchaException as e:
            return {"status" : False,
                "message" : str(e)}

        if n_pages == 0:
            return {"status" : False,
                "message" : "No jobs found for that query"}

        # Data structures
        listing_list = []
        listings_dict = {}

        # Run the queries
        output_df = await indeed_engine.collate_data(listings_dict, listing_list, n_pages)

    # Return a data stream
    response = StreamingResponse(io.StringIO(output_df.to_csv(index=False)), media_type="text/csv")
    # Edit the headers so its a download
    response.headers["Content-Disposition"] = "attachment; filename=export_jobs_" + str(date.today().strftime('%Y-%m-%d')) + ".csv"

    return response