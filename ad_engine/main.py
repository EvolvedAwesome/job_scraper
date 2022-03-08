from fastapi import FastAPI
from fastapi.responses import StreamingResponse, RedirectResponse
from typing import Optional, Literal, Union
import io
from datetime import date

from job_engine import AdzunaEngine, IndeedEngine, CaptchaException

app = FastAPI(
    title="Job board scraper",
    description="An API for scraping jobs from the Indeed and Adzuna job boards and exporting their info in CSV format",
    version="1.1.0",
    contact={
        "name": "Folded Studio",
        "email": "office@folded.co.nz",
    }
)

@app.get("/", include_in_schema=False)
async def docs_redirect():
    return RedirectResponse(url='/docs')

@app.get("/search")
async def job_search(
        job_board: Literal['Adzuna', 'Indeed'] = 'Adzuna',
        search_just_title_or_title_and_description: Literal['just_title', 'title_and_description'] =  'just_title',
        search_terms: str = 'Aboriginal Politics',
        results_must_include_every_term: Literal['true', 'false'] = 'false'
):
    if job_board == 'Adzuna':
        # Generate a new adzuna_engine instance
        adzuna_engine = AdzunaEngine()

        if search_just_title_or_title_and_description == 'just_title':
            # This is where we define how the search search_terms are matched to the jobs in the adzuna database
            adzuna_engine.query_contents['qtl'] = search_terms
        else:
            # This is where we define how the search search_terms are matched to the jobs in the adzuna database
            if results_must_include_every_term == 'true':
                # This is where we define how the search search_terms are matched to the jobs in the adzuna database
                adzuna_engine.query_contents['qph'] = search_terms
            else:
                # This is where we define how the search search_terms are matched to the jobs in the adzuna database
                adzuna_engine.query_contents['qor'] = search_terms

        # Pagination
        n_pages = await adzuna_engine.get_number_of_pages()

        # No captcha systems to speak of here.
        if n_pages == 0:
            return {
                "status" : False,
                "message" : "No jobs found for that query"
            }

        # Run the queries
        output_df = await adzuna_engine.collate_data(n_pages)

    elif job_board == 'Indeed':
        # Generate a new adzuna_engine instance
        indeed_engine = IndeedEngine()

        if search_just_title_or_title_and_description == 'just_title':
            # This is where we define how the search search_terms are matched to the jobs in the adzuna database
            indeed_engine.query_contents['as_ttl'] = search_terms
        else:
            # This is where we define how the search search_terms are matched to the jobs in the adzuna database
            if results_must_include_every_term == 'true':
                # This is where we define how the search search_terms are matched to the jobs in the adzuna database
                indeed_engine.query_contents['as_and'] = search_terms
            else:
                # This is where we define how the search search_terms are matched to the jobs in the adzuna database
                indeed_engine.query_contents['as_any'] = search_terms


        # Pagination
        try:
            n_pages = await indeed_engine.get_number_of_pages()
        except CaptchaException as e:
            return {"status" : False,
                "message" : str(e)}

        if n_pages == 0:
            return {"status" : False,
                "message" : "No jobs found for that query"}

        # Run the queries
        output_df = await indeed_engine.collate_data(n_pages)

    # Return a data stream
    response = StreamingResponse(io.StringIO(output_df.to_csv(index=False)), media_type="text/csv")
    # Edit the headers so its a download
    response.headers["Content-Disposition"] = "attachment; filename=export_jobs_" + str(date.today().strftime('%Y-%m-%d')) + ".csv"

    return response