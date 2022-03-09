# AD Engine

A docker image for scraping jobs from assorted job sites and returning a CSV of the job data.

This engine currently supports the following job sites:

- Indeed
- Adzuna
- Seek

# Usage

To run the application for **development**, install the required dependencies:

```
pip install -r requirements.txt
```

Then navigate to the `ad_engine` directory and run:

```
uvicorn main:app --reload
```

To deploy the application in **production**, use the provided docker-compose file which will handle the build process for you:

```
docker-compose up -d 
```

We strongly reccomend placing this behind a reverse proxy with SSL. Solutions that connect directly to docker such as `traefik` are preferred but `nginx` would also be appropriate.

# Documentation

Navigating to the base URL will redirect you to the documentation. This gives in depth descriptions of the auto-documented API parameters and context.

You can also access the documentation at the following links:

```
<base_url>/docs
<base_url>/redoc
```