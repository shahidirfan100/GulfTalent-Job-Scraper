# GulfTalent Jobs Scraper

This Apify actor scrapes job listings from GulfTalent using HTTP requests and Cheerio for parsing. It is designed to be fast and lightweight, avoiding headless browsers like Playwright or Puppeteer.

## Features

- Scrapes GulfTalent job search results.
- Extracts job title, company, location, date posted, and description.
- Handles pagination to collect multiple pages of results.
- Saves results to the Apify dataset.

## Input

The actor accepts the following input fields:

- `keyword`: Search term(s) for jobs.
- `location`: Optional location filter.
- `posted_date`: "24h", "7d", "30d", or "anytime".
- `results_wanted`: The maximum number of jobs to scrape.

## Output

The actor outputs a dataset of job listings with the following fields:

- `title`: The job title.
- `company`: The company name.
- `location`: The job location.
- `date_posted`: When the job was posted.
- `description_html`: The job description in HTML format.
- `description_text`: The job description in plain text.
- `url`: The URL of the job posting.