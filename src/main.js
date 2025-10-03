import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset, KeyValueStore } from 'crawlee';
import { Cookie, CookieJar } from 'tough-cookie';

const BASE_URL = 'https://www.gulftalent.com';

await Actor.init();

const input = await Actor.getInput();
const {
    startUrl: inputStartUrl,
    keyword,
    location,
    posted_date,
    collectDetails = true,
    maxJobs,
    maxPages,
    cookies,
    proxyConfiguration,
} = input;

if (!inputStartUrl && !keyword) {
    throw new Error('Either \'startUrl\' or \'keyword\' must be provided as input.');
}

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

// --- STATE MANAGEMENT ---
const state = await KeyValueStore.getAutoSavedValue('CRAWLER_STATE', {
    pagesScraped: 0,
    jobsScraped: 0,
});

// --- COOKIE JAR SETUP ---
const cookieJar = new CookieJar();
if (cookies) {
    const cookieStrings = cookies.split(';').map(c => c.trim());
    for (const cookieString of cookieStrings) {
        try {
            await cookieJar.setCookie(Cookie.parse(cookieString), BASE_URL);
        } catch (e) {
            log.warning(`Failed to parse cookie: ${cookieString}`, { error: e.message });
        }
    }
}

const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConfig,
    cookieJar,
    requestHandler: async ({ $, request, log, crawler }) => {
        const { userData: { label } } = request;

        if (label === 'LIST') {
            log.info(`Scraping list page: ${request.url} (Page ${state.pagesScraped + 1})`);
            state.pagesScraped++;

            const scriptContent = $('script:contains("facetedSearchResultsValue")').html();
            if (!scriptContent) {
                log.warning(`Could not find the script tag with job data on ${request.url}. This might be a block page or the page structure may have changed.`);
                return;
            }

            const jsonStringMatch = scriptContent.match(/facetedSearchResultsValue\', (.*)\)/);
            if (!jsonStringMatch || !jsonStringMatch[1]) {
                log.warning(`Could not extract JSON from script tag on ${request.url}`);
                return;
            }

            try {
                const searchResults = JSON.parse(jsonStringMatch[1]);
                const jobs = searchResults.results.data;
                const totalResults = searchResults.results.total;

                if (jobs.length === 0) {
                    log.info('No more jobs found on this page. Stopping pagination.');
                    return;
                }

                for (const job of jobs) {
                    if (maxJobs && state.jobsScraped >= maxJobs) {
                        break; // Stop processing jobs on this page if maxJobs is reached
                    }

                    const jobUrl = new URL(job.link, BASE_URL).href;
                    const jobData = {
                        title: job.title,
                        company: job.company_name,
                        location: job.location,
                        date_posted: new Date(job.posted_date_ts * 1000).toISOString(),
                        url: jobUrl,
                    };

                    if (collectDetails) {
                        await crawler.addRequests([{ 
                            url: jobUrl,
                            userData: {
                                label: 'DETAIL',
                                jobData,
                            },
                        }]);
                    } else {
                        await Dataset.pushData({
                            ...jobData,
                            description_html: null,
                            description_text: null,
                        });
                    }
                    state.jobsScraped++;
                }

                // --- PAGINATION ---
                const continueCrawling = (!maxJobs || state.jobsScraped < maxJobs) && (!maxPages || state.pagesScraped < maxPages);
                if (continueCrawling && state.jobsScraped < totalResults) {
                    const currentPageMatch = request.url.match(/page=(\d+)/);
                    const currentPage = currentPageMatch ? parseInt(currentPageMatch[1], 10) : 1;
                    const nextUrl = request.url.includes('page=')
                        ? request.url.replace(/page=\d+/, `page=${currentPage + 1}`)
                        : `${request.url}${request.url.includes('?') ? '&' : '?'}page=${currentPage + 1}`;

                    log.info(`Enqueuing next page: ${nextUrl}`);
                    await crawler.addRequests([{ url: nextUrl, userData: { label: 'LIST' } }]);
                }

            } catch (e) {
                log.error(`Failed to parse job data from script tag on ${request.url}`, { error: e.message });
            }
        } else if (label === 'DETAIL') {
            log.info(`Scraping detail page: ${request.url}`);
            const descriptionElement = $('.job-description');
            const description_html = descriptionElement.html()?.trim() || null;
            const description_text = descriptionElement.text()?.trim() || null;

            await Dataset.pushData({
                ...request.userData.jobData,
                description_html,
                description_text,
            });
        }
    },
    failedRequestHandler: async ({ request, log }) => {
        log.warning(`Request ${request.url} failed and will be retried.`);
    },
});

// --- START URLS ---
const startUrls = [];
if (inputStartUrl) {
    startUrls.push({ url: inputStartUrl, userData: { label: 'LIST' } });
} else {
    const constructedUrl = new URL('/jobs/search', BASE_URL);
    if (keyword) {
        constructedUrl.searchParams.set('keyword', keyword);
    }
    if (location) {
        constructedUrl.searchParams.set('location', location);
    }
    if (posted_date && posted_date !== 'anytime') {
        const dateFilterMap = {
            '24h': 1,
            '7d': 7,
            '30d': 30,
        };
        if (dateFilterMap[posted_date]) {
            constructedUrl.searchParams.set('filters[posted_date]', dateFilterMap[posted_date]);
        }
    }
    constructedUrl.searchParams.set('page', 1);
    startUrls.push({ url: constructedUrl.href, userData: { label: 'LIST' } });
}

log.info('Starting crawl...');
await crawler.run(startUrls);
log.info('Crawl finished.');

await Actor.exit();
