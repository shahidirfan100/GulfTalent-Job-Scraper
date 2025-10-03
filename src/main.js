import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset, KeyValueStore } from 'crawlee';

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

// Parse cookies into headers format
const cookieHeader = cookies ? cookies.trim() : '';

const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConfig,
    maxRequestRetries: 3,
    maxConcurrency: 5,
    requestHandlerTimeoutSecs: 120,
    
    // Pre-navigation hooks to add cookies
    preNavigationHooks: [
        async ({ request, session }) => {
            // Add cookies to request headers
            if (cookieHeader) {
                request.headers = {
                    ...request.headers,
                    'Cookie': cookieHeader,
                };
            }
            
            // Add common headers to avoid blocking
            request.headers = {
                ...request.headers,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0',
            };
        },
    ],

    requestHandler: async ({ $, request, log, crawler }) => {
        const { userData: { label } } = request;

        if (label === 'LIST') {
            log.info(`Scraping list page: ${request.url} (Page ${state.pagesScraped + 1})`);
            
            // Check if we've been blocked
            if ($('title').text().toLowerCase().includes('access denied') || 
                $('title').text().toLowerCase().includes('captcha') ||
                $('body').text().toLowerCase().includes('blocked')) {
                log.warning(`Possible blocking detected on ${request.url}`);
                throw new Error('Blocked - will retry with different proxy');
            }

            state.pagesScraped++;

            // Try to find the script with job data
            const scriptContent = $('script:contains("facetedSearchResultsValue")').html();
            
            if (!scriptContent) {
                log.warning(`Could not find the script tag with job data on ${request.url}.`);
                
                // Fallback: Try to scrape jobs directly from HTML
                const jobCards = $('.search-result-item, .job-item, [data-job-id]');
                log.info(`Found ${jobCards.length} job cards using fallback selector`);
                
                if (jobCards.length === 0) {
                    log.warning('No jobs found. Page might be blocked or structure changed.');
                    log.info('Page title:', $('title').text());
                    return;
                }

                // Process each job card
                jobCards.each((i, elem) => {
                    if (maxJobs && state.jobsScraped >= maxJobs) return false;

                    const $card = $(elem);
                    const titleElem = $card.find('h2 a, .job-title a, a[data-job-title]').first();
                    const companyElem = $card.find('.company-name, [data-company-name]').first();
                    const locationElem = $card.find('.location, [data-location]').first();
                    const dateElem = $card.find('.date, .posted-date, time').first();
                    
                    const title = titleElem.text().trim();
                    const jobUrl = titleElem.attr('href');
                    
                    if (!title || !jobUrl) return;

                    const fullUrl = jobUrl.startsWith('http') ? jobUrl : new URL(jobUrl, BASE_URL).href;
                    
                    const jobData = {
                        title,
                        company: companyElem.text().trim() || 'Not specified',
                        location: locationElem.text().trim() || 'Not specified',
                        date_posted: dateElem.text().trim() || dateElem.attr('datetime') || 'Not specified',
                        url: fullUrl,
                    };

                    if (collectDetails) {
                        crawler.addRequests([{ 
                            url: fullUrl,
                            userData: {
                                label: 'DETAIL',
                                jobData,
                            },
                        }]);
                    } else {
                        Dataset.pushData({
                            ...jobData,
                            description_html: null,
                            description_text: null,
                        });
                    }
                    state.jobsScraped++;
                });

                // Try to find pagination
                const nextLink = $('a.next, .pagination .next, [rel="next"]').attr('href');
                if (nextLink && (!maxJobs || state.jobsScraped < maxJobs) && (!maxPages || state.pagesScraped < maxPages)) {
                    const nextUrl = nextLink.startsWith('http') ? nextLink : new URL(nextLink, BASE_URL).href;
                    log.info(`Enqueuing next page: ${nextUrl}`);
                    await crawler.addRequests([{ url: nextUrl, userData: { label: 'LIST' } }]);
                }

                return;
            }

            // Original JSON parsing logic
            const jsonStringMatch = scriptContent.match(/facetedSearchResultsValue['"]\s*,\s*({[\s\S]*?})\s*\)/);
            if (!jsonStringMatch || !jsonStringMatch[1]) {
                log.warning(`Could not extract JSON from script tag on ${request.url}`);
                return;
            }

            try {
                const searchResults = JSON.parse(jsonStringMatch[1]);
                const jobs = searchResults.results?.data || [];
                const totalResults = searchResults.results?.total || 0;

                log.info(`Found ${jobs.length} jobs on this page. Total: ${totalResults}`);

                if (jobs.length === 0) {
                    log.info('No more jobs found on this page. Stopping pagination.');
                    return;
                }

                for (const job of jobs) {
                    if (maxJobs && state.jobsScraped >= maxJobs) {
                        log.info(`Reached maxJobs limit: ${maxJobs}`);
                        break;
                    }

                    const jobUrl = job.link?.startsWith('http') 
                        ? job.link 
                        : new URL(job.link || '', BASE_URL).href;
                    
                    const jobData = {
                        title: job.title || 'Not specified',
                        company: job.company_name || 'Not specified',
                        location: job.location || 'Not specified',
                        date_posted: job.posted_date_ts 
                            ? new Date(job.posted_date_ts * 1000).toISOString() 
                            : 'Not specified',
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
                const continueCrawling = (!maxJobs || state.jobsScraped < maxJobs) && 
                                       (!maxPages || state.pagesScraped < maxPages);
                
                if (continueCrawling && state.jobsScraped < totalResults) {
                    const currentPageMatch = request.url.match(/page[=\/](\d+)/);
                    const currentPage = currentPageMatch ? parseInt(currentPageMatch[1], 10) : 1;
                    
                    let nextUrl;
                    if (request.url.includes('page=')) {
                        nextUrl = request.url.replace(/page=\d+/, `page=${currentPage + 1}`);
                    } else if (request.url.includes('/page/')) {
                        nextUrl = request.url.replace(/\/page\/\d+/, `/page/${currentPage + 1}`);
                    } else {
                        nextUrl = `${request.url}${request.url.includes('?') ? '&' : '?'}page=${currentPage + 1}`;
                    }

                    log.info(`Enqueuing next page: ${nextUrl} (Jobs scraped: ${state.jobsScraped}/${totalResults})`);
                    await crawler.addRequests([{ url: nextUrl, userData: { label: 'LIST' } }]);
                } else {
                    log.info('Pagination stopped. Conditions met or no more pages.');
                }

            } catch (e) {
                log.error(`Failed to parse job data from script tag on ${request.url}`, { error: e.message, stack: e.stack });
            }

        } else if (label === 'DETAIL') {
            log.info(`Scraping detail page: ${request.url}`);
            
            // Try multiple selectors for job description
            const descriptionElement = $('.job-description').first() || 
                                      $('.description').first() || 
                                      $('[itemprop="description"]').first() ||
                                      $('.job-details').first();
            
            const description_html = descriptionElement.html()?.trim() || null;
            const description_text = descriptionElement.text()?.trim() || null;

            // Extract additional details if available
            const salary = $('.salary, [itemprop="baseSalary"]').text().trim() || null;
            const jobType = $('.job-type, [itemprop="employmentType"]').text().trim() || null;
            const category = $('.category, .job-category').text().trim() || null;

            await Dataset.pushData({
                ...request.userData.jobData,
                description_html,
                description_text,
                salary,
                jobType,
                category,
            });
        }
    },

    failedRequestHandler: async ({ request, log }) => {
        log.warning(`Request ${request.url} failed after retries.`, {
            errorMessages: request.errorMessages,
        });
    },
});

// --- START URLS ---
const startUrls = [];
if (inputStartUrl) {
    startUrls.push({ url: inputStartUrl, userData: { label: 'LIST' } });
} else {
    const constructedUrl = new URL('/jobs/search', BASE_URL);
    
    if (keyword) {
        constructedUrl.searchParams.set('search_keyword', keyword);
    }
    if (location) {
        constructedUrl.searchParams.set('city', location);
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
    constructedUrl.searchParams.set('page', '1');
    
    startUrls.push({ url: constructedUrl.href, userData: { label: 'LIST' } });
}

log.info('Starting crawl...', { startUrls: startUrls.map(u => u.url) });
await crawler.run(startUrls);
log.info('Crawl finished.', { 
    totalPages: state.pagesScraped, 
    totalJobs: state.jobsScraped 
});

await Actor.exit();