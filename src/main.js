import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset, KeyValueStore } from 'crawlee';

const BASE_URL = 'https://www.gulftalent.com';
const DEFAULT_SEARCH_URL = 'https://www.gulftalent.com/jobs/search';

// Configuration
const CRAWLER_CONFIG = {
    maxConcurrency: 3, // Reduced to be more gentle
    maxRequestRetries: 3,
    requestHandlerTimeoutSecs: 120,
};

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

// --- INPUT VALIDATION ---
if (!inputStartUrl && !keyword) {
    log.info('No startUrl or keyword provided. Using default URL to fetch all available jobs.');
}

if (maxJobs && maxJobs < 1) {
    throw new Error('maxJobs must be >= 1');
}

if (maxPages && maxPages < 1) {
    throw new Error('maxPages must be >= 1');
}

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

// --- STATE MANAGEMENT ---
const state = await KeyValueStore.getAutoSavedValue('CRAWLER_STATE', {
    pagesScraped: 0,
    jobsScraped: 0,
    seenUrls: new Set(),
});

// Convert Set to Array for serialization
if (Array.isArray(state.seenUrls)) {
    state.seenUrls = new Set(state.seenUrls);
} else if (!(state.seenUrls instanceof Set)) {
    state.seenUrls = new Set();
}

// --- HELPER FUNCTIONS ---

/**
 * Validates and normalizes a URL
 */
function validateAndNormalizeUrl(url, baseUrl) {
    if (!url || typeof url !== 'string') return null;
    
    try {
        if (url.startsWith('/')) {
            return new URL(url, baseUrl).href;
        }
        return url.startsWith('http') ? url : new URL(url, baseUrl).href;
    } catch (e) {
        log.warning(`Invalid URL: ${url}`, { error: e.message });
        return null;
    }
}

/**
 * Checks if the page shows blocking/captcha
 */
function isBlocked($) {
    const title = $('title').text().toLowerCase();
    const bodyStart = $('body').text().substring(0, 500).toLowerCase();
    
    const blockingIndicators = [
        'access denied',
        'captcha',
        'cloudflare',
        'security check',
        'blocked',
        'robot',
    ];
    
    return blockingIndicators.some(indicator => 
        title.includes(indicator) || bodyStart.includes(indicator)
    );
}

/**
 * Extracts metadata from job detail page
 */
function extractMetadata($, labelText) {
    const selectors = [
        `span[style*="color: #6c757d"]:contains("${labelText}")`,
        `.job-metadata span:contains("${labelText}")`,
        `label:contains("${labelText}")`,
    ];
    
    for (const selector of selectors) {
        try {
            const element = $(selector).parent();
            const value = element.find('span').last().text().trim();
            if (value && value !== 'Not Specified') {
                return value;
            }
        } catch (e) {
            // Continue to next selector
        }
    }
    
    log.debug(`Metadata not found: ${labelText}`);
    return null;
}

/**
 * Cleans and extracts job description
 */
function cleanDescription($) {
    const descriptionContainer = $('.job-details, .job-description, [class*="job-content"]').first();
    
    if (!descriptionContainer.length) {
        log.warning('Description container not found');
        return { html: null, text: null };
    }
    
    const $desc = descriptionContainer.clone();
    
    const unwantedSelectors = [
        '.header-ribbon',
        '.row.space-bottom-sm',
        '.space-bottom-sm',
        'h4:contains("About the Company")',
        '.btn',
        '.btn-primary',
        '[class*="apply"]',
        '[data-cy*="apply"]',
    ];
    
    unwantedSelectors.forEach(selector => {
        $desc.find(selector).remove();
    });
    
    $desc.find('h4:contains("About the Company")').nextAll().remove();
    
    let description_html = '';
    let description_text = '';
    
    const companyKeywords = ['Linum Consult', 'All Linum Consultants', 'recruitment agency'];
    
    $desc.find('p').each((i, elem) => {
        const text = $(elem).text().trim();
        
        if (text && !companyKeywords.some(keyword => text.includes(keyword))) {
            description_html += $.html(elem);
            description_text += text + '\n\n';
        }
    });
    
    if (!description_text.trim()) {
        description_text = $desc.text().trim();
        description_html = $desc.html()?.trim() || null;
    }
    
    description_text = description_text
        .replace(/\s+/g, ' ')
        .replace(/\n\s*\n/g, '\n\n')
        .trim();
    
    description_html = description_html
        .replace(/>\s+</g, '><')
        .trim();
    
    return {
        html: description_html || null,
        text: description_text || null,
    };
}

/**
 * Extracts page number from URL
 */
function extractPageNumber(url) {
    const pageMatch = url.match(/[?&]page=(\d+)/i);
    if (pageMatch) {
        return parseInt(pageMatch[1], 10);
    }
    
    const pathMatch = url.match(/\/page\/(\d+)/i);
    if (pathMatch) {
        return parseInt(pathMatch[1], 10);
    }
    
    // Check for mobile format: /mobile/search/jobs-in-_?page=2
    const mobileMatch = url.match(/\/jobs-in-[^?]*\?page=(\d+)/i);
    if (mobileMatch) {
        return parseInt(mobileMatch[1], 10);
    }
    
    return 1;
}

/**
 * Constructs the next page URL for mobile or desktop
 */
function getNextPageUrl(currentUrl, currentPage) {
    const nextPage = currentPage + 1;
    
    // Handle mobile URLs: /mobile/search/jobs-in-_?page=2
    if (currentUrl.includes('/mobile/')) {
        if (currentUrl.includes('?page=')) {
            return currentUrl.replace(/\?page=\d+/, `?page=${nextPage}`);
        } else if (currentUrl.includes('&page=')) {
            return currentUrl.replace(/&page=\d+/, `&page=${nextPage}`);
        } else {
            // Add page parameter
            return `${currentUrl}?page=${nextPage}`;
        }
    }
    
    // Handle desktop URLs
    if (currentUrl.includes('page=')) {
        return currentUrl.replace(/page=\d+/, `page=${nextPage}`);
    } else if (currentUrl.includes('/page/')) {
        return currentUrl.replace(/\/page\/\d+/, `/page/${nextPage}`);
    } else {
        const separator = currentUrl.includes('?') ? '&' : '?';
        return `${currentUrl}${separator}page=${nextPage}`;
    }
}

// Parse cookies into headers format
const cookieHeader = cookies ? cookies.trim() : '';

if (cookieHeader) {
    log.info('Using custom cookies for requests');
}

const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConfig,
    maxRequestRetries: CRAWLER_CONFIG.maxRequestRetries,
    maxConcurrency: CRAWLER_CONFIG.maxConcurrency,
    requestHandlerTimeoutSecs: CRAWLER_CONFIG.requestHandlerTimeoutSecs,
    
    preNavigationHooks: [
        async ({ request }) => {
            if (cookieHeader) {
                request.headers = {
                    ...request.headers,
                    'Cookie': cookieHeader,
                };
            }
            
            // Use desktop user agent to try to get desktop version
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
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0',
                'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
            };
        },
    ],

    requestHandler: async ({ $, request, crawler }) => {
        const { userData: { label } } = request;

        if (label === 'LIST') {
            const currentPage = extractPageNumber(request.url);
            
            log.info(`Scraping list page: ${request.url}`, {
                page: currentPage,
                pagesScraped: state.pagesScraped + 1,
                jobsScraped: state.jobsScraped,
            });
            
            if (isBlocked($)) {
                log.warning(`Blocking detected on ${request.url}`);
                throw new Error('Blocked - will retry with different proxy');
            }

            state.pagesScraped++;

            // Detect if we're on mobile or desktop
            const isMobilePage = request.url.includes('/mobile/');
            
            if (isMobilePage) {
                log.info('Processing mobile page format');
            } else {
                log.info('Processing desktop page format');
            }

            // Try to find the script with job data (desktop version)
            const scriptContent = $('script:contains("facetedSearchResultsValue")').html();
            
            if (scriptContent) {
                log.info('Found JSON data in script tag (desktop format)');
                
                const jsonStringMatch = scriptContent.match(/facetedSearchResultsValue['"]\s*,\s*({[\s\S]*?})\s*\)/);
                if (jsonStringMatch && jsonStringMatch[1]) {
                    try {
                        const searchResults = JSON.parse(jsonStringMatch[1]);
                        const jobs = searchResults.results?.data || [];
                        const totalResults = searchResults.results?.total || 0;

                        log.info(`Found ${jobs.length} jobs on this page`, {
                            totalAvailable: totalResults,
                            scraped: state.jobsScraped,
                        });

                        if (jobs.length === 0) {
                            log.info('No more jobs found. End of results.');
                            return;
                        }

                        for (const job of jobs) {
                            if (maxJobs && state.jobsScraped >= maxJobs) {
                                log.info(`Reached maxJobs limit: ${maxJobs}`);
                                break;
                            }

                            const jobUrl = validateAndNormalizeUrl(job.link, BASE_URL);
                            if (!jobUrl || state.seenUrls.has(jobUrl)) {
                                if (state.seenUrls.has(jobUrl)) {
                                    log.debug('Skipping duplicate job URL');
                                }
                                continue;
                            }
                            
                            state.seenUrls.add(jobUrl);
                            
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

                        // Check if we should continue
                        const continueCrawling = (!maxJobs || state.jobsScraped < maxJobs) && 
                                               (!maxPages || state.pagesScraped < maxPages);
                        
                        if (continueCrawling && state.jobsScraped < totalResults) {
                            const nextUrl = getNextPageUrl(request.url, currentPage);
                            log.info(`Enqueuing next page: ${nextUrl}`, {
                                progress: `${state.jobsScraped}/${totalResults}`,
                            });
                            await crawler.addRequests([{ url: nextUrl, userData: { label: 'LIST' } }]);
                        } else {
                            log.info('Pagination stopped.', {
                                reason: !continueCrawling ? 'Limits reached' : 'All jobs scraped',
                            });
                        }

                        return;
                    } catch (e) {
                        log.error(`Failed to parse JSON data`, { 
                            error: e.message, 
                            stack: e.stack 
                        });
                    }
                }
            }

            // --- FALLBACK: HTML SCRAPING (Mobile or Desktop without JSON) ---
            log.info('Using HTML scraping fallback');
            
            try {
                // Find job links - both mobile and desktop formats
                let jobLinks;
                
                if (isMobilePage) {
                    // Mobile: links are direct in the page
                    jobLinks = $('a[href^="/mobile/"]').filter((i, el) => {
                        const href = $(el).attr('href');
                        return href && href.match(/\/mobile\/[^/]+\/jobs\/[^/]+-\d+$/);
                    });
                } else {
                    // Desktop: try to find job cards or links
                    jobLinks = $('.search-result-item a, .job-item a, [data-job-id] a, a[href*="/jobs/"]').filter((i, el) => {
                        const href = $(el).attr('href');
                        return href && !href.includes('/search') && !href.includes('/category');
                    });
                }
                
                log.info(`Found ${jobLinks.length} job links via HTML scraping`);
                
                if (jobLinks.length === 0) {
                    log.warning('No job links found on this page. Might be end of results or page structure changed.');
                    log.info('Page title:', $('title').text());
                    return;
                }

                const processedThisPage = new Set();
                
                for (let i = 0; i < jobLinks.length; i++) {
                    if (maxJobs && state.jobsScraped >= maxJobs) {
                        log.info(`Reached maxJobs limit: ${maxJobs}`);
                        break;
                    }

                    const $link = $(jobLinks[i]);
                    const href = $link.attr('href');
                    const fullUrl = validateAndNormalizeUrl(href, BASE_URL);
                    
                    if (!fullUrl || state.seenUrls.has(fullUrl) || processedThisPage.has(fullUrl)) {
                        continue;
                    }
                    
                    state.seenUrls.add(fullUrl);
                    processedThisPage.add(fullUrl);
                    
                    // Extract basic info
                    const $container = $link.closest('div, li, article, tr');
                    let title = $link.text().trim();
                    
                    // If link text is empty, try to find title nearby
                    if (!title || title.length < 3) {
                        title = $container.find('h2, h3, .job-title, [class*="title"]').first().text().trim();
                    }
                    
                    const company = $container.find('.company-name, [class*="company"]')
                        .first().text().trim() || 'Not specified';
                    const location = $container.find('.location, [class*="location"]')
                        .first().text().trim() || 'Not specified';
                    const date = $container.find('.date, time, [class*="date"]')
                        .first().text().trim() || 'Not specified';
                    
                    if (!title || title === 'Not specified') {
                        log.debug('Skipping job with no title', { url: fullUrl });
                        continue;
                    }
                    
                    const jobData = {
                        title,
                        company,
                        location,
                        date_posted: date,
                        url: fullUrl,
                    };
                    
                    if (collectDetails) {
                        await crawler.addRequests([{ 
                            url: fullUrl,
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

                log.info(`Processed ${processedThisPage.size} jobs from this page`);

                // Handle pagination
                const continueCrawling = (!maxJobs || state.jobsScraped < maxJobs) && 
                                       (!maxPages || state.pagesScraped < maxPages);
                
                if (continueCrawling && processedThisPage.size > 0) {
                    const nextUrl = getNextPageUrl(request.url, currentPage);
                    log.info(`Enqueuing next page: ${nextUrl}`);
                    await crawler.addRequests([{ url: nextUrl, userData: { label: 'LIST' } }]);
                } else {
                    log.info('Pagination stopped.', {
                        reason: processedThisPage.size === 0 ? 'No jobs found' : 'Limits reached',
                        totalScraped: state.jobsScraped,
                    });
                }

            } catch (e) {
                log.error(`Failed during HTML scraping on ${request.url}`, { 
                    error: e.message, 
                    stack: e.stack 
                });
            }

        } else if (label === 'DETAIL') {
            log.info(`Scraping detail page: ${request.url}`);
            
            try {
                const jobType = extractMetadata($, 'Job Type');
                const jobLocation = extractMetadata($, 'Job Location');
                const nationality = extractMetadata($, 'Nationality');
                const salary = extractMetadata($, 'Salary');
                const gender = extractMetadata($, 'Gender');
                const arabicFluency = extractMetadata($, 'Arabic Fluency');
                const jobFunction = extractMetadata($, 'Job Function');
                const companyIndustry = extractMetadata($, 'Company Industry');

                const description = cleanDescription($);
                
                if (!description.text) {
                    log.warning(`No description found for job: ${request.userData.jobData.title}`);
                }

                await Dataset.pushData({
                    ...request.userData.jobData,
                    description_html: description.html,
                    description_text: description.text,
                    jobType,
                    jobLocation,
                    nationality,
                    salary,
                    gender,
                    arabicFluency,
                    jobFunction,
                    companyIndustry,
                });
                
            } catch (e) {
                log.error(`Failed to extract details from ${request.url}`, {
                    error: e.message,
                    stack: e.stack,
                });
                
                await Dataset.pushData({
                    ...request.userData.jobData,
                    description_html: null,
                    description_text: null,
                    error: 'Failed to extract job details',
                });
            }
        }
    },

    failedRequestHandler: async ({ request }) => {
        log.warning(`Request ${request.url} failed after ${CRAWLER_CONFIG.maxRequestRetries} retries`, {
            errorMessages: request.errorMessages,
            userData: request.userData,
        });
    },
});

// --- START URLS ---
const startUrls = [];

if (inputStartUrl) {
    startUrls.push({ url: inputStartUrl, userData: { label: 'LIST' } });
} else if (keyword || location || posted_date) {
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
} else {
    log.info('No search parameters provided. Fetching all available jobs from default URL.');
    startUrls.push({ url: DEFAULT_SEARCH_URL, userData: { label: 'LIST' } });
}

log.info('Starting crawl...', { 
    startUrls: startUrls.map(u => u.url),
    config: {
        collectDetails,
        maxJobs: maxJobs || 'unlimited',
        maxPages: maxPages || 'unlimited',
        usingProxy: !!proxyConfig,
        usingCookies: !!cookieHeader,
    }
});

await crawler.run(startUrls);

// Convert Set back to Array for storage
state.seenUrls = Array.from(state.seenUrls);

log.info('Crawl finished successfully! 🎉', { 
    totalPages: state.pagesScraped, 
    totalJobs: state.jobsScraped 
});

await Actor.exit();