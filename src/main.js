import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset, KeyValueStore } from 'crawlee';

const BASE_URL = 'https://www.gulftalent.com';
const DEFAULT_SEARCH_URL = 'https://www.gulftalent.com/jobs/search';

// Configuration
const CRAWLER_CONFIG = {
    maxConcurrency: 5,
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
// If no input provided, use default URL to fetch all jobs
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
});

// --- HELPER FUNCTIONS ---

/**
 * Validates and normalizes a URL
 */
function validateAndNormalizeUrl(url, baseUrl) {
    if (!url || typeof url !== 'string') return null;
    
    try {
        // Handle relative URLs starting with /
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
        'not found',
    ];
    
    return blockingIndicators.some(indicator => 
        title.includes(indicator) || bodyStart.includes(indicator)
    );
}

/**
 * Extracts metadata from job detail page
 */
function extractMetadata($, labelText) {
    // Try multiple selector strategies
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
    
    // Clone to avoid modifying original DOM
    const $desc = descriptionContainer.clone();
    
    // Remove unwanted sections
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
    
    // Remove company info sections
    $desc.find('h4:contains("About the Company")').nextAll().remove();
    
    let description_html = '';
    let description_text = '';
    
    // Extract only paragraphs with actual job description
    const companyKeywords = ['Linum Consult', 'All Linum Consultants', 'recruitment agency'];
    
    $desc.find('p').each((i, elem) => {
        const text = $(elem).text().trim();
        
        // Skip empty paragraphs and company info
        if (text && !companyKeywords.some(keyword => text.includes(keyword))) {
            description_html += $.html(elem);
            description_text += text + '\n\n';
        }
    });
    
    // Fallback: get all text if no paragraphs found
    if (!description_text.trim()) {
        description_text = $desc.text().trim();
        description_html = $desc.html()?.trim() || null;
    }
    
    // Clean up excessive whitespace
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
 * Detects if we're on a mobile page and extracts the canonical desktop URL
 */
function getDesktopUrl($, currentUrl) {
    // Check for canonical link
    const canonical = $('link[rel="canonical"]').attr('href');
    if (canonical && !canonical.includes('/mobile/')) {
        return canonical;
    }
    
    // If on mobile URL, convert to desktop
    if (currentUrl.includes('/mobile/')) {
        // Mobile URLs are like: /mobile/search/jobs-in-_
        // Desktop equivalent: /jobs/search
        return currentUrl.replace('/mobile/search/jobs-in-_', '/jobs/search');
    }
    
    return currentUrl;
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
    
    return 1;
}

/**
 * Constructs the next page URL
 */
function getNextPageUrl(currentUrl, currentPage) {
    const nextPage = currentPage + 1;
    
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
    
    // Pre-navigation hooks to add cookies and headers
    preNavigationHooks: [
        async ({ request }) => {
            // Add cookies to request headers
            if (cookieHeader) {
                request.headers = {
                    ...request.headers,
                    'Cookie': cookieHeader,
                };
            }
            
            // Add common headers to avoid blocking and mobile redirect
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
                // Prevent mobile redirect
                'Sec-Ch-Ua-Mobile': '?0',
            };
        },
    ],

    requestHandler: async ({ $, request, crawler }) => {
        const { userData: { label } } = request;

        if (label === 'LIST') {
            log.info(`Scraping list page: ${request.url}`, {
                page: state.pagesScraped + 1,
                jobsScraped: state.jobsScraped,
            });
            
            // Check if we've been redirected to mobile or blocked
            if (isBlocked($)) {
                log.warning(`Blocking detected on ${request.url}`);
                throw new Error('Blocked - will retry with different proxy');
            }
            
            // Check if redirected to mobile version
            const isMobilePage = request.url.includes('/mobile/') || 
                                $('meta[name="viewport"]').attr('content')?.includes('mobile');
            
            if (isMobilePage) {
                log.warning('Redirected to mobile page. Attempting to use mobile scraping strategy...');
                
                try {
                    state.pagesScraped++;
                    
                    // Mobile page scraping - simpler structure
                    const jobLinks = $('a[href*="/jobs/"], a[href*="/mobile/"]').filter((i, el) => {
                        const href = $(el).attr('href');
                        return href && (href.includes('/jobs/') || href.includes('/mobile/')) && 
                               !href.includes('/search') && !href.includes('/category');
                    });
                    
                    log.info(`Found ${jobLinks.length} job links on mobile page`);
                    
                    const processedUrls = new Set();
                    
                    for (let i = 0; i < jobLinks.length; i++) {
                        if (maxJobs && state.jobsScraped >= maxJobs) {
                            log.info(`Reached maxJobs limit: ${maxJobs}`);
                            break;
                        }
                        
                        const $link = $(jobLinks[i]);
                        const href = $link.attr('href');
                        const fullUrl = validateAndNormalizeUrl(href, BASE_URL);
                        
                        if (!fullUrl || processedUrls.has(fullUrl)) continue;
                        processedUrls.add(fullUrl);
                        
                        // Try to extract basic info from the link context
                        const $container = $link.closest('div, li, article');
                        const title = $link.text().trim() || 'Not specified';
                        
                        // Look for company, location, date in siblings or nearby elements
                        const company = $container.find('.company-name, [class*="company"]')
                            .first().text().trim() || 'Not specified';
                        const location = $container.find('.location, [class*="location"]')
                            .first().text().trim() || 'Not specified';
                        const date = $container.find('.date, time, [class*="date"]')
                            .first().text().trim() || 'Not specified';
                        
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
                    
                    // Try to find next page link on mobile
                    const continueCrawling = (!maxJobs || state.jobsScraped < maxJobs) && 
                                           (!maxPages || state.pagesScraped < maxPages);
                    
                    if (continueCrawling && jobLinks.length > 0) {
                        const currentPage = extractPageNumber(request.url);
                        // Try to get desktop URL and add page parameter
                        const desktopUrl = getDesktopUrl($, request.url);
                        const nextUrl = getNextPageUrl(desktopUrl, currentPage);
                        
                        log.info(`Enqueuing next page: ${nextUrl}`);
                        await crawler.addRequests([{ url: nextUrl, userData: { label: 'LIST' } }]);
                    }
                    
                    return;
                    
                } catch (e) {
                    log.error(`Failed during mobile page scraping on ${request.url}`, { 
                        error: e.message, 
                        stack: e.stack 
                    });
                    return;
                }
            }

            state.pagesScraped++;

            // Try to find the script with job data (desktop version)
            const scriptContent = $('script:contains("facetedSearchResultsValue")').html();
            
            if (!scriptContent) {
                log.warning(`Could not find script tag with job data on ${request.url}. Trying HTML fallback...`);
                
                // --- FALLBACK: HTML SCRAPING ---
                try {
                    const jobCards = $('.search-result-item, .job-item, [data-job-id], .job-list-item, [class*="job-card"]');
                    log.info(`Found ${jobCards.length} job cards using fallback selector`);
                    
                    if (jobCards.length === 0) {
                        log.warning('No jobs found. Page might be blocked or structure changed.');
                        log.info('Page title:', $('title').text());
                        log.info('Page URL:', request.url);
                        
                        // Try alternative: look for any links that might be job postings
                        const alternativeLinks = $('a[href*="/jobs/"]').filter((i, el) => {
                            const href = $(el).attr('href');
                            return href && !href.includes('/search') && !href.includes('/category');
                        });
                        
                        if (alternativeLinks.length > 0) {
                            log.info(`Found ${alternativeLinks.length} job links using alternative method`);
                        } else {
                            return;
                        }
                    }

                    // Process each job card
                    for (let i = 0; i < jobCards.length; i++) {
                        if (maxJobs && state.jobsScraped >= maxJobs) {
                            log.info(`Reached maxJobs limit: ${maxJobs}`);
                            break;
                        }

                        const $card = $(jobCards[i]);
                        const titleElem = $card.find('h2 a, h3 a, .job-title a, a[data-job-title]').first();
                        const companyElem = $card.find('.company-name, [data-company-name], [class*="company"]').first();
                        const locationElem = $card.find('.location, [data-location], [class*="location"]').first();
                        const dateElem = $card.find('.date, .posted-date, time, [class*="date"]').first();
                        
                        const title = titleElem.text().trim();
                        const jobUrl = titleElem.attr('href');
                        
                        if (!title || !jobUrl) {
                            log.debug('Skipping job card with missing title or URL');
                            continue;
                        }

                        const fullUrl = validateAndNormalizeUrl(jobUrl, BASE_URL);
                        if (!fullUrl) continue;
                        
                        const jobData = {
                            title,
                            company: companyElem.text().trim() || 'Not specified',
                            location: locationElem.text().trim() || 'Not specified',
                            date_posted: dateElem.text().trim() || dateElem.attr('datetime') || 'Not specified',
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

                    // Handle pagination
                    const continueCrawling = (!maxJobs || state.jobsScraped < maxJobs) && 
                                           (!maxPages || state.pagesScraped < maxPages);
                    
                    if (continueCrawling) {
                        // Try multiple pagination selectors
                        const paginationSelectors = [
                            'a.next',
                            '.pagination .next',
                            '[rel="next"]',
                            'a:contains("Next")',
                            '.pagination a:last',
                        ];
                        
                        let nextLink = null;
                        for (const selector of paginationSelectors) {
                            nextLink = $(selector).attr('href');
                            if (nextLink) break;
                        }
                        
                        if (nextLink) {
                            const nextUrl = validateAndNormalizeUrl(nextLink, BASE_URL);
                            if (nextUrl) {
                                log.info(`Enqueuing next page: ${nextUrl}`);
                                await crawler.addRequests([{ url: nextUrl, userData: { label: 'LIST' } }]);
                            }
                        } else {
                            // Fallback: construct next page URL manually
                            const currentPage = extractPageNumber(request.url);
                            const nextUrl = getNextPageUrl(request.url, currentPage);
                            log.info(`No pagination link found. Trying constructed URL: ${nextUrl}`);
                            await crawler.addRequests([{ url: nextUrl, userData: { label: 'LIST' } }]);
                        }
                    }

                } catch (e) {
                    log.error(`Failed during HTML fallback scraping on ${request.url}`, { 
                        error: e.message, 
                        stack: e.stack 
                    });
                }

                return;
            }

            // --- PRIMARY: JSON PARSING (Desktop version) ---
            const jsonStringMatch = scriptContent.match(/facetedSearchResultsValue['"]\s*,\s*({[\s\S]*?})\s*\)/);
            if (!jsonStringMatch || !jsonStringMatch[1]) {
                log.warning(`Could not extract JSON from script tag on ${request.url}`);
                return;
            }

            try {
                const searchResults = JSON.parse(jsonStringMatch[1]);
                const jobs = searchResults.results?.data || [];
                const totalResults = searchResults.results?.total || 0;

                log.info(`Found ${jobs.length} jobs on this page`, {
                    totalAvailable: totalResults,
                    scraped: state.jobsScraped,
                });

                if (jobs.length === 0) {
                    log.info('No more jobs found on this page. Stopping pagination.');
                    return;
                }

                for (const job of jobs) {
                    if (maxJobs && state.jobsScraped >= maxJobs) {
                        log.info(`Reached maxJobs limit: ${maxJobs}`);
                        break;
                    }

                    const jobUrl = validateAndNormalizeUrl(job.link, BASE_URL);
                    if (!jobUrl) {
                        log.warning('Skipping job with invalid URL', { job });
                        continue;
                    }
                    
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
                    const currentPage = extractPageNumber(request.url);
                    const nextUrl = getNextPageUrl(request.url, currentPage);

                    log.info(`Enqueuing next page: ${nextUrl}`, {
                        progress: `${state.jobsScraped}/${totalResults}`,
                    });
                    await crawler.addRequests([{ url: nextUrl, userData: { label: 'LIST' } }]);
                } else {
                    log.info('Pagination stopped.', {
                        reason: !continueCrawling ? 'Limits reached' : 'All jobs scraped',
                        totalScraped: state.jobsScraped,
                    });
                }

            } catch (e) {
                log.error(`Failed to parse job data from script tag on ${request.url}`, { 
                    error: e.message, 
                    stack: e.stack 
                });
            }

        } else if (label === 'DETAIL') {
            log.info(`Scraping detail page: ${request.url}`);
            
            try {
                // Extract structured job details
                const jobType = extractMetadata($, 'Job Type');
                const jobLocation = extractMetadata($, 'Job Location');
                const nationality = extractMetadata($, 'Nationality');
                const salary = extractMetadata($, 'Salary');
                const gender = extractMetadata($, 'Gender');
                const arabicFluency = extractMetadata($, 'Arabic Fluency');
                const jobFunction = extractMetadata($, 'Job Function');
                const companyIndustry = extractMetadata($, 'Company Industry');

                // Extract and clean description
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
                
                // Save basic data even if detail extraction fails
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
    // User provided a specific URL
    startUrls.push({ url: inputStartUrl, userData: { label: 'LIST' } });
} else if (keyword || location || posted_date) {
    // Build search URL from parameters
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
    // No input provided - use default URL to fetch all jobs
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

log.info('Crawl finished successfully! 🎉', { 
    totalPages: state.pagesScraped, 
    totalJobs: state.jobsScraped 
});

await Actor.exit();