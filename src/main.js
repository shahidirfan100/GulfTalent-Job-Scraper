import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset, KeyValueStore } from 'crawlee';

const BASE_URL = 'https://www.gulftalent.com';
const DEFAULT_SEARCH_URL = 'https://www.gulftalent.com/jobs/search';

// Configuration
const CRAWLER_CONFIG = {
    maxConcurrency: 3,
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
 * Constructs search URL with page parameter
 * CRITICAL: The pagination works via the page parameter in the query string
 */
function buildSearchUrl(baseSearchUrl, page) {
    try {
        const url = new URL(baseSearchUrl);
        url.searchParams.set('page', page.toString());
        return url.href;
    } catch (e) {
        // If URL parsing fails, try simple append
        const separator = baseSearchUrl.includes('?') ? '&' : '?';
        return `${baseSearchUrl}${separator}page=${page}`;
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
            
            // CRITICAL: Use desktop user agent and headers to avoid mobile redirect
            request.headers = {
                ...request.headers,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0',
                'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
            };
        },
    ],

    requestHandler: async ({ $, request, crawler, body }) => {
        const { userData: { label, baseSearchUrl, currentPage = 1 } } = request;

        if (label === 'LIST') {
            log.info(`Processing page ${currentPage}: ${request.url}`, {
                pagesScraped: state.pagesScraped,
                jobsScraped: state.jobsScraped,
            });
            
            if (isBlocked($)) {
                log.warning(`Blocking detected on ${request.url}`);
                throw new Error('Blocked - will retry with different proxy');
            }

            state.pagesScraped++;

            // Check if we're on mobile version (this is the problem!)
            const actualUrl = request.loadedUrl || request.url;
            const isMobile = actualUrl.includes('/mobile/');
            
            if (isMobile) {
                log.warning(`⚠️ REDIRECTED TO MOBILE VERSION: ${actualUrl}`);
                log.info('This is why pagination fails - mobile version has different structure!');
            }

            // STRATEGY 1: Try to extract JSON data from script tag (desktop version)
            const scriptContent = $('script:contains("facetedSearchResultsValue")').html();
            let jobs = [];
            let totalResults = 0;
            
            if (scriptContent) {
                log.info('✅ Found facetedSearchResultsValue JSON (desktop version)');
                
                const jsonStringMatch = scriptContent.match(/facetedSearchResultsValue['"]\s*,\s*({[\s\S]*?})\s*\)/);
                if (jsonStringMatch && jsonStringMatch[1]) {
                    try {
                        const searchResults = JSON.parse(jsonStringMatch[1]);
                        jobs = searchResults.results?.data || [];
                        totalResults = searchResults.results?.total || 0;
                        
                        log.info(`📊 JSON data parsed: ${jobs.length} jobs on this page, ${totalResults} total available`);
                    } catch (e) {
                        log.error(`Failed to parse JSON data`, { error: e.message });
                    }
                }
            }

            // STRATEGY 2: Fallback to HTML scraping if no JSON found
            if (jobs.length === 0) {
                log.info('⚠️ No JSON found, using HTML scraping fallback...');
                
                // Try multiple selectors to find job links
                const jobLinkSelectors = [
                    'a[href*="/jobs/"][href*="-"]',  // Desktop format: /country/jobs/title-id
                    'a[href^="/mobile/"][href*="/jobs/"]',  // Mobile format: /mobile/country/jobs/title-id
                ];
                
                let $jobLinks = $();
                for (const selector of jobLinkSelectors) {
                    const $links = $(selector).filter((i, el) => {
                        const href = $(el).attr('href');
                        // Must be a job detail page (has job ID number at end)
                        return href && 
                               href.match(/\/jobs\/[^/]+-\d+$/) && 
                               !href.includes('/search') &&
                               !href.includes('/category');
                    });
                    
                    if ($links.length > 0) {
                        $jobLinks = $links;
                        log.info(`✅ Found ${$links.length} job links using selector: ${selector}`);
                        break;
                    }
                }
                
                if ($jobLinks.length === 0) {
                    log.error(`❌ NO JOB LINKS FOUND ON PAGE!`);
                    log.info('Page title:', $('title').text());
                    log.info('Page URL:', actualUrl);
                    log.info('Body text preview:', $('body').text().substring(0, 200));
                    
                    // Check if we've hit the end
                    if (state.jobsScraped > 0 && currentPage > 1) {
                        log.info('Likely reached end of results.');
                        return;
                    }
                    
                    throw new Error('No jobs found on first page - likely blocked or site structure changed');
                }

                // Extract job data from HTML
                const processedThisPage = new Set();
                
                $jobLinks.each((i, el) => {
                    const $link = $(el);
                    const href = $link.attr('href');
                    const fullUrl = validateAndNormalizeUrl(href, BASE_URL);
                    
                    if (!fullUrl || state.seenUrls.has(fullUrl) || processedThisPage.has(fullUrl)) {
                        return; // Skip
                    }
                    
                    processedThisPage.add(fullUrl);
                    
                    // Try to extract job info from surrounding context
                    const $container = $link.closest('div, li, article, tr');
                    let title = $link.text().trim();
                    
                    // If link text is empty or too short, look for title nearby
                    if (!title || title.length < 3) {
                        const $titleElem = $container.find('h2, h3, h4, .job-title, [class*="title"]').first();
                        title = $titleElem.text().trim();
                    }
                    
                    if (!title || title.length < 3) {
                        log.debug('Skipping job with no valid title', { url: fullUrl });
                        return;
                    }
                    
                    const company = $container.find('.company-name, [class*="company"]')
                        .first().text().trim() || 'Not specified';
                    const location = $container.find('.location, [class*="location"]')
                        .first().text().trim() || 'Not specified';
                    const date = $container.find('.date, time, [class*="date"]')
                        .first().text().trim() || 'Not specified';
                    
                    jobs.push({
                        title,
                        company_name: company,
                        location,
                        posted_date_ts: null,
                        link: fullUrl,
                        date_posted_text: date,
                    });
                });
                
                log.info(`📋 Extracted ${jobs.length} jobs from HTML`);
            }

            // PROCESS ALL JOBS
            for (const job of jobs) {
                if (maxJobs && state.jobsScraped >= maxJobs) {
                    log.info(`✋ Reached maxJobs limit: ${maxJobs}`);
                    return; // Stop processing completely
                }

                const jobUrl = validateAndNormalizeUrl(job.link, BASE_URL);
                if (!jobUrl || state.seenUrls.has(jobUrl)) {
                    continue;
                }
                
                state.seenUrls.add(jobUrl);
                
                const jobData = {
                    title: job.title || 'Not specified',
                    company: job.company_name || 'Not specified',
                    location: job.location || 'Not specified',
                    date_posted: job.posted_date_ts 
                        ? new Date(job.posted_date_ts * 1000).toISOString() 
                        : (job.date_posted_text || 'Not specified'),
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

            log.info(`✅ Page ${currentPage} complete: ${jobs.length} jobs processed (Total: ${state.jobsScraped})`);

            // PAGINATION LOGIC
            const shouldContinue = (!maxJobs || state.jobsScraped < maxJobs) && 
                                  (!maxPages || state.pagesScraped < maxPages);
            
            if (!shouldContinue) {
                log.info(`🛑 Stopping: ${maxJobs ? 'maxJobs' : 'maxPages'} limit reached`);
                return;
            }

            // If we got jobs, try next page
            if (jobs.length > 0) {
                const nextPage = currentPage + 1;
                const nextPageUrl = buildSearchUrl(baseSearchUrl || request.url, nextPage);
                
                log.info(`➡️ Enqueueing page ${nextPage}: ${nextPageUrl}`);
                
                await crawler.addRequests([{ 
                    url: nextPageUrl,
                    userData: { 
                        label: 'LIST',
                        baseSearchUrl: baseSearchUrl || request.url,
                        currentPage: nextPage,
                    },
                }]);
            } else if (currentPage === 1) {
                // No jobs on first page is an error
                throw new Error('No jobs found on first page');
            } else {
                log.info(`🏁 No jobs on page ${currentPage}, assuming end of results`);
            }

        } else if (label === 'DETAIL') {
            log.info(`Scraping job detail: ${request.url}`);
            
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
                    log.warning(`No description found for: ${request.userData.jobData.title}`);
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
        log.warning(`❌ Request failed after ${CRAWLER_CONFIG.maxRequestRetries} retries: ${request.url}`, {
            errorMessages: request.errorMessages,
        });
    },
});

// --- BUILD START URLS ---
const startUrls = [];
let baseSearchUrl;

if (inputStartUrl) {
    baseSearchUrl = inputStartUrl;
    // Ensure page parameter is set to 1
    const url = new URL(inputStartUrl);
    url.searchParams.set('page', '1');
    startUrls.push({ 
        url: url.href,
        userData: { 
            label: 'LIST',
            baseSearchUrl: inputStartUrl,
            currentPage: 1,
        } 
    });
} else {
    // Build search URL from parameters or use default
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
    baseSearchUrl = constructedUrl.href;
    
    startUrls.push({ 
        url: constructedUrl.href,
        userData: { 
            label: 'LIST',
            baseSearchUrl: constructedUrl.href,
            currentPage: 1,
        } 
    });
}

log.info('🚀 Starting GulfTalent Job Scraper', { 
    startUrl: startUrls[0].url,
    config: {
        collectDetails,
        maxJobs: maxJobs || 'unlimited',
        maxPages: maxPages || 'unlimited',
        usingProxy: !!proxyConfig,
        usingCookies: !!cookieHeader,
    }
});

log.info('📝 IMPORTANT: If the scraper gets stuck at 25 jobs, it means:');
log.info('   1. Site is redirecting to mobile version (check logs for "REDIRECTED TO MOBILE")');
log.info('   2. Pagination links are not being found');
log.info('   3. Try using a different proxy or adding cookies');

await crawler.run(startUrls);

// Convert Set back to Array for storage
state.seenUrls = Array.from(state.seenUrls);

log.info('✅ Scraping completed!', { 
    totalPages: state.pagesScraped, 
    totalJobs: state.jobsScraped 
});

await Actor.exit();