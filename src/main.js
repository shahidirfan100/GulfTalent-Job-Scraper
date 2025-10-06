import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset, KeyValueStore } from 'crawlee';

const BASE_URL = 'https://www.gulftalent.com';

await Actor.init();

const input = await Actor.getInput();
const { keyword, location, posted_date, collectDetails = true, maxJobs, maxPages, cookies, proxyConfiguration } = input;

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

// State management
const state = await KeyValueStore.getAutoSavedValue('CRAWLER_STATE', {
    pagesScraped: 0,
    jobsScraped: 0,
    seenJobIds: [],
});

const seenJobIds = new Set(state.seenJobIds);

// Helper functions (keep your existing helper functions)
function validateAndNormalizeUrl(url, baseUrl) {
    if (!url || typeof url !== 'string') return null;
    try {
        if (url.startsWith('/')) return new URL(url, baseUrl).href;
        return url.startsWith('http') ? url : new URL(url, baseUrl).href;
    } catch (e) {
        return null;
    }
}

function extractJobId(url) {
    const match = url.match(/\/jobs\/[^/]+-(\d+)/);
    return match ? match[1] : null;
}

function cleanDescription($) {
    // Keep your existing cleanDescription function
    const descriptionContainer = $('.job-details, .job-description, [class*="job-content"]').first();
    if (!descriptionContainer.length) return { html: null, text: null };
    
    const $desc = descriptionContainer.clone();
    
    ['.header-ribbon', '.row.space-bottom-sm', '.space-bottom-sm', 
     'h4:contains("About the Company")', '.btn', '.btn-primary', 
     '[class*="apply"]', '[data-cy*="apply"]'].forEach(selector => {
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
    
    description_text = description_text.replace(/\s+/g, ' ').replace(/\n\s*\n/g, '\n\n').trim();
    description_html = description_html.replace(/>\s+</g, '><').trim();
    
    return { html: description_html || null, text: description_text || null };
}

function extractMetadata($, labelText) {
    try {
        const element = $(`span[style*="color: #6c757d"]:contains("${labelText}")`).parent();
        const value = element.find('span').last().text().trim();
        return (value && value !== 'Not Specified') ? value : null;
    } catch (e) {
        return null;
    }
}

// NEW: Function to extract search parameters from initial page
function extractSearchParams($) {
    try {
        const scriptContent = $('script:contains("facetedSearchResultsValue")').html();
        if (scriptContent) {
            const paramsMatch = scriptContent.match(/searchParams['"]\s*:\s*({[^}]+})/);
            if (paramsMatch && paramsMatch[1]) {
                // Clean and parse the JSON
                const cleanJson = paramsMatch[1]
                    .replace(/(\w+):/g, '"$1":')
                    .replace(/'/g, '"');
                return JSON.parse(cleanJson);
            }
        }
        return null;
    } catch (e) {
        log.warning('Failed to extract search params:', e.message);
        return null;
    }
}

// NEW: Function to build API URL
function buildApiUrl(baseParams, page = 1) {
    const params = {
        search_keyword: baseParams.search_keyword || '',
        city: baseParams.city || '',
        country: baseParams.country || '',
        category: baseParams.category || '',
        employment_type: baseParams.employment_type || '',
        industry: baseParams.industry || '',
        seniority: baseParams.seniority || '',
        page: page,
        // Add other parameters as needed
    };
    
    const queryString = Object.entries(params)
        .filter(([_, value]) => value)
        .map(([key, value]) => `${key}=${encodeURIComponent(value)}`)
        .join('&');
    
    return `${BASE_URL}/jobs/search?${queryString}`;
}

const cookieHeader = cookies ? cookies.trim() : '';

const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConfig,
    maxRequestRetries: 3,
    maxConcurrency: 3,
    requestHandlerTimeoutSecs: 60,
    
    preNavigationHooks: [
        async ({ request }) => {
            request.headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Cache-Control': 'no-cache',
                ...(cookieHeader && { 'Cookie': cookieHeader }),
            };
        },
    ],

    requestHandler: async ({ $, request, crawler }) => {
        const { userData } = request;

        if (userData.label === 'INITIAL') {
            log.info('🔍 Extracting search parameters from initial page');
            
            // Extract search parameters from the page
            const searchParams = extractSearchParams($) || {};
            
            // Override with user input if provided
            if (keyword) searchParams.search_keyword = keyword;
            if (location) searchParams.city = location;
            
            log.info('📊 Search parameters:', searchParams);
            
            // Start with page 1
            await crawler.addRequests([{
                url: buildApiUrl(searchParams, 1),
                userData: { 
                    label: 'LIST', 
                    page: 1,
                    searchParams,
                    baseUrl: request.url
                },
            }]);

        } else if (userData.label === 'LIST') {
            const pageNum = userData.page || 1;
            log.info(`📄 Processing page ${pageNum}`, { url: request.url });

            state.pagesScraped++;

            // Extract jobs from JSON data in script tags
            let jobs = [];
            let totalJobs = 0;
            let jobsPerPage = 20;
            
            const scriptContent = $('script:contains("facetedSearchResultsValue")').html();
            if (scriptContent) {
                try {
                    // Find the JSON data more reliably
                    const jsonMatch = scriptContent.match(/facetedSearchResultsValue\s*,\s*({[\s\S]*?})\s*\)/);
                    if (jsonMatch && jsonMatch[1]) {
                        const cleanJson = jsonMatch[1]
                            .replace(/(\w+):/g, '"$1":')
                            .replace(/'/g, '"');
                        
                        const searchResults = JSON.parse(cleanJson);
                        jobs = searchResults.results?.data || [];
                        totalJobs = searchResults.results?.total || 0;
                        jobsPerPage = searchResults.results?.per_page || 20;
                        
                        log.info(`✅ Found ${jobs.length} jobs in JSON data (Total: ${totalJobs})`);
                    }
                } catch (e) {
                    log.warning('Failed to parse JSON data:', e.message);
                }
            }

            // Fallback: HTML scraping if JSON not found
            if (jobs.length === 0) {
                log.info('🔄 Falling back to HTML scraping');
                const $jobLinks = $('a[href*="/jobs/"]').filter((i, el) => {
                    const href = $(el).attr('href');
                    return href && /\/jobs\/[^/]+-\d+$/.test(href) && 
                           !href.includes('/search') && !href.includes('/category');
                });

                $jobLinks.each((i, el) => {
                    const $link = $(el);
                    const href = $link.attr('href');
                    const jobId = extractJobId(href);
                    
                    if (!jobId || seenJobIds.has(jobId)) return;
                    
                    const $container = $link.closest('div, li, article, .job-item');
                    let title = $link.text().trim();
                    if (!title || title.length < 3) {
                        title = $container.find('h2, h3, .job-title, [class*="title"]').first().text().trim();
                    }
                    if (!title || title.length < 3) return;
                    
                    const fullUrl = validateAndNormalizeUrl(href, BASE_URL);
                    if (!fullUrl) return;
                    
                    jobs.push({
                        id: jobId,
                        link: fullUrl,
                        title: title,
                        company_name: $container.find('.company-name, [class*="company"]').first().text().trim() || 'Not specified',
                        location: $container.find('.location, [class*="location"]').first().text().trim() || 'Not specified',
                    });
                });
                
                log.info(`📋 Extracted ${jobs.length} jobs from HTML`);
            }

            // Process jobs found on this page
            let processedThisPage = 0;
            for (const job of jobs) {
                if (maxJobs && state.jobsScraped >= maxJobs) {
                    log.info(`✋ Reached maxJobs limit (${maxJobs})`);
                    break;
                }

                if (seenJobIds.has(job.id)) continue;
                seenJobIds.add(job.id);

                const jobData = {
                    title: job.title || 'Not specified',
                    company: job.company_name || 'Not specified',
                    location: job.location || 'Not specified',
                    date_posted: job.posted_date || 'Not specified',
                    url: job.link,
                };

                if (collectDetails) {
                    await crawler.addRequests([{ 
                        url: job.link,
                        userData: { label: 'DETAIL', jobData },
                    }]);
                } else {
                    await Dataset.pushData(jobData);
                }
                
                state.jobsScraped++;
                processedThisPage++;
            }

            log.info(`✅ Page ${pageNum}: Processed ${processedThisPage} jobs (Total: ${state.jobsScraped})`);

            // Calculate if we should continue to next page
            const hasMorePages = totalJobs > 0 ? (pageNum * jobsPerPage) < totalJobs : processedThisPage > 0;
            const shouldContinue = 
                hasMorePages &&
                (!maxJobs || state.jobsScraped < maxJobs) &&
                (!maxPages || state.pagesScraped < maxPages);

            if (shouldContinue) {
                const nextPage = pageNum + 1;
                const nextUrl = buildApiUrl(userData.searchParams, nextPage);
                
                log.info(`➡️ Adding page ${nextPage}: ${nextUrl}`);
                
                await crawler.addRequests([{
                    url: nextUrl,
                    userData: { 
                        label: 'LIST', 
                        page: nextPage,
                        searchParams: userData.searchParams,
                        baseUrl: userData.baseUrl
                    },
                }]);
            } else {
                log.info('🏁 Stopping pagination', {
                    reason: !hasMorePages ? 'No more pages' : 'Limit reached',
                    totalJobs: state.jobsScraped,
                    totalPages: state.pagesScraped,
                });
            }

        } else if (userData.label === 'DETAIL') {
            try {
                const description = cleanDescription($);
                
                await Dataset.pushData({
                    ...userData.jobData,
                    description_html: description.html,
                    description_text: description.text,
                    jobType: extractMetadata($, 'Job Type'),
                    jobLocation: extractMetadata($, 'Job Location'),
                    nationality: extractMetadata($, 'Nationality'),
                    salary: extractMetadata($, 'Salary'),
                    gender: extractMetadata($, 'Gender'),
                    arabicFluency: extractMetadata($, 'Arabic Fluency'),
                    jobFunction: extractMetadata($, 'Job Function'),
                    companyIndustry: extractMetadata($, 'Company Industry'),
                });
            } catch (e) {
                log.error(`Failed to extract details: ${e.message}`);
                await Dataset.pushData({
                    ...userData.jobData,
                    description_html: null,
                    description_text: null,
                    error: 'Failed to extract details',
                });
            }
        }
    },

    failedRequestHandler: async ({ request }) => {
        log.warning(`❌ Request failed: ${request.url}`, {
            errors: request.errorMessages,
        });
    },
});

// Build start URL
let startUrl;
if (input.startUrl) {
    startUrl = input.startUrl;
} else {
    const url = new URL('/jobs/search', BASE_URL);
    if (keyword) url.searchParams.set('search_keyword', keyword);
    if (location) url.searchParams.set('city', location);
    startUrl = url.href;
}

log.info('🚀 Starting scraper', { startUrl });

// Start with initial page to extract parameters
await crawler.run([{
    url: startUrl,
    userData: { label: 'INITIAL' },
}]);

// Save state
state.seenJobIds = Array.from(seenJobIds);

log.info('✅ Scraping complete!', {
    totalJobs: state.jobsScraped,
    totalPages: state.pagesScraped,
});

await Actor.exit();