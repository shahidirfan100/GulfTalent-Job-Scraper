import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset, KeyValueStore } from 'crawlee';

const BASE_URL = 'https://www.gulftalent.com';
const DEFAULT_SEARCH_URL = 'https://www.gulftalent.com/jobs/search';

// Configuration
const CRAWLER_CONFIG = {
    maxConcurrency: 5,
    maxRequestRetries: 2,
    requestHandlerTimeoutSecs: 60,
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

// Input validation
if (maxJobs && maxJobs < 1) throw new Error('maxJobs must be >= 1');
if (maxPages && maxPages < 1) throw new Error('maxPages must be >= 1');

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

// State management
const state = await KeyValueStore.getAutoSavedValue('CRAWLER_STATE', {
    pagesScraped: 0,
    jobsScraped: 0,
    seenJobIds: [],
});

// Convert array back to Set
const seenJobIds = new Set(state.seenJobIds);

// Helper functions
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
    // Extract job ID from URLs like: /uae/jobs/title-12345 or /mobile/uae/jobs/title-12345
    const match = url.match(/\/jobs\/[^/]+-(\d+)/);
    return match ? match[1] : null;
}

function isBlocked($) {
    const title = $('title').text().toLowerCase();
    const bodyStart = $('body').text().substring(0, 300).toLowerCase();
    return ['access denied', 'captcha', 'cloudflare', 'blocked'].some(indicator => 
        title.includes(indicator) || bodyStart.includes(indicator)
    );
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

function cleanDescription($) {
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

const cookieHeader = cookies ? cookies.trim() : '';

const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConfig,
    maxRequestRetries: CRAWLER_CONFIG.maxRequestRetries,
    maxConcurrency: CRAWLER_CONFIG.maxConcurrency,
    requestHandlerTimeoutSecs: CRAWLER_CONFIG.requestHandlerTimeoutSecs,
    
    preNavigationHooks: [
        async ({ request }) => {
            request.headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Ch-Ua-Mobile': '?0',
                'Cache-Control': 'no-cache',
                ...(cookieHeader && { 'Cookie': cookieHeader }),
            };
        },
    ],

    requestHandler: async ({ $, request, crawler }) => {
        const { userData } = request;

        if (userData.label === 'LIST') {
            const pageNum = userData.page || 1;
            log.info(`📄 Scraping page ${pageNum}`, {
                url: request.url,
                jobsScraped: state.jobsScraped,
                pagesScraped: state.pagesScraped,
            });
            
            if (isBlocked($)) {
                log.error('🚫 Page blocked - stopping');
                throw new Error('Blocked by website');
            }

            state.pagesScraped++;
            
            // Check if mobile redirect happened
            const actualUrl = request.loadedUrl || request.url;
            if (actualUrl.includes('/mobile/')) {
                log.warning(`⚠️ Mobile redirect detected: ${actualUrl}`);
            }

            // Try to find JSON data first
            let jobs = [];
            let totalAvailable = 0;
            
            const scriptContent = $('script:contains("facetedSearchResultsValue")').html();
            if (scriptContent) {
                const jsonMatch = scriptContent.match(/facetedSearchResultsValue['"]\s*,\s*({[\s\S]*?})\s*\)/);
                if (jsonMatch && jsonMatch[1]) {
                    try {
                        const searchResults = JSON.parse(jsonMatch[1]);
                        jobs = searchResults.results?.data || [];
                        totalAvailable = searchResults.results?.total || 0;
                        log.info(`✅ Found JSON data: ${jobs.length} jobs, ${totalAvailable} total`);
                    } catch (e) {
                        log.warning('Failed to parse JSON:', e.message);
                    }
                }
            }

            // Fallback: HTML scraping
            if (jobs.length === 0) {
                log.info('🔄 Using HTML fallback');
                
                const $links = $('a[href*="/jobs/"]').filter((i, el) => {
                    const href = $(el).attr('href');
                    return href && href.match(/\/jobs\/[^/]+-\d+$/) && 
                           !href.includes('/search') && !href.includes('/category');
                });
                
                log.info(`Found ${$links.length} job links in HTML`);
                
                if ($links.length === 0) {
                    if (pageNum === 1) {
                        log.error('❌ No jobs found on first page!');
                        log.info('Title:', $('title').text());
                        log.info('Body preview:', $('body').text().substring(0, 200));
                    } else {
                        log.info('📭 Empty page - assuming end of results');
                    }
                    return;
                }

                const processedIds = new Set();
                $links.each((i, el) => {
                    const $link = $(el);
                    const href = $link.attr('href');
                    const jobId = extractJobId(href);
                    
                    if (!jobId || seenJobIds.has(jobId) || processedIds.has(jobId)) return;
                    processedIds.add(jobId);
                    
                    const $container = $link.closest('div, li, article');
                    let title = $link.text().trim();
                    if (!title || title.length < 3) {
                        title = $container.find('h2, h3, .job-title').first().text().trim();
                    }
                    if (!title || title.length < 3) return;
                    
                    const fullUrl = validateAndNormalizeUrl(href, BASE_URL);
                    if (!fullUrl) return;
                    
                    jobs.push({
                        link: fullUrl,
                        title,
                        company_name: $container.find('.company-name, [class*="company"]').first().text().trim() || 'Not specified',
                        location: $container.find('.location, [class*="location"]').first().text().trim() || 'Not specified',
                        posted_date_ts: null,
                    });
                });
                
                log.info(`📋 Extracted ${jobs.length} unique jobs from HTML`);
            }

            // Process jobs
            let addedThisPage = 0;
            for (const job of jobs) {
                if (maxJobs && state.jobsScraped >= maxJobs) {
                    log.info(`✋ Reached maxJobs limit (${maxJobs})`);
                    return;
                }

                const jobId = extractJobId(job.link);
                if (!jobId || seenJobIds.has(jobId)) continue;
                
                seenJobIds.add(jobId);
                
                const jobUrl = validateAndNormalizeUrl(job.link, BASE_URL);
                if (!jobUrl) continue;
                
                const jobData = {
                    title: job.title || 'Not specified',
                    company: job.company_name || 'Not specified',
                    location: job.location || 'Not specified',
                    date_posted: job.posted_date_ts ? new Date(job.posted_date_ts * 1000).toISOString() : 'Not specified',
                    url: jobUrl,
                };

                if (collectDetails) {
                    await crawler.addRequests([{ 
                        url: jobUrl,
                        userData: { label: 'DETAIL', jobData },
                    }]);
                } else {
                    await Dataset.pushData({
                        ...jobData,
                        description_html: null,
                        description_text: null,
                    });
                }
                
                state.jobsScraped++;
                addedThisPage++;
            }

            log.info(`✅ Page ${pageNum} done: +${addedThisPage} jobs (Total: ${state.jobsScraped})`);

            // Decide if we should fetch next page
            const shouldContinue = 
                addedThisPage > 0 && // Got jobs on this page
                (!maxJobs || state.jobsScraped < maxJobs) &&
                (!maxPages || state.pagesScraped < maxPages) &&
                (totalAvailable === 0 || state.jobsScraped < totalAvailable); // More jobs available

            if (!shouldContinue) {
                log.info('🏁 Stopping pagination', {
                    reason: addedThisPage === 0 ? 'No jobs found' : 'Limit reached',
                    totalJobs: state.jobsScraped,
                    totalPages: state.pagesScraped,
                });
                return;
            }

            // Build next page URL
            const nextPage = pageNum + 1;
            let nextUrl;
            
            // Use the original base URL from userData if available
            if (userData.baseUrl) {
                const url = new URL(userData.baseUrl);
                url.searchParams.set('page', nextPage.toString());
                nextUrl = url.href;
            } else {
                // Fallback: modify current URL
                const url = new URL(request.url);
                url.searchParams.set('page', nextPage.toString());
                nextUrl = url.href;
            }
            
            log.info(`➡️ Next page ${nextPage}: ${nextUrl}`);
            
            await crawler.addRequests([{
                url: nextUrl,
                userData: { 
                    label: 'LIST', 
                    page: nextPage,
                    baseUrl: userData.baseUrl || request.url,
                },
            }]);

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
let baseUrl;

if (inputStartUrl) {
    const url = new URL(inputStartUrl);
    url.searchParams.set('page', '1');
    startUrl = url.href;
    baseUrl = inputStartUrl;
} else {
    const url = new URL('/jobs/search', BASE_URL);
    
    if (keyword) url.searchParams.set('search_keyword', keyword);
    if (location) url.searchParams.set('city', location);
    if (posted_date && posted_date !== 'anytime') {
        const dateMap = { '24h': 1, '7d': 7, '30d': 30 };
        if (dateMap[posted_date]) url.searchParams.set('filters[posted_date]', dateMap[posted_date]);
    }
    
    url.searchParams.set('page', '1');
    startUrl = url.href;
    baseUrl = url.href;
}

log.info('🚀 Starting scraper', {
    startUrl,
    maxJobs: maxJobs || '∞',
    maxPages: maxPages || '∞',
    collectDetails,
    proxy: !!proxyConfig,
});

await crawler.run([{
    url: startUrl,
    userData: { label: 'LIST', page: 1, baseUrl },
}]);

// Save seen job IDs
state.seenJobIds = Array.from(seenJobIds);

log.info('✅ Scraping complete!', {
    totalJobs: state.jobsScraped,
    totalPages: state.pagesScraped,
});

await Actor.exit();