import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset, KeyValueStore } from 'crawlee';

const BASE_URL = 'https://www.gulftalent.com';

await Actor.init();

const input = await Actor.getInput();
const { 
    keyword, 
    location, 
    posted_date, 
    collectDetails = true, 
    maxJobs, 
    maxPages,
    cookies,
    proxyConfiguration 
} = input;

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

// State management
const state = await KeyValueStore.getAutoSavedValue('CRAWLER_STATE', {
    pagesScraped: 0,
    jobsScraped: 0,
    seenJobIds: [],
});

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
    const match = url.match(/\/jobs\/[^/]+-(\d+)/);
    return match ? match[1] : null;
}

function cleanDescription($) {
    try {
        const descriptionContainer = $('.job-details, .job-description, [class*="job-content"]').first();
        if (!descriptionContainer.length) return { html: null, text: null };
        
        // Clone to avoid modifying original
        const $desc = descriptionContainer.clone();
        
        // Remove unwanted elements
        $desc.find('.header-ribbon, .btn, [class*="apply"], .row.space-bottom-sm').remove();
        
        // Remove everything after "About the Company"
        const aboutCompany = $desc.find('h4:contains("About the Company")');
        if (aboutCompany.length) {
            aboutCompany.nextAll().remove();
            aboutCompany.remove();
        }
        
        const html = $desc.html()?.trim() || null;
        const text = $desc.text().trim() || null;
        
        return { 
            html: html ? html.replace(/>\s+</g, '><').trim() : null, 
            text: text ? text.replace(/\s+/g, ' ').trim() : null 
        };
    } catch (e) {
        return { html: null, text: null };
    }
}

function extractMetadata($, labelText) {
    try {
        const element = $(`span:contains("${labelText}")`).closest('div, p');
        const value = element.find('span').last().text().trim();
        return (value && value !== 'Not Specified') ? value : null;
    } catch (e) {
        return null;
    }
}

// Simple function to check if we're blocked
function isBlocked($) {
    const title = $('title').text().toLowerCase();
    return title.includes('captcha') || title.includes('blocked') || title.includes('access denied');
}

// Build start URL
function buildStartUrl() {
    const url = new URL('/jobs/search', BASE_URL);
    
    if (keyword) url.searchParams.set('search_keyword', keyword);
    if (location) url.searchParams.set('city', location);
    
    // Add date filter if specified
    if (posted_date && posted_date !== 'anytime') {
        const dateMap = { 
            '24h': '1', 
            '7d': '7', 
            '30d': '30' 
        };
        if (dateMap[posted_date]) {
            url.searchParams.set('filters[posted_date]', dateMap[posted_date]);
        }
    }
    
    return url.href;
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
                'Cache-Control': 'no-cache',
                ...(cookieHeader && { 'Cookie': cookieHeader }),
            };
        },
    ],

    requestHandler: async ({ $, request, crawler }) => {
        const { userData } = request;

        log.info(`Processing ${userData.label} page ${userData.page || 1}`, { url: request.url });

        if (isBlocked($)) {
            log.error('🚫 Page blocked - stopping');
            throw new Error('Blocked by website');
        }

        if (userData.label === 'LIST') {
            const pageNum = userData.page || 1;
            state.pagesScraped++;

            // Extract job links from the page
            const jobLinks = [];
            const processedIds = new Set();

            $('a[href*="/jobs/"]').each((i, el) => {
                const $link = $(el);
                const href = $link.attr('href');
                
                if (!href || !href.match(/\/jobs\/[^/]+-\d+$/)) return;
                if (href.includes('/search') || href.includes('/category')) return;
                
                const jobId = extractJobId(href);
                if (!jobId || seenJobIds.has(jobId) || processedIds.has(jobId)) return;
                
                processedIds.add(jobId);

                // Find the job card container
                const $container = $link.closest('div, li, article, .job-item, .job-card');
                
                let title = $link.text().trim();
                if (!title || title.length < 3) {
                    title = $container.find('h2, h3, h4, .job-title').first().text().trim();
                }
                if (!title || title.length < 3) return;

                const company = $container.find('.company-name, [class*="company"]').first().text().trim() || 'Not specified';
                const jobLocation = $container.find('.location, [class*="location"]').first().text().trim() || 'Not specified';
                const fullUrl = validateAndNormalizeUrl(href, BASE_URL);

                if (!fullUrl) return;

                jobLinks.push({
                    id: jobId,
                    url: fullUrl,
                    title,
                    company,
                    location: jobLocation
                });
            });

            log.info(`Found ${jobLinks.length} job links on page ${pageNum}`);

            // Process job links
            let processedThisPage = 0;
            for (const job of jobLinks) {
                if (maxJobs && state.jobsScraped >= maxJobs) {
                    log.info(`Reached maxJobs limit (${maxJobs})`);
                    break;
                }

                if (seenJobIds.has(job.id)) continue;
                seenJobIds.add(job.id);

                const jobData = {
                    title: job.title,
                    company: job.company,
                    location: job.location,
                    date_posted: 'Not specified', // We'll try to get this from detail page
                    url: job.url,
                    page: pageNum
                };

                if (collectDetails) {
                    await crawler.addRequests([{ 
                        url: job.url,
                        userData: { 
                            label: 'DETAIL', 
                            jobData,
                            page: pageNum
                        },
                    }]);
                } else {
                    await Dataset.pushData(jobData);
                }
                
                state.jobsScraped++;
                processedThisPage++;
            }

            log.info(`Page ${pageNum} completed: ${processedThisPage} jobs processed`);

            // Check if we should continue to next page
            const shouldContinue = 
                processedThisPage > 0 &&
                (!maxJobs || state.jobsScraped < maxJobs) &&
                (!maxPages || state.pagesScraped < maxPages);

            if (shouldContinue) {
                // Build next page URL
                const nextPage = pageNum + 1;
                const currentUrl = new URL(request.url);
                currentUrl.searchParams.set('page', nextPage.toString());
                const nextUrl = currentUrl.href;

                log.info(`Adding next page: ${nextUrl}`);

                await crawler.addRequests([{
                    url: nextUrl,
                    userData: { 
                        label: 'LIST', 
                        page: nextPage
                    },
                }]);
            } else {
                log.info('Stopping pagination', {
                    totalJobs: state.jobsScraped,
                    totalPages: state.pagesScraped
                });
            }

        } else if (userData.label === 'DETAIL') {
            try {
                const description = cleanDescription($);
                
                const detailData = {
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
                };

                await Dataset.pushData(detailData);
                
            } catch (e) {
                log.error(`Failed to extract details: ${e.message}`);
                // Push basic job data even if details fail
                await Dataset.pushData({
                    ...userData.jobData,
                    error: 'Failed to extract details'
                });
            }
        }
    },

    failedRequestHandler: async ({ request }) => {
        log.warning(`Request failed: ${request.url}`, {
            errors: request.errorMessages,
        });
    },
});

// Start with the first page
const startUrl = buildStartUrl();
log.info('Starting scraper', { startUrl });

await crawler.run([{
    url: startUrl,
    userData: { label: 'LIST', page: 1 },
}]);

// Save state
state.seenJobIds = Array.from(seenJobIds);

log.info('Scraping complete!', {
    totalJobs: state.jobsScraped,
    totalPages: state.pagesScraped,
});

await Actor.exit();