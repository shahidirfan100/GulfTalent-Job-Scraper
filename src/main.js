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

// Helper functions (keep your existing ones)
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

// NEW: Function to extract the critical search configuration from the page
function extractSearchConfig($) {
    try {
        const scriptContent = $('script').filter((i, el) => {
            const content = $(el).html();
            return content && content.includes('facetedSearchResultsValue');
        }).html();

        if (scriptContent) {
            // Extract the search configuration object
            const configMatch = scriptContent.match(/searchConfig\s*=\s*({[\s\S]*?});/);
            if (configMatch && configMatch[1]) {
                const cleanJson = configMatch[1]
                    .replace(/(['"])?([a-zA-Z0-9_]+)(['"])?:/g, '"$2":')
                    .replace(/'/g, '"');
                return JSON.parse(cleanJson);
            }

            // Extract search parameters
            const paramsMatch = scriptContent.match(/searchParams\s*=\s*({[\s\S]*?});/);
            if (paramsMatch && paramsMatch[1]) {
                const cleanJson = paramsMatch[1]
                    .replace(/(['"])?([a-zA-Z0-9_]+)(['"])?:/g, '"$2":')
                    .replace(/'/g, '"');
                return JSON.parse(cleanJson);
            }
        }
        return null;
    } catch (e) {
        log.warning('Failed to extract search config:', e.message);
        return null;
    }
}

// NEW: Function to build the actual API request URL
function buildApiRequestUrl(searchConfig, page = 1) {
    const baseParams = {
        search_keyword: searchConfig.search_keyword || '',
        city: searchConfig.city || '',
        country: searchConfig.country || '',
        category: searchConfig.category || '',
        employment_type: searchConfig.employment_type || '',
        industry: searchConfig.industry || '',
        seniority: searchConfig.seniority || '',
        has_external_application: searchConfig.has_external_application || '',
        page: page.toString()
    };

    // Add date filter if specified
    if (searchConfig.posted_date) {
        baseParams['filters[posted_date]'] = searchConfig.posted_date;
    }

    const queryString = Object.entries(baseParams)
        .filter(([_, value]) => value && value !== '')
        .map(([key, value]) => `${key}=${encodeURIComponent(value)}`)
        .join('&');

    return `${BASE_URL}/jobs/search?${queryString}`;
}

// NEW: Function to extract job data from the embedded JSON
function extractJobsFromEmbeddedJSON($) {
    try {
        const scriptContent = $('script').filter((i, el) => {
            const content = $(el).html();
            return content && content.includes('facetedSearchResultsValue');
        }).html();

        if (scriptContent) {
            // Look for the facetedSearchResultsValue data
            const jsonMatch = scriptContent.match(/facetedSearchResultsValue\s*,\s*({[\s\S]*?})\s*\)/);
            if (jsonMatch && jsonMatch[1]) {
                const cleanJson = jsonMatch[1]
                    .replace(/(['"])?([a-zA-Z0-9_]+)(['"])?:/g, '"$2":')
                    .replace(/'/g, '"')
                    .replace(/,\s*}/g, '}') // Remove trailing commas
                    .replace(/,\s*]/g, ']'); // Remove trailing commas in arrays

                const searchData = JSON.parse(cleanJson);
                const jobs = searchData.results?.data || [];
                const totalJobs = searchData.results?.total || 0;
                const perPage = searchData.results?.per_page || 20;

                log.info(`📊 Extracted ${jobs.length} jobs from embedded JSON (Total: ${totalJobs}, Per page: ${perPage})`);

                return {
                    jobs: jobs.map(job => ({
                        id: job.id?.toString(),
                        title: job.title,
                        company_name: job.company?.name || 'Not specified',
                        location: job.location || 'Not specified',
                        link: `${BASE_URL}${job.url}`,
                        posted_date: job.posted_date || null,
                        employment_type: job.employment_type || null,
                        industry: job.industry || null,
                        seniority: job.seniority || null
                    })),
                    totalJobs,
                    perPage
                };
            }
        }
    } catch (e) {
        log.error('Failed to extract jobs from embedded JSON:', e.message);
    }

    return { jobs: [], totalJobs: 0, perPage: 20 };
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
                'Referer': BASE_URL,
                ...(cookieHeader && { 'Cookie': cookieHeader }),
            };
        },
    ],

    requestHandler: async ({ $, request, crawler }) => {
        const { userData } = request;

        if (userData.label === 'INITIAL_CONFIG') {
            log.info('🔍 Extracting search configuration from initial page');
            
            // Extract search configuration from the page
            const searchConfig = extractSearchConfig($) || {};
            
            // Override with user input
            if (keyword) searchConfig.search_keyword = keyword;
            if (location) searchConfig.city = location;
            if (posted_date && posted_date !== 'anytime') {
                const dateMap = { '24h': 1, '7d': 7, '30d': 30 };
                if (dateMap[posted_date]) searchConfig.posted_date = dateMap[posted_date];
            }

            log.info('📋 Search configuration:', searchConfig);
            
            // Start pagination from page 1
            const firstPageUrl = buildApiRequestUrl(searchConfig, 1);
            
            await crawler.addRequests([{
                url: firstPageUrl,
                userData: { 
                    label: 'PAGINATED_LIST', 
                    page: 1,
                    searchConfig,
                    baseUrl: request.url
                },
            }]);

        } else if (userData.label === 'PAGINATED_LIST') {
            const pageNum = userData.page || 1;
            log.info(`📄 Processing page ${pageNum}`, { url: request.url });

            state.pagesScraped++;

            // Extract jobs from embedded JSON data
            const { jobs, totalJobs, perPage } = extractJobsFromEmbeddedJSON($);
            
            let processedJobs = jobs;

            // Fallback to HTML parsing if JSON extraction failed
            if (processedJobs.length === 0) {
                log.info('🔄 Falling back to HTML parsing');
                processedJobs = [];
                
                const $jobCards = $('[class*="job-item"], .job-card, .job-listing, [data-cy*="job"]');
                
                if ($jobCards.length === 0) {
                    // Try to find job links directly
                    $('a[href*="/jobs/"]').each((i, el) => {
                        const $link = $(el);
                        const href = $link.attr('href');
                        
                        if (href && /\/jobs\/[^/]+-\d+$/.test(href)) {
                            const jobId = extractJobId(href);
                            if (!jobId || seenJobIds.has(jobId)) return;
                            
                            const $container = $link.closest('div, li, article, [class*="job"]');
                            let title = $link.text().trim();
                            if (!title || title.length < 3) {
                                title = $container.find('h2, h3, h4, [class*="title"]').first().text().trim();
                            }
                            if (!title || title.length < 3) return;
                            
                            const fullUrl = validateAndNormalizeUrl(href, BASE_URL);
                            if (!fullUrl) return;
                            
                            processedJobs.push({
                                id: jobId,
                                title: title,
                                company_name: $container.find('[class*="company"], .company-name').first().text().trim() || 'Not specified',
                                location: $container.find('[class*="location"], .location').first().text().trim() || 'Not specified',
                                link: fullUrl,
                            });
                        }
                    });
                } else {
                    // Parse job cards
                    $jobCards.each((i, el) => {
                        const $card = $(el);
                        const $link = $card.find('a[href*="/jobs/"]').first();
                        const href = $link.attr('href');
                        
                        if (!href) return;
                        
                        const jobId = extractJobId(href);
                        if (!jobId || seenJobIds.has(jobId)) return;
                        
                        const title = $link.text().trim() || 
                                    $card.find('h2, h3, h4, [class*="title"]').first().text().trim();
                        if (!title || title.length < 3) return;
                        
                        const fullUrl = validateAndNormalizeUrl(href, BASE_URL);
                        if (!fullUrl) return;
                        
                        processedJobs.push({
                            id: jobId,
                            title: title,
                            company_name: $card.find('[class*="company"], .company-name').first().text().trim() || 'Not specified',
                            location: $card.find('[class*="location"], .location').first().text().trim() || 'Not specified',
                            link: fullUrl,
                        });
                    });
                }
                
                log.info(`📋 Extracted ${processedJobs.length} jobs from HTML`);
            }

            // Process the jobs found on this page
            let processedThisPage = 0;
            for (const job of processedJobs) {
                if (maxJobs && state.jobsScraped >= maxJobs) {
                    log.info(`✋ Reached maxJobs limit (${maxJobs})`);
                    break;
                }

                if (!job.id || seenJobIds.has(job.id)) continue;
                seenJobIds.add(job.id);

                const jobData = {
                    title: job.title || 'Not specified',
                    company: job.company_name || 'Not specified',
                    location: job.location || 'Not specified',
                    date_posted: job.posted_date || 'Not specified',
                    employment_type: job.employment_type || null,
                    industry: job.industry || null,
                    seniority: job.seniority || null,
                    url: job.link,
                };

                if (collectDetails && job.link) {
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

            // Calculate if there are more pages
            const totalPages = totalJobs > 0 ? Math.ceil(totalJobs / perPage) : 0;
            const hasMorePages = totalPages > pageNum;
            const shouldContinue = 
                (processedThisPage > 0 || hasMorePages) &&
                (!maxJobs || state.jobsScraped < maxJobs) &&
                (!maxPages || state.pagesScraped < maxPages);

            if (shouldContinue && hasMorePages) {
                const nextPage = pageNum + 1;
                const nextUrl = buildApiRequestUrl(userData.searchConfig, nextPage);
                
                log.info(`➡️ Adding page ${nextPage}/${totalPages}: ${nextUrl}`);
                
                await crawler.addRequests([{
                    url: nextUrl,
                    userData: { 
                        label: 'PAGINATED_LIST', 
                        page: nextPage,
                        searchConfig: userData.searchConfig,
                        baseUrl: userData.baseUrl
                    },
                }]);
            } else {
                log.info('🏁 Stopping pagination', {
                    reason: !shouldContinue ? 'Limit reached' : 'No more pages',
                    totalJobs: state.jobsScraped,
                    totalPages: state.pagesScraped,
                    detectedTotalPages: totalPages,
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

// Build start URL - we'll use this to extract the initial configuration
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

// Start with initial page to extract configuration
await crawler.run([{
    url: startUrl,
    userData: { label: 'INITIAL_CONFIG' },
}]);

// Save state
state.seenJobIds = Array.from(seenJobIds);

log.info('✅ Scraping complete!', {
    totalJobs: state.jobsScraped,
    totalPages: state.pagesScraped,
});

await Actor.exit();