import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset, KeyValueStore } from 'crawlee';

const BASE_URL = 'https://www.gulftalent.com';

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

if (maxJobs && maxJobs < 1) throw new Error('maxJobs must be >= 1');
if (maxPages && maxPages < 1) throw new Error('maxPages must be >= 1');

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);
const state = await KeyValueStore.getAutoSavedValue('CRAWLER_STATE', {
    pagesScraped: 0,
    jobsScraped: 0,
    seenJobIds: [],
});

const seenJobIds = new Set(state.seenJobIds);

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

    $desc.find('p').each((i, elem) => {
        const text = $(elem).text().trim();
        if (text) {
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
                'Accept': 'application/json, text/html, */*; q=0.01',
                'X-Requested-With': 'XMLHttpRequest',
                ...(cookieHeader && { 'Cookie': cookieHeader }),
            };
        },
    ],

    requestHandler: async ({ $, request, crawler }) => {
        const { userData } = request;

        if (userData.label === 'LIST') {
            const pageNum = userData.page || 1;
            log.info(`📄 Scraping page ${pageNum}`, { url: request.url });
            if (isBlocked($)) throw new Error('Blocked by website');
            state.pagesScraped++;

            let jobs = [];
            let totalAvailable = 0;

            // ✅ First try AJAX JSON endpoint
            if (request.url.includes('/ajax/jobs/search')) {
                let json;
                try {
                    json = JSON.parse($.root().text() || $.text() || '{}');
                } catch (e) {
                    log.warning('Failed to parse AJAX JSON');
                }

                if (json?.results?.data) {
                    jobs = json.results.data;
                    totalAvailable = json.results.total || 0;
                    log.info(`✅ AJAX response: ${jobs.length} jobs`);
                }
            } else {
                // Fallback to HTML page
                const scriptContent = $('script:contains("facetedSearchResultsValue")').html();
                if (scriptContent) {
                    const jsonMatch = scriptContent.match(/facetedSearchResultsValue['"]\s*,\s*({[\s\S]*?})\s*\)/);
                    if (jsonMatch && jsonMatch[1]) {
                        try {
                            const searchResults = JSON.parse(jsonMatch[1]);
                            jobs = searchResults.results?.data || [];
                            totalAvailable = searchResults.results?.total || 0;
                            log.info(`✅ Found JSON data: ${jobs.length} jobs`);
                        } catch {}
                    }
                }
            }

            // Extract and process jobs
            let addedThisPage = 0;
            for (const job of jobs) {
                if (maxJobs && state.jobsScraped >= maxJobs) return;
                const jobUrl = validateAndNormalizeUrl(job.link, BASE_URL);
                const jobId = extractJobId(jobUrl);
                if (!jobId || seenJobIds.has(jobId)) continue;
                seenJobIds.add(jobId);

                const jobData = {
                    title: job.title || 'Not specified',
                    company: job.company_name || 'Not specified',
                    location: job.location || 'Not specified',
                    date_posted: job.posted_date_ts ? new Date(job.posted_date_ts * 1000).toISOString() : 'Not specified',
                    url: jobUrl,
                };

                if (collectDetails) {
                    await crawler.addRequests([{ url: jobUrl, userData: { label: 'DETAIL', jobData } }]);
                } else {
                    await Dataset.pushData({ ...jobData, description_html: null, description_text: null });
                }

                state.jobsScraped++;
                addedThisPage++;
            }

            log.info(`✅ Page ${pageNum} done: +${addedThisPage} jobs`);

            // 🔁 Pagination fix using AJAX endpoint
            const hasMore = (!maxPages || pageNum < maxPages) && (!maxJobs || state.jobsScraped < maxJobs);
            if (hasMore && addedThisPage > 0) {
                const nextPage = pageNum + 1;
                const params = new URLSearchParams({
                    country: '10111111000000',
                    page: nextPage.toString(),
                    search_keyword: keyword || '',
                    city: location || '',
                    employment_type: '',
                    has_external_application: '',
                    industry: '',
                    seniority: '',
                });
                const nextUrl = `${BASE_URL}/ajax/jobs/search?${params.toString()}`;
                log.info(`➡️ Next page ${nextPage}: ${nextUrl}`);
                await crawler.addRequests([{ url: nextUrl, userData: { label: 'LIST', page: nextPage } }]);
            }
        }

        // Job details
        else if (userData.label === 'DETAIL') {
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
                await Dataset.pushData({ ...userData.jobData, error: 'Failed to extract details' });
            }
        }
    },

    failedRequestHandler: async ({ request }) => {
        log.warning(`❌ Request failed: ${request.url}`);
    },
});

let startUrl;
if (inputStartUrl) {
    const url = new URL(inputStartUrl);
    url.searchParams.set('page', '1');
    startUrl = url.href;
} else {
    const url = new URL('/jobs/search', BASE_URL);
    if (keyword) url.searchParams.set('search_keyword', keyword);
    if (location) url.searchParams.set('city', location);
    url.searchParams.set('page', '1');
    startUrl = url.href;
}

log.info('🚀 Starting scraper', { startUrl });

await crawler.run([{ url: startUrl, userData: { label: 'LIST', page: 1 } }]);

state.seenJobIds = Array.from(seenJobIds);
log.info('✅ Scraping complete!', { totalJobs: state.jobsScraped, totalPages: state.pagesScraped });
await Actor.exit();
