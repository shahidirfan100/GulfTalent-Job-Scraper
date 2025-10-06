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
    } catch {
        return null;
    }
}

function extractJobId(url) {
    const match = url?.match(/\/jobs\/[^/]+-(\d+)/);
    return match ? match[1] : null;
}

function isBlocked($) {
    const title = $('title').text().toLowerCase();
    const bodyStart = $('body').text().substring(0, 300).toLowerCase();
    return ['access denied', 'captcha', 'cloudflare', 'blocked'].some(i => title.includes(i) || bodyStart.includes(i));
}

function extractMetadata($, labelText) {
    try {
        const element = $(`span[style*="color: #6c757d"]:contains("${labelText}")`).parent();
        const value = element.find('span').last().text().trim();
        return (value && value !== 'Not Specified') ? value : null;
    } catch {
        return null;
    }
}

function cleanDescription($) {
    const container = $('.job-details, .job-description, [class*="job-content"]').first();
    if (!container.length) return { html: null, text: null };
    const $desc = container.clone();
    ['.header-ribbon', '.btn', '.apply', 'h4:contains("About the Company")'].forEach(sel => $desc.find(sel).remove());
    return {
        html: $desc.html()?.trim() || null,
        text: $desc.text().replace(/\s+/g, ' ').trim(),
    };
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
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36',
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
            let json = null;

            // Try JSON parse
            const bodyText = $.root().text() || $.text();
            try {
                json = JSON.parse(bodyText);
                if (json?.results?.data) {
                    jobs = json.results.data;
                    totalAvailable = json.results.total || 0;
                    log.info(`✅ Parsed JSON with ${jobs.length} jobs`);
                } else if (json?.data) {
                    jobs = json.data;
                    totalAvailable = json.total || 0;
                    log.info(`✅ Parsed alt JSON with ${jobs.length} jobs`);
                }
            } catch {
                // Fallback to HTML parsing if not JSON
                log.warning('⚠️ Response not JSON, falling back to HTML parsing');
                const script = $('script:contains("facetedSearchResultsValue")').html() || '';
                const match = script.match(/facetedSearchResultsValue['"]\\s*,\\s*({[\\s\\S]*?})\\s*\\)/);
                if (match && match[1]) {
                    try {
                        const parsed = JSON.parse(match[1]);
                        jobs = parsed.results?.data || [];
                        totalAvailable = parsed.results?.total || 0;
                    } catch {}
                }
                if (jobs.length === 0) {
                    $('a[href*="/jobs/"]').each((i, el) => {
                        const href = $(el).attr('href');
                        const full = validateAndNormalizeUrl(href, BASE_URL);
                        const id = extractJobId(full);
                        if (full && id && !seenJobIds.has(id)) jobs.push({ link: full });
                    });
                    log.info(`HTML fallback found ${jobs.length} job links`);
                }
            }

            // Process jobs
            let added = 0;
            for (const job of jobs) {
                if (maxJobs && state.jobsScraped >= maxJobs) break;
                const jobUrl = validateAndNormalizeUrl(job.link || job.url, BASE_URL);
                const jobId = extractJobId(jobUrl);
                if (!jobId || seenJobIds.has(jobId)) continue;
                seenJobIds.add(jobId);

                const jobData = {
                    title: job.title || job.job_title || 'Not specified',
                    company: job.company_name || job.employer_name || 'Not specified',
                    location: job.location || job.city || 'Not specified',
                    date_posted: job.posted_date_ts
                        ? new Date(job.posted_date_ts * 1000).toISOString()
                        : job.posted_date || 'Not specified',
                    url: jobUrl,
                };

                if (collectDetails) {
                    await crawler.addRequests([{ url: jobUrl, userData: { label: 'DETAIL', jobData } }]);
                } else {
                    await Dataset.pushData({ ...jobData, description_html: null, description_text: null });
                }
                state.jobsScraped++;
                added++;
            }

            log.info(`✅ Page ${pageNum} done, added ${added} jobs`);

            // Pagination: only continue if jobs found
            const more = (totalAvailable && state.jobsScraped < totalAvailable) || added > 0;
            if (more && (!maxPages || pageNum < maxPages)) {
                const nextPage = pageNum + 1;
                const params = new URL(request.url).searchParams;
                params.set('page', nextPage.toString());
                const nextUrl = `${BASE_URL}/jobs/search?${params.toString()}`;
                log.info(`➡️ Next page ${nextPage}: ${nextUrl}`);
                await crawler.addRequests([{ url: nextUrl, userData: { label: 'LIST', page: nextPage } }]);
            } else {
                log.info('🏁 Finished all pages');
            }
        }

        if (userData.label === 'DETAIL') {
            try {
                const desc = cleanDescription($);
                await Dataset.pushData({
                    ...userData.jobData,
                    description_html: desc.html,
                    description_text: desc.text,
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
                log.error(`❌ Detail parse failed: ${e.message}`);
            }
        }
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
log.info('✅ Scraping complete!', { totalJobs: state.jobsScraped, pages: state.pagesScraped });
await Actor.exit();
