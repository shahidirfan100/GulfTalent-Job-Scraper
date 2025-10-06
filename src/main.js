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
    if (!url) return null;
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

    // Remove unwanted bits
    ['.header-ribbon', '.row.space-bottom-sm', '.space-bottom-sm',
     'h4:contains("About the Company")', '.btn', '.btn-primary',
     '[class*="apply"]', '[data-cy*="apply"]'].forEach(sel => {
        $desc.find(sel).remove();
    });
    $desc.find('h4:contains("About the Company")').nextAll().remove();

    let html = '';
    let text = '';
    $desc.find('p').each((i, p) => {
        const t = $(p).text().trim();
        if (t) {
            html += $.html(p);
            text += t + '\n\n';
        }
    });
    if (!text.trim()) {
        text = $desc.text().trim();
        html = $desc.html()?.trim() || null;
    }
    text = text.replace(/\s+/g, ' ').replace(/\n\s*\n/g, '\n\n').trim();
    html = html.replace(/>\s+</g, '><').trim();

    return { html: html || null, text: text || null };
}

const cookieHeader = cookies ? cookies.trim() : '';

const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConfig,
    maxRequestRetries: CRAWLER_CONFIG.maxRequestRetries,
    maxConcurrency: CRAWLER_CONFIG.maxConcurrency,
    requestHandlerTimeoutSecs: CRAWLER_CONFIG.requestHandlerTimeoutSecs,

    preNavigationHooks: [
        async ({ request }) => {
            // Anti-blocking headers
            request.headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/html, */*; q=0.01',
                'X-Requested-With': 'XMLHttpRequest',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': BASE_URL + '/jobs/search',
                ...(cookieHeader && { 'Cookie': cookieHeader }),
            };
        },
    ],

    requestHandler: async ({ $, request, crawler }) => {
        const { userData } = request;

        if (userData.label === 'LIST') {
            const pageNum = userData.page || 1;
            log.info(`📄 LIST — page ${pageNum}`, { url: request.url });
            if (isBlocked($)) {
                log.error('❌ BLOCKED on LIST', { url: request.url });
                return;
            }
            state.pagesScraped++;

            let jobs = [];
            let totalAvailable = 0;
            let parsedJson = null;

            const bodyText = $.root().text() || $.text();
            // Try JSON parse
            try {
                parsedJson = JSON.parse(bodyText);
            } catch (e) {
                // Not JSON — fallback
                log.warning('⚠️ Response not valid JSON, fallback to HTML parsing');
            }

            if (parsedJson) {
                if (parsedJson.results && Array.isArray(parsedJson.results.data)) {
                    jobs = parsedJson.results.data;
                    totalAvailable = parsedJson.results.total || 0;
                    log.info(`✅ JSON parsed: ${jobs.length} jobs, totalAvailable = ${totalAvailable}`);
                } else if (Array.isArray(parsedJson.data)) {
                    jobs = parsedJson.data;
                    totalAvailable = parsedJson.total || 0;
                    log.info(`✅ Alt JSON parsed: ${jobs.length} jobs, total = ${totalAvailable}`);
                } else {
                    log.warning('JSON is structured differently', { keys: Object.keys(parsedJson) });
                }
            } else {
                // HTML fallback: look for embedded JSON in <script>
                const scriptContent = $('script:contains("facetedSearchResultsValue")').html() || '';
                const jsonMatch = scriptContent.match(/facetedSearchResultsValue['"]\s*,\s*({[\s\S]*?})\s*\)/);
                if (jsonMatch && jsonMatch[1]) {
                    try {
                        const parsed = JSON.parse(jsonMatch[1]);
                        jobs = parsed.results?.data || [];
                        totalAvailable = parsed.results?.total || 0;
                        log.info(`Fallback JSON from script: ${jobs.length} jobs`);
                    } catch (err) {
                        log.warning('Fallback JSON parse failed', { message: err.message });
                    }
                }
                if (jobs.length === 0) {
                    // Final fallback: HTML link extraction
                    $('a[href*="/jobs/"]').each((i, el) => {
                        const href = $(el).attr('href');
                        const full = validateAndNormalizeUrl(href, BASE_URL);
                        const id = extractJobId(full);
                        if (full && id && !seenJobIds.has(id)) {
                            jobs.push({ link: full });
                        }
                    });
                    log.info(`HTML fallback link parsing: got ${jobs.length} job links`);
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
                    title: job.title || job.job_title || 'Not Specified',
                    company: job.company_name || job.employer_name || 'Not Specified',
                    location: job.location || job.city || 'Not Specified',
                    date_posted: job.posted_date_ts
                              ? new Date(job.posted_date_ts * 1000).toISOString()
                              : job.posted_date || 'Not Specified',
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

            log.info(`✅ Page ${pageNum} added ${added} jobs (total so far: ${state.jobsScraped})`);

            // Decide next page
            const moreByCount = totalAvailable > 0 ? (state.jobsScraped < totalAvailable) : (added > 0);
            const canContinue = (!maxPages || pageNum < maxPages) && (!maxJobs || state.jobsScraped < maxJobs);

            if (moreByCount && canContinue) {
                const nextPage = pageNum + 1;
                const urlObj = new URL(request.url);
                const params = urlObj.searchParams;
                params.set('page', nextPage.toString());

                // Build next URL — prefer AJAX endpoint
                const nextUrl = `${BASE_URL}/ajax/jobs/search?${params.toString()}`;
                log.info(`➡️ Enqueue LIST page ${nextPage}: ${nextUrl}`);
                await crawler.addRequests([{ url: nextUrl, userData: { label: 'LIST', page: nextPage } }]);
            } else {
                log.info('🏁 Pagination ended — no further pages');
            }
        }
        else if (userData.label === 'DETAIL') {
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
                log.error(`❌ DETAIL parse error: ${e.message}`, { url: request.url });
                await Dataset.pushData({ ...userData.jobData, error: 'DETAIL parse failed' });
            }
        }
    },

    failedRequestHandler: async ({ request }) => {
        log.warning(`❌ Request failed: ${request.url}`);
    },
});

// Build start URL preserving filters
let startUrl;
if (inputStartUrl) {
    const u = new URL(inputStartUrl);
    u.searchParams.set('page', '1');
    startUrl = u.href;
} else {
    const u = new URL('/jobs/search', BASE_URL);
    if (keyword) u.searchParams.set('search_keyword', keyword);
    if (location) u.searchParams.set('city', location);
    u.searchParams.set('page', '1');
    startUrl = u.href;
}

log.info('🚀 Starting scraper', { startUrl });
await crawler.run([{ url: startUrl, userData: { label: 'LIST', page: 1 } }]);

state.seenJobIds = Array.from(seenJobIds);
log.info('✅ Done! jobs:', state.jobsScraped, 'pages:', state.pagesScraped);
await Actor.exit();
