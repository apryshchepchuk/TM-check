// scripts/tm_bulk_similarity_check.mjs

import fs from 'node:fs/promises';
import path from 'node:path';

const API_BASE = 'https://sis.nipo.gov.ua/api/v1/open-data/';
const OUT_DIR = 'out';

const DEFAULTS = {
  from: '',
  to: '',
  dateMode: 'app_date',
  threshold: 80,
  maxPages: 0,
  requestDelayMs: 1150,
  watchlistFile: 'watchlist.txt',
  appClassFilter: '3,5,10',
  includeRaw: false,
};

const MIN_COMPARE_CHARS = 4;

const GLOBAL_WEAK_TOKENS = new Set([
  'a', 'an', 'and', 'or', 'of', 'the', 'for', 'to', 'in', 'on', 'with',
  'c', 'k', 'm', 'd', 'de', 'du', 'la', 'le', 'el', 'di', 'da', 'do', 'pro',
  'та', 'і', 'й', 'в', 'у', 'з', 'із', 'для', 'на', 'до', 'від', 'при',
  'bio', 'біо', 'био', 'med', 'мед', 'pharma', 'фарма', 'farm',
  'forte', 'форте', 'plus', 'плюс', 'ultra', 'ультра', 'extra', 'екстра',
  'new', 'нова', 'новий', 'classic', 'класик', 'original', 'оригінал',
  'group', 'груп', 'company', 'компанія', 'clinic', 'клініка', 'lab', 'labs',
  'academy', 'school', 'club', 'media', 'marketing', 'family', 'home',
  '*', '®', '™',
]);

const CYR_TO_LAT = {
  а: 'a', б: 'b', в: 'v', г: 'h', ґ: 'g', д: 'd', е: 'e', є: 'ie', ж: 'zh', з: 'z',
  и: 'y', і: 'i', ї: 'i', й: 'i', к: 'k', л: 'l', м: 'm', н: 'n', о: 'o', п: 'p',
  р: 'r', с: 's', т: 't', у: 'u', ф: 'f', х: 'kh', ц: 'ts', ч: 'ch', ш: 'sh',
  щ: 'shch', ь: '', ю: 'iu', я: 'ia', ы: 'y', э: 'e', ё: 'e', ъ: '',
};

function parseBool(value, defaultValue = false) {
  if (value === undefined || value === null || value === '') return defaultValue;
  const s = String(value).trim().toLowerCase();
  if (['true', '1', 'yes', 'y', 'так'].includes(s)) return true;
  if (['false', '0', 'no', 'n', 'ні'].includes(s)) return false;
  throw new Error(`Invalid boolean value: ${value}`);
}

function parseArgs(argv) {
  const args = { ...DEFAULTS };

  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    const val = argv[i + 1];
    if (!key.startsWith('--')) continue;
    i += 1;

    switch (key) {
      case '--from': args.from = String(val || '').trim(); break;
      case '--to': args.to = String(val || '').trim(); break;
      case '--date-mode': args.dateMode = String(val || '').trim(); break;
      case '--threshold': args.threshold = Number(val); break;
      case '--max-pages': args.maxPages = Number(val); break;
      case '--request-delay-ms': args.requestDelayMs = Number(val); break;
      case '--watchlist-file': args.watchlistFile = String(val || '').trim(); break;
      case '--app-class-filter': args.appClassFilter = String(val || '').trim(); break;
      case '--include-raw': args.includeRaw = parseBool(val, false); break;
      default: throw new Error(`Unknown argument: ${key}`);
    }
  }

  if (!/^\d{2}\.\d{2}\.\d{4}$/.test(args.from)) {
    throw new Error('Invalid --from. Expected format дд.мм.рррр, e.g. 01.01.2026');
  }
  if (!/^\d{2}\.\d{2}\.\d{4}$/.test(args.to)) {
    throw new Error('Invalid --to. Expected format дд.мм.рррр, e.g. 29.04.2026');
  }
  if (!['app_date', 'last_update'].includes(args.dateMode)) {
    throw new Error('--date-mode must be app_date or last_update');
  }
  if (!Number.isFinite(args.threshold) || args.threshold < 0 || args.threshold > 100) {
    throw new Error('--threshold must be a number from 0 to 100');
  }
  if (!Number.isFinite(args.maxPages) || args.maxPages < 0) {
    throw new Error('--max-pages must be 0 or a positive number');
  }
  if (!Number.isFinite(args.requestDelayMs) || args.requestDelayMs < 0) {
    throw new Error('--request-delay-ms must be 0 or a positive number');
  }
  if (!args.watchlistFile) {
    throw new Error('--watchlist-file is required');
  }

  args.classFilterSet = parseClassFilter(args.appClassFilter);
  return args;
}

function parseClassFilter(value) {
  const s = String(value || '').trim();
  if (!s || s.toLowerCase() === 'all' || s === '*') return new Set();
  const classes = s
    .split(/[;,|\s]+/)
    .map((x) => x.replace(/\D+/g, ''))
    .filter(Boolean);
  return new Set(classes);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildInitialUrl(args) {
  const url = new URL(API_BASE);
  url.searchParams.set('obj_state', '1'); // заявки
  url.searchParams.set('obj_type', '4'); // торговельні марки

  if (args.dateMode === 'app_date') {
    url.searchParams.set('app_date_from', args.from);
    url.searchParams.set('app_date_to', args.to);
  } else {
    url.searchParams.set('last_update_from', args.from);
    url.searchParams.set('last_update_to', args.to);
  }

  return url.toString();
}

async function fetchJson(url, attempt = 1) {
  const res = await fetch(url, {
    headers: {
      accept: 'application/json',
      'user-agent': 'tm-bulk-similarity-check/0.3 GitHub Actions',
    },
  });

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    if ((res.status === 429 || res.status >= 500) && attempt < 4) {
      const waitMs = 2500 * attempt;
      console.warn(`HTTP ${res.status}. Retry ${attempt}/3 after ${waitMs} ms`);
      await sleep(waitMs);
      return fetchJson(url, attempt + 1);
    }
    throw new Error(`HTTP ${res.status} for ${url}\n${body.slice(0, 1000)}`);
  }

  return res.json();
}

async function fetchApplications(args) {
  let url = buildInitialUrl(args);
  const all = [];
  let page = 0;
  let totalCount = null;
  let stoppedByMaxPages = false;

  while (url) {
    page += 1;
    if (args.maxPages > 0 && page > args.maxPages) {
      stoppedByMaxPages = true;
      console.log(`Stopped by max_pages=${args.maxPages}`);
      break;
    }

    console.log(`Fetching page ${page}: ${url}`);
    const json = await fetchJson(url);
    if (totalCount === null) totalCount = json.count ?? null;

    const results = Array.isArray(json.results) ? json.results : [];
    all.push(...results);
    url = json.next || null;

    if (url) await sleep(args.requestDelayMs);
  }

  return {
    totalCount,
    fetchedCount: all.length,
    fetchedPages: page - (stoppedByMaxPages ? 1 : 0),
    stoppedByMaxPages,
    applications: all,
  };
}

function asArray(value) {
  if (value === null || value === undefined) return [];
  return Array.isArray(value) ? value : [value];
}

function getTextValue(value) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return String(value).trim();
  }
  if (typeof value === 'object') {
    if (typeof value['#text'] === 'string') return value['#text'].trim();
    if (typeof value.text === 'string') return value.text.trim();
    if (typeof value.value === 'string') return value.value.trim();
  }
  return '';
}

function uniqueStrings(items) {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    const s = String(item || '').replace(/\s+/g, ' ').trim();
    if (!s) continue;
    const key = s.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(s);
  }
  return out;
}

function getByPath(obj, parts) {
  let cur = obj;
  for (const part of parts) {
    if (cur === null || cur === undefined) return undefined;
    cur = cur[part];
  }
  return cur;
}

function findValuesByKeyDeep(obj, targetKeys, maxDepth = 12) {
  const target = new Set(targetKeys);
  const values = [];
  const stack = [{ value: obj, depth: 0 }];
  const seen = new WeakSet();

  while (stack.length) {
    const { value, depth } = stack.pop();
    if (value === null || value === undefined || depth > maxDepth) continue;

    if (typeof value === 'object') {
      if (seen.has(value)) continue;
      seen.add(value);

      if (Array.isArray(value)) {
        for (const item of value) stack.push({ value: item, depth: depth + 1 });
      } else {
        for (const [k, v] of Object.entries(value)) {
          if (target.has(k)) values.push(v);
          stack.push({ value: v, depth: depth + 1 });
        }
      }
    }
  }

  return values;
}

function extractWordElements(item) {
  const data = item?.data || {};
  const direct = getByPath(data, ['WordMarkSpecification', 'MarkSignificantVerbalElement']);

  const fromDirect = asArray(direct).map(getTextValue).filter(Boolean);
  const fromDeep = findValuesByKeyDeep(data, ['MarkSignificantVerbalElement'])
    .flatMap(asArray)
    .map(getTextValue)
    .filter(Boolean);

  return uniqueStrings([...fromDirect, ...fromDeep]);
}

function extractImageUrls(item) {
  const values = findValuesByKeyDeep(item?.data || {}, [
    'MarkImageFilename',
    'ImageFilename',
    'FileName',
    'filename',
  ])
    .flatMap(asArray)
    .map(getTextValue)
    .filter(Boolean)
    .map((v) => {
      if (v.startsWith('http')) return v;
      return `https://sis.nipo.gov.ua${v.startsWith('/') ? '' : '/'}${v}`;
    });

  return uniqueStrings(values);
}

function extractClasses(item) {
  const values = findValuesByKeyDeep(item?.data || {}, [
    'ClassNumber',
    'class_number',
    'ClassNo',
  ])
    .flatMap(asArray)
    .map(getTextValue)
    .filter(Boolean)
    .map((s) => s.replace(/\D+/g, ''))
    .filter(Boolean);

  return uniqueStrings(values).sort((a, b) => Number(a) - Number(b));
}

function extractCurrentStage(item) {
  const stages = asArray(item?.data?.stages);
  const current = stages.find((stage) => stage?.status === 'current' || stage?.status === 'active');
  if (current?.title) return String(current.title).trim();
  const done = stages.find((stage) => stage?.status === 'done');
  if (done?.title) return String(done.title).trim();
  return '';
}

function extractApplicationStatus(item) {
  return String(item?.data?.application_status || '').trim();
}

function extractNameFromAddressBook(addressBook) {
  if (!addressBook || typeof addressBook !== 'object') return [];

  const nameBlock =
    getByPath(addressBook, ['FormattedNameAddress', 'Name'])
    ?? getByPath(addressBook, ['Name'])
    ?? addressBook;

  const values = findValuesByKeyDeep(nameBlock, [
    'FreeFormatNameLine',
    'OrganizationName',
    'LegalEntityName',
    'IndividualName',
    'PersonName',
    'FormattedName',
    'NameLine',
  ], 10)
    .flatMap(asArray)
    .map(getTextValue)
    .filter(Boolean);

  return uniqueStrings(values);
}

function extractPartyNamesFromParty(party, partyType) {
  if (!party || typeof party !== 'object') return [getTextValue(party)].filter(Boolean);

  const addressBookKeys = [
    `${partyType}AddressBook`,
    'ApplicantAddressBook',
    'HolderAddressBook',
    'RepresentativeAddressBook',
    'OwnerAddressBook',
    'AddressBook',
  ];

  const values = [];
  for (const key of addressBookKeys) {
    if (party[key]) values.push(...extractNameFromAddressBook(party[key]));
  }

  if (values.length) return uniqueStrings(values);

  const fallback = findValuesByKeyDeep(party, [
    'FreeFormatNameLine',
    'OrganizationName',
    'LegalEntityName',
    'IndividualName',
    'PersonName',
  ], 8)
    .flatMap(asArray)
    .map(getTextValue)
    .filter(Boolean);

  return uniqueStrings(fallback);
}

function extractApplicants(item) {
  const data = item?.data || {};
  const applicantDetails = getByPath(data, ['ApplicantDetails']);
  const directApplicants = getByPath(applicantDetails, ['Applicant']);
  const names = [];

  for (const applicant of asArray(directApplicants)) {
    names.push(...extractPartyNamesFromParty(applicant, 'Applicant'));
  }

  if (!names.length && applicantDetails) {
    const deepApplicants = findValuesByKeyDeep(applicantDetails, ['Applicant'], 8).flatMap(asArray);
    for (const applicant of deepApplicants) names.push(...extractPartyNamesFromParty(applicant, 'Applicant'));
  }

  return uniqueStrings(names).slice(0, 10);
}

function formatDate(value) {
  const s = String(value || '').trim();
  if (!s) return '';
  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (iso) return `${iso[3]}.${iso[2]}.${iso[1]}`;
  return s;
}

function transliterateCyrToLat(input) {
  return String(input || '')
    .toLowerCase()
    .split('')
    .map((ch) => CYR_TO_LAT[ch] ?? ch)
    .join('');
}

function normalizeText(input) {
  return String(input || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[’'`ʼ]/g, '')
    .replace(/&/g, ' and ')
    .replace(/[*®™©]/g, ' ')
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function compactText(input) {
  return normalizeText(input).replace(/\s+/g, '');
}

function tokens(input) {
  return normalizeText(input).split(' ').filter(Boolean);
}

function sortedTokens(input) {
  return tokens(input).sort().join(' ');
}

function textCharLength(input) {
  return normalizeText(input).replace(/\s+/g, '').length;
}

function isTooShortForCompare(input) {
  const norm = normalizeText(input);
  if (!norm) return true;
  if (norm === '*') return true;
  if (textCharLength(norm) < MIN_COMPARE_CHARS) return true;

  const t = tokens(norm);
  if (t.length === 1 && t[0].length < MIN_COMPARE_CHARS) return true;
  if (t.length === 1 && GLOBAL_WEAK_TOKENS.has(t[0])) return true;

  return false;
}

function getComparableTexts(items) {
  return uniqueStrings(items).filter((x) => !isTooShortForCompare(x));
}

function normalizeApplication(item) {
  const wordElements = extractWordElements(item);
  const comparableWordElements = getComparableTexts(wordElements);
  const imageUrls = extractImageUrls(item);

  return {
    app_number: String(item?.app_number || '').trim(),
    app_date: formatDate(item?.app_date),
    last_update: item?.last_update || '',
    obj_state: item?.obj_state || '',
    obj_type: item?.obj_type || '',
    application_status: extractApplicationStatus(item),
    current_stage: extractCurrentStage(item),
    word_elements: wordElements,
    comparable_word_elements: comparableWordElements,
    word_text: wordElements.join(' | '),
    classes: extractClasses(item),
    applicants: extractApplicants(item),
    image_urls: imageUrls,
    has_word: wordElements.length > 0,
    has_comparable_word: comparableWordElements.length > 0,
    has_image: imageUrls.length > 0,
  };
}

async function loadWatchlist(filePath) {
  const raw = await fs.readFile(filePath, 'utf8');
  const lines = raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith('#'));

  const seen = new Set();
  const items = [];

  for (const line of lines) {
    const name = line.replace(/\s+/g, ' ').trim();
    if (!name || isTooShortForCompare(name)) continue;
    const key = normalizeText(name);
    if (seen.has(key)) continue;
    seen.add(key);

    const variants = uniqueStrings([
      name,
      normalizeText(name),
      compactText(name),
      transliterateCyrToLat(name),
      compactText(transliterateCyrToLat(name)),
    ]).filter((x) => !isTooShortForCompare(x));

    items.push({
      id: `WATCH_${String(items.length + 1).padStart(5, '0')}`,
      name,
      normalized_name: normalizeText(name),
      compact_name: compactText(name),
      variants,
    });
  }

  return items;
}

function classOverlap(aClasses, filterSet) {
  if (!filterSet || filterSet.size === 0) return [];
  const a = new Set((aClasses || []).map(String).filter(Boolean));
  return [...a].filter((x) => filterSet.has(x)).sort((x, y) => Number(x) - Number(y));
}

function passesClassFilter(app, filterSet) {
  if (!filterSet || filterSet.size === 0) return true;
  return classOverlap(app.classes, filterSet).length > 0;
}

function levenshtein(a, b) {
  if (a === b) return 0;
  if (!a) return b.length;
  if (!b) return a.length;

  const prev = Array.from({ length: b.length + 1 }, (_, i) => i);
  const curr = Array.from({ length: b.length + 1 }, () => 0);

  for (let i = 1; i <= a.length; i += 1) {
    curr[0] = i;
    for (let j = 1; j <= b.length; j += 1) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      curr[j] = Math.min(curr[j - 1] + 1, prev[j] + 1, prev[j - 1] + cost);
    }
    for (let j = 0; j <= b.length; j += 1) prev[j] = curr[j];
  }

  return prev[b.length];
}

function ratioRaw(a, b) {
  if (!a || !b) return 0;
  if (a === b) return 100;
  const dist = levenshtein(a, b);
  return Math.max(0, Math.round((1 - dist / Math.max(a.length, b.length)) * 100));
}

function ratio(a, b) {
  return ratioRaw(normalizeText(a), normalizeText(b));
}

function partialRatio(a, b) {
  const x = normalizeText(a);
  const y = normalizeText(b);
  if (!x || !y) return 0;
  if (x === y) return 100;

  const shorter = x.length <= y.length ? x : y;
  const longer = x.length <= y.length ? y : x;
  if (isTooShortForCompare(shorter)) return 0;

  if (longer.includes(shorter)) {
    const raw = Math.round((shorter.length / longer.length) * 100);
    return Math.min(100, Math.max(70, raw));
  }

  let best = 0;
  const window = shorter.length;
  for (let i = 0; i <= longer.length - window; i += 1) {
    best = Math.max(best, ratioRaw(shorter, longer.slice(i, i + window)));
    if (best === 100) break;
  }
  return best;
}

function tokenSortRatio(a, b) {
  return ratio(sortedTokens(a), sortedTokens(b));
}

function isWeakSingleToken(text) {
  const t = tokens(text);
  return t.length === 1 && GLOBAL_WEAK_TOKENS.has(t[0]);
}

function capScore(score, method, appText, watchText) {
  const appNorm = normalizeText(appText);
  const watchNorm = normalizeText(watchText);
  const appTokens = tokens(appNorm);
  const watchTokens = tokens(watchNorm);

  if (isTooShortForCompare(appNorm) || isTooShortForCompare(watchNorm)) {
    return { score: 0, method: `${method}_ignored_short` };
  }

  // Слабкий однословний збіг типу forte/pro/bio не має давати високий score.
  if (isWeakSingleToken(appNorm) || isWeakSingleToken(watchNorm)) {
    return { score: Math.min(score, 55), method: `${method}_weak_token_cap` };
  }

  // Якщо заявка містить всю watch-назву + додаткове слово, це сильний сигнал.
  if (method === 'watch_contained_in_application') {
    return { score, method };
  }

  // Якщо заявка дала лише один фрагмент із багатослівної watch-назви — обмежуємо.
  if (appTokens.length === 1 && watchTokens.length > 1) {
    if (appTokens[0].length < 5) return { score: Math.min(score, 60), method: `${method}_short_fragment_cap` };
    return { score: Math.min(score, 78), method: `${method}_single_fragment_cap` };
  }

  // Короткі однословні збіги знижуємо.
  if (appTokens.length === 1 && watchTokens.length === 1 && Math.min(appTokens[0].length, watchTokens[0].length) < 5) {
    return { score: Math.min(score, 75), method: `${method}_short_word_cap` };
  }

  return { score, method };
}

function scoreTexts(applicationText, watchText) {
  const appVariants = uniqueStrings([
    applicationText,
    normalizeText(applicationText),
    compactText(applicationText),
    transliterateCyrToLat(applicationText),
    compactText(transliterateCyrToLat(applicationText)),
  ]).map(normalizeText).filter(Boolean);

  const watchVariants = uniqueStrings([
    watchText,
    normalizeText(watchText),
    compactText(watchText),
    transliterateCyrToLat(watchText),
    compactText(transliterateCyrToLat(watchText)),
  ]).map(normalizeText).filter(Boolean);

  let best = { score: 0, method: '', comparedA: '', comparedB: '' };

  for (const app of appVariants) {
    for (const watch of watchVariants) {
      if (isTooShortForCompare(app) || isTooShortForCompare(watch)) continue;

      const candidates = [];

      if (app === watch) {
        candidates.push({ score: 100, method: app.includes(' ') || watch.includes(' ') ? 'exact' : 'exact_or_compact' });
      }

      if (compactText(app) && compactText(app) === compactText(watch)) {
        candidates.push({ score: 100, method: 'exact_no_space' });
      }

      if (app.includes(watch) && textCharLength(watch) >= MIN_COMPARE_CHARS) {
        candidates.push({ score: 95, method: 'watch_contained_in_application' });
      }

      if (watch.includes(app) && textCharLength(app) >= MIN_COMPARE_CHARS) {
        candidates.push({ score: partialRatio(app, watch), method: 'application_fragment_of_watch' });
      }

      candidates.push({ score: ratio(app, watch), method: 'levenshtein' });
      candidates.push({ score: partialRatio(app, watch), method: 'partial' });
      candidates.push({ score: tokenSortRatio(app, watch), method: 'token_sort' });

      for (const candidate of candidates) {
        const capped = capScore(candidate.score, candidate.method, app, watch);
        if (capped.score > best.score) {
          best = {
            score: capped.score,
            method: capped.method,
            comparedA: app,
            comparedB: watch,
          };
        }
      }
    }
  }

  return best;
}

function classifyRisk(score) {
  if (score >= 95) return 'HIGH';
  if (score >= 90) return 'MEDIUM_HIGH';
  if (score >= 80) return 'MEDIUM';
  if (score >= 75) return 'LOW';
  return 'VERY_LOW';
}

function buildReason(match) {
  const parts = [
    `score=${match.score}`,
    `method=${match.method}`,
  ];

  if (match.matched_filter_classes) {
    parts.push(`application class filter overlap=${match.matched_filter_classes}`);
  }

  return parts.join('; ');
}

function compareApplications(applications, watchlist, args) {
  const matches = [];
  let comparisonCount = 0;

  for (const app of applications) {
    if (!app.comparable_word_elements.length) continue;

    for (const watched of watchlist) {
      let best = null;

      for (const appText of app.comparable_word_elements) {
        for (const variant of watched.variants) {
          comparisonCount += 1;
          const s = scoreTexts(appText, variant);
          if (!best || s.score > best.score) {
            best = {
              ...s,
              application_text: appText,
              watch_variant: variant,
            };
          }
        }
      }

      if (!best || best.score < args.threshold) continue;

      const matchedFilterClasses = classOverlap(app.classes, args.classFilterSet);
      const row = {
        risk: classifyRisk(best.score),
        score: best.score,
        method: best.method,
        watch_id: watched.id,
        watch_name: watched.name,
        watch_variant: best.watch_variant,
        application_text: best.application_text,
        app_all_word_elements: app.word_elements.join(' | '),
        app_number: app.app_number,
        app_date: app.app_date,
        last_update: app.last_update,
        app_classes: app.classes.join(';'),
        matched_filter_classes: matchedFilterClasses.join(';'),
        applicants: app.applicants.join(' | '),
        current_stage: app.current_stage,
        application_status: app.application_status,
        image_urls: app.image_urls.join(' | '),
        compared_application_normalized: best.comparedA,
        compared_watch_normalized: best.comparedB,
      };

      row.reason = buildReason(row);
      matches.push(row);
    }
  }

  const riskOrder = { HIGH: 5, MEDIUM_HIGH: 4, MEDIUM: 3, LOW: 2, VERY_LOW: 1 };

  matches.sort((a, b) =>
    (riskOrder[b.risk] ?? 0) - (riskOrder[a.risk] ?? 0)
    || b.score - a.score
    || a.watch_name.localeCompare(b.watch_name)
    || a.app_number.localeCompare(b.app_number));

  return { matches, comparisonCount };
}

function csvEscape(value) {
  const s = Array.isArray(value) ? value.join(' | ') : String(value ?? '');
  if (/[",\n\r;]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function toCsv(rows, headers) {
  const lines = [headers.join(';')];
  for (const row of rows) lines.push(headers.map((h) => csvEscape(row[h])).join(';'));
  return lines.join('\n');
}

function countBy(items, keyFn) {
  const map = new Map();
  for (const item of items) {
    const key = keyFn(item) || '(empty)';
    map.set(key, (map.get(key) || 0) + 1);
  }
  return [...map.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
}

function escapeMd(value) {
  return String(value ?? '').replace(/\|/g, '\\|').replace(/\n/g, ' ');
}

function buildSummary({ args, fetchMeta, watchlist, normalized, filtered, skippedByClass, pending, matches, comparisonCount }) {
  const total = normalized.length;
  const withWords = normalized.filter((x) => x.has_word).length;
  const withComparableWords = normalized.filter((x) => x.has_comparable_word).length;
  const withImages = normalized.filter((x) => x.has_image).length;
  const riskStats = countBy(matches, (m) => m.risk);
  const methodStats = countBy(matches, (m) => m.method);

  const classFilterText = args.classFilterSet.size
    ? [...args.classFilterSet].sort((a, b) => Number(a) - Number(b)).join(',')
    : 'all';

  const lines = [];
  lines.push('# TM Bulk Similarity Check — summary');
  lines.push('');
  lines.push(`- Date mode: **${args.dateMode}**`);
  lines.push(`- Date range: **${args.from} — ${args.to}**`);
  lines.push(`- Watchlist file: **${args.watchlistFile}**`);
  lines.push(`- Watchlist names: **${watchlist.length}**`);
  lines.push(`- Threshold: **${args.threshold}**`);
  lines.push(`- Application class filter: **${classFilterText}**`);
  lines.push(`- API total count: **${fetchMeta.totalCount ?? 'n/a'}**`);
  lines.push(`- Fetched pages: **${fetchMeta.fetchedPages}**`);
  lines.push(`- Fetched applications: **${fetchMeta.fetchedCount}**`);
  if (fetchMeta.stoppedByMaxPages) lines.push('- Stopped by max_pages: **yes**');
  lines.push('');
  lines.push(`- Applications with word elements: **${withWords} / ${total}**`);
  lines.push(`- Applications with comparable word elements: **${withComparableWords} / ${total}**`);
  lines.push(`- Applications with images: **${withImages} / ${total}**`);
  lines.push(`- Applications after class filter: **${filtered.length} / ${total}**`);
  lines.push(`- Applications skipped by class filter: **${skippedByClass.length} / ${total}**`);
  lines.push(`- Pending without comparable word elements after class filter: **${pending.length}**`);
  lines.push(`- Fuzzy comparisons performed: **${comparisonCount}**`);
  lines.push(`- Matches: **${matches.length}**`);
  lines.push('');

  lines.push('## Risk breakdown');
  lines.push('');
  lines.push('| Risk | Count |');
  lines.push('|---|---:|');
  for (const [risk, count] of riskStats) lines.push(`| ${escapeMd(risk)} | ${count} |`);
  if (!riskStats.length) lines.push('| none | 0 |');
  lines.push('');

  lines.push('## Method breakdown');
  lines.push('');
  lines.push('| Method | Count |');
  lines.push('|---|---:|');
  for (const [method, count] of methodStats.slice(0, 30)) lines.push(`| ${escapeMd(method)} | ${count} |`);
  if (!methodStats.length) lines.push('| none | 0 |');
  lines.push('');

  if (matches.length) {
    lines.push('## Top matches');
    lines.push('');
    lines.push('| Risk | Score | Watch name | Application text | App no | App date | Classes | Applicant |');
    lines.push('|---|---:|---|---|---|---|---|---|');
    for (const m of matches.slice(0, 100)) {
      lines.push(`| ${escapeMd(m.risk)} | ${m.score} | ${escapeMd(m.watch_name)} | ${escapeMd(m.application_text)} | ${escapeMd(m.app_number)} | ${escapeMd(m.app_date)} | ${escapeMd(m.app_classes)} | ${escapeMd(m.applicants).slice(0, 160)} |`);
    }
    if (matches.length > 100) lines.push(`\nShowing first 100 of ${matches.length} matches.`);
  } else {
    lines.push('No matches found above threshold.');
  }

  return lines.join('\n');
}

async function writeJson(file, data) {
  await fs.writeFile(file, JSON.stringify(data, null, 2), 'utf8');
}

async function main() {
  const args = parseArgs(process.argv);
  await fs.mkdir(OUT_DIR, { recursive: true });

  console.log('Run args:', JSON.stringify({ ...args, classFilterSet: [...args.classFilterSet] }, null, 2));

  console.log('Loading watchlist...');
  const watchlist = await loadWatchlist(args.watchlistFile);
  if (!watchlist.length) throw new Error(`No usable names found in ${args.watchlistFile}`);
  console.log(`Loaded watchlist names: ${watchlist.length}`);

  console.log('Fetching applications...');
  const fetchMeta = await fetchApplications(args);

  console.log('Normalizing applications...');
  const normalized = fetchMeta.applications.map(normalizeApplication);

  const filtered = normalized.filter((app) => passesClassFilter(app, args.classFilterSet));
  const skippedByClass = normalized.filter((app) => !passesClassFilter(app, args.classFilterSet));
  const pending = filtered.filter((app) => !app.has_comparable_word);
  const comparable = filtered.filter((app) => app.has_comparable_word);

  console.log(`Applications after class filter: ${filtered.length}`);
  console.log(`Comparable applications: ${comparable.length}`);
  console.log('Comparing...');

  const { matches, comparisonCount } = compareApplications(comparable, watchlist, args);

  const matchHeaders = [
    'risk',
    'score',
    'method',
    'watch_id',
    'watch_name',
    'watch_variant',
    'application_text',
    'app_all_word_elements',
    'app_number',
    'app_date',
    'last_update',
    'app_classes',
    'matched_filter_classes',
    'applicants',
    'current_stage',
    'application_status',
    'image_urls',
    'compared_application_normalized',
    'compared_watch_normalized',
    'reason',
  ];

  const appHeaders = [
    'app_number',
    'app_date',
    'last_update',
    'application_status',
    'current_stage',
    'classes',
    'has_word',
    'has_comparable_word',
    'word_elements',
    'comparable_word_elements',
    'applicants',
    'has_image',
    'image_urls',
  ];

  await writeJson(path.join(OUT_DIR, 'watchlist_normalized.json'), watchlist);
  await writeJson(path.join(OUT_DIR, 'normalized_applications.json'), normalized);
  await writeJson(path.join(OUT_DIR, 'matches.json'), matches);
  await writeJson(path.join(OUT_DIR, 'pending_without_words.json'), pending);

  if (args.includeRaw) {
    await writeJson(path.join(OUT_DIR, 'raw_applications.json'), fetchMeta.applications);
  }

  await fs.writeFile(path.join(OUT_DIR, 'matches.csv'), toCsv(matches, matchHeaders), 'utf8');
  await fs.writeFile(path.join(OUT_DIR, 'pending_without_words.csv'), toCsv(pending, appHeaders), 'utf8');
  await fs.writeFile(path.join(OUT_DIR, 'skipped_by_class_filter.csv'), toCsv(skippedByClass, appHeaders), 'utf8');
  await fs.writeFile(path.join(OUT_DIR, 'filtered_applications.csv'), toCsv(filtered, appHeaders), 'utf8');

  await fs.writeFile(
    path.join(OUT_DIR, 'summary.md'),
    buildSummary({ args, fetchMeta, watchlist, normalized, filtered, skippedByClass, pending, matches, comparisonCount }),
    'utf8',
  );

  console.log(`Done. Fetched=${fetchMeta.fetchedCount}, filtered=${filtered.length}, matches=${matches.length}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
