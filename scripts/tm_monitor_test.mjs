

import fs from 'node:fs/promises';
import path from 'node:path';

const API_BASE = 'https://sis.nipo.gov.ua/api/v1/open-data/';
const OUT_DIR = 'out';
const WATCHLIST_PATH = 'watchlist.json';

const DEFAULTS = {
  from: '',
  to: '',
  dateMode: 'last_update',
  threshold: 75,
  maxPages: 0,
  requestDelayMs: 1100,
  requireClassOverlap: true,
};

// Короткі/службові елементи не мають самостійної розрізняльної сили для fuzzy-match.
const MIN_COMPARE_CHARS = 4;

const GLOBAL_WEAK_TOKENS = new Set([
  'a', 'an', 'and', 'or', 'of', 'the', 'for', 'to', 'in', 'on',
  'c', 'k', 'de', 'du', 'la', 'le', 'el', 'di', 'da',
  'та', 'і', 'й', 'в', 'у', 'з', 'із', 'для', 'на', 'до',
  'bio', 'біо', 'био', 'med', 'мед',
  'forte', 'форте',
]);

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
      case '--from':
        args.from = val;
        break;

      case '--to':
        args.to = val;
        break;

      case '--date-mode':
        args.dateMode = val;
        break;

      case '--threshold':
        args.threshold = Number(val);
        break;

      case '--max-pages':
        args.maxPages = Number(val);
        break;

      case '--request-delay-ms':
        args.requestDelayMs = Number(val);
        break;

      case '--require-class-overlap':
        args.requireClassOverlap = parseBool(val, true);
        break;

      default:
        throw new Error(`Unknown argument: ${key}`);
    }
  }

  if (!/^\d{2}\.\d{2}\.\d{4}$/.test(args.from)) {
    throw new Error('Invalid --from. Expected format дд.мм.рррр, e.g. 23.04.2026');
  }

  if (!/^\d{2}\.\d{2}\.\d{4}$/.test(args.to)) {
    throw new Error('Invalid --to. Expected format дд.мм.рррр, e.g. 24.04.2026');
  }

  if (!['last_update', 'app_date'].includes(args.dateMode)) {
    throw new Error('--date-mode must be last_update or app_date');
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

  return args;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildInitialUrl(args) {
  const url = new URL(API_BASE);

  url.searchParams.set('obj_state', '1'); // заявка
  url.searchParams.set('obj_type', '4'); // ТМ / знаки для товарів і послуг

  if (args.dateMode === 'last_update') {
    url.searchParams.set('last_update_from', args.from);
    url.searchParams.set('last_update_to', args.to);
  } else {
    url.searchParams.set('app_date_from', args.from);
    url.searchParams.set('app_date_to', args.to);
  }

  return url.toString();
}

async function fetchJson(url, attempt = 1) {
  const res = await fetch(url, {
    headers: {
      accept: 'application/json',
      'user-agent': 'tm-monitor-test/0.2 GitHub Actions',
    },
  });

  if (!res.ok) {
    const body = await res.text().catch(() => '');

    if ((res.status === 429 || res.status >= 500) && attempt < 4) {
      const waitMs = 2000 * attempt;
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

    if (totalCount === null) {
      totalCount = json.count ?? null;
    }

    const results = Array.isArray(json.results) ? json.results : [];
    all.push(...results);

    url = json.next || null;

    if (url) {
      await sleep(args.requestDelayMs);
    }
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

  if (typeof value === 'string' || typeof value === 'number') {
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
        for (const item of value) {
          stack.push({ value: item, depth: depth + 1 });
        }
      } else {
        for (const [k, v] of Object.entries(value)) {
          if (target.has(k)) {
            values.push(v);
          }

          stack.push({ value: v, depth: depth + 1 });
        }
      }
    }
  }

  return values;
}

function extractWordElements(item) {
  const data = item?.data || {};

  const direct = getByPath(data, [
    'WordMarkSpecification',
    'MarkSignificantVerbalElement',
  ]);

  const fromDirect = asArray(direct)
    .map(getTextValue)
    .filter(Boolean);

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
  const data = item?.data || {};

  const values = findValuesByKeyDeep(data, [
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
  if (!party || typeof party !== 'object') {
    return [getTextValue(party)].filter(Boolean);
  }

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
    if (party[key]) {
      values.push(...extractNameFromAddressBook(party[key]));
    }
  }

  if (values.length) {
    return uniqueStrings(values);
  }

  // Fallback: шукаємо лише name-поля, але не address-поля, щоб не підхоплювати адреси.
  const directNameBlock = party.Name ?? party.name ?? null;

  if (directNameBlock) {
    values.push(...extractNameFromAddressBook(directNameBlock));
  }

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

  return uniqueStrings([...values, ...fallback]);
}

function extractApplicants(item) {
  const data = item?.data || {};
  const applicantDetails = getByPath(data, ['ApplicantDetails']);
  const directApplicants = getByPath(applicantDetails, ['Applicant']);

  const names = [];

  for (const applicant of asArray(directApplicants)) {
    names.push(...extractPartyNamesFromParty(applicant, 'Applicant'));
  }

  // Fallback для нестандартної структури.
  if (!names.length && applicantDetails) {
    const deepApplicants = findValuesByKeyDeep(applicantDetails, ['Applicant'], 8)
      .flatMap(asArray);

    for (const applicant of deepApplicants) {
      names.push(...extractPartyNamesFromParty(applicant, 'Applicant'));
    }
  }

  return uniqueStrings(names).slice(0, 10);
}

function normalizeAppNumber(value) {
  return String(value || '').trim();
}

function normalizeApplication(item) {
  const wordElements = extractWordElements(item);
  const imageUrls = extractImageUrls(item);

  return {
    app_number: normalizeAppNumber(item?.app_number),
    app_date: item?.app_date || '',
    last_update: item?.last_update || '',
    obj_state: item?.obj_state || '',
    obj_type: item?.obj_type || '',
    obj_state_id: item?.obj_state_id ?? '',
    obj_type_id: item?.obj_type_id ?? '',
    registration_number: item?.registration_number || '',
    registration_date: item?.registration_date || '',
    word_elements: wordElements,
    comparable_word_elements: getComparableTexts(wordElements),
    word_text: wordElements.join(' | '),
    classes: extractClasses(item),
    applicants: extractApplicants(item),
    image_urls: imageUrls,
    has_word: wordElements.length > 0,
    has_comparable_word: getComparableTexts(wordElements).length > 0,
    has_image: imageUrls.length > 0,
  };
}

const CYR_TO_LAT = {
  а: 'a',
  б: 'b',
  в: 'v',
  г: 'h',
  ґ: 'g',
  д: 'd',
  е: 'e',
  є: 'ie',
  ж: 'zh',
  з: 'z',
  и: 'y',
  і: 'i',
  ї: 'i',
  й: 'i',
  к: 'k',
  л: 'l',
  м: 'm',
  н: 'n',
  о: 'o',
  п: 'p',
  р: 'r',
  с: 's',
  т: 't',
  у: 'u',
  ф: 'f',
  х: 'kh',
  ц: 'ts',
  ч: 'ch',
  ш: 'sh',
  щ: 'shch',
  ь: '',
  ю: 'iu',
  я: 'ia',
  ы: 'y',
  э: 'e',
  ё: 'e',
  ъ: '',
};

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
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
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
  if (textCharLength(norm) < MIN_COMPARE_CHARS) return true;

  const t = tokens(norm);

  if (t.length === 1 && t[0].length < MIN_COMPARE_CHARS) return true;
  if (t.length === 1 && GLOBAL_WEAK_TOKENS.has(t[0])) return true;

  return false;
}

function getComparableTexts(items) {
  return uniqueStrings(items).filter((x) => !isTooShortForCompare(x));
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

      curr[j] = Math.min(
        curr[j - 1] + 1,
        prev[j] + 1,
        prev[j - 1] + cost,
      );
    }

    for (let j = 0; j <= b.length; j += 1) {
      prev[j] = curr[j];
    }
  }

  return prev[b.length];
}

function ratio(a, b) {
  const x = normalizeText(a);
  const y = normalizeText(b);

  if (!x || !y) return 0;
  if (x === y) return 100;

  const dist = levenshtein(x, y);

  return Math.max(
    0,
    Math.round((1 - dist / Math.max(x.length, y.length)) * 100),
  );
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
    best = Math.max(best, ratio(shorter, longer.slice(i, i + window)));

    if (best === 100) break;
  }

  return best;
}

function tokenSortRatio(a, b) {
  return ratio(sortedTokens(a), sortedTokens(b));
}

function buildWeakTerms(watched) {
  const terms = [
    ...GLOBAL_WEAK_TOKENS,
    ...(watched?.weak_terms || []),
  ];

  const normalized = [];

  for (const term of terms) {
    normalized.push(normalizeText(term));
    normalized.push(normalizeText(transliterateCyrToLat(term)));
  }

  return new Set(normalized.filter(Boolean));
}

function capScoreForWeakOrPartialToken(score, method, comparedA, comparedB, watchedWeakTerms) {
  const aTokens = tokens(comparedA);
  const bTokens = tokens(comparedB);

  if (!aTokens.length || !bTokens.length) {
    return { score: 0, method: `${method}_empty` };
  }

  const aNorm = normalizeText(comparedA);
  const bNorm = normalizeText(comparedB);

  const shorterTokens = aTokens.length <= bTokens.length ? aTokens : bTokens;
  const longerTokens = aTokens.length <= bTokens.length ? bTokens : aTokens;
  const shorterText = aTokens.length <= bTokens.length ? aNorm : bNorm;

  if (isTooShortForCompare(shorterText)) {
    return { score: 0, method: `${method}_ignored_short` };
  }

  // Якщо збіг тримається лише на одному слові з багатослівної ТМ — не дозволяємо високий score.
  if (shorterTokens.length === 1 && longerTokens.length > 1) {
    const token = shorterTokens[0];
    const isWeak = watchedWeakTerms.has(token) || GLOBAL_WEAK_TOKENS.has(token);

    if (isWeak) {
      return {
        score: Math.min(score, 55),
        method: `${method}_weak_token_cap`,
      };
    }

    if (token.length < 5) {
      return {
        score: Math.min(score, 60),
        method: `${method}_short_token_cap`,
      };
    }

    return {
      score: Math.min(score, 70),
      method: `${method}_single_token_cap`,
    };
  }

  // Якщо обидві сторони — короткі однословні елементи, знижуємо вагу.
  if (
    aTokens.length === 1
    && bTokens.length === 1
    && Math.min(aTokens[0].length, bTokens[0].length) < 5
  ) {
    return {
      score: Math.min(score, 75),
      method: `${method}_short_word_cap`,
    };
  }

  return { score, method };
}

function scoreTexts(applicationText, watchVariant, watchedWeakTerms = new Set()) {
  const variantsA = uniqueStrings([
    applicationText,
    transliterateCyrToLat(applicationText),
  ])
    .map(normalizeText)
    .filter(Boolean);

  const variantsB = uniqueStrings([
    watchVariant,
    transliterateCyrToLat(watchVariant),
  ])
    .map(normalizeText)
    .filter(Boolean);

  let best = {
    score: 0,
    method: '',
    comparedA: '',
    comparedB: '',
  };

  for (const va of variantsA) {
    for (const vb of variantsB) {
      if (isTooShortForCompare(va) || isTooShortForCompare(vb)) continue;

      const exact = va === vb ? 100 : 0;
      const contains = va.includes(vb) || vb.includes(va)
        ? partialRatio(va, vb)
        : 0;
      const lev = ratio(va, vb);
      const partial = partialRatio(va, vb);
      const token = tokenSortRatio(va, vb);

      const candidates = [
        { score: exact, method: 'exact' },
        { score: contains, method: 'contains' },
        { score: lev, method: 'levenshtein' },
        { score: partial, method: 'partial' },
        { score: token, method: 'token_sort' },
      ];

      for (const c of candidates) {
        const capped = capScoreForWeakOrPartialToken(
          c.score,
          c.method,
          va,
          vb,
          watchedWeakTerms,
        );

        if (capped.score > best.score) {
          best = {
            score: capped.score,
            method: capped.method,
            comparedA: va,
            comparedB: vb,
          };
        }
      }
    }
  }

  return best;
}

function classOverlap(aClasses, bClasses) {
  const a = new Set((aClasses || []).map(String).filter(Boolean));
  const b = new Set((bClasses || []).map(String).filter(Boolean));

  const overlap = [...a].filter((x) => b.has(x));

  return overlap.sort((x, y) => Number(x) - Number(y));
}

function normalizeWatchlist(raw) {
  return raw.map((item, idx) => {
    const variants = uniqueStrings([
      item.name,
      ...(Array.isArray(item.variants) ? item.variants : []),
    ]).filter((x) => !isTooShortForCompare(x));

    return {
      id: item.id || `WATCH_${idx + 1}`,
      name: item.name || variants[0] || '',
      variants,
      weak_terms: Array.isArray(item.weak_terms) ? item.weak_terms : [],
      classes: (item.classes || []).map(String).filter(Boolean),
      notes: item.notes || '',
    };
  });
}

function classifyRisk(textScore, overlap) {
  // Без перетину класів не піднімаємо ризик, навіть якщо текстово схоже.
  if (!overlap.length) return 'VERY_LOW_NO_CLASS_OVERLAP';

  if (textScore >= 92) return 'HIGH';
  if (textScore >= 85) return 'MEDIUM_HIGH';
  if (textScore >= 75) return 'MEDIUM';
  if (textScore >= 65) return 'LOW';

  return 'VERY_LOW';
}

function compareApplications(normalizedApps, watchlist, args) {
  const matches = [];
  const threshold = args.threshold;

  for (const app of normalizedApps) {
    const appTexts = app.comparable_word_elements.length
      ? app.comparable_word_elements
      : [];

    if (!appTexts.length) continue;

    for (const watched of watchlist) {
      const overlap = classOverlap(app.classes, watched.classes);

      if (args.requireClassOverlap && overlap.length === 0) {
        continue;
      }

      const watchedWeakTerms = buildWeakTerms(watched);
      let best = null;

      for (const appText of appTexts) {
        for (const variant of watched.variants) {
          const s = scoreTexts(appText, variant, watchedWeakTerms);

          if (!best || s.score > best.score) {
            best = {
              ...s,
              application_text: appText,
              watch_variant: variant,
            };
          }
        }
      }

      if (!best || best.score < threshold) continue;

      matches.push({
        app_number: app.app_number,
        app_date: app.app_date,
        last_update: app.last_update,
        application_text: best.application_text,
        watch_id: watched.id,
        watch_name: watched.name,
        watch_variant: best.watch_variant,
        score: best.score,
        method: best.method,
        risk: classifyRisk(best.score, overlap),
        app_classes: app.classes.join(';'),
        watch_classes: watched.classes.join(';'),
        overlapping_classes: overlap.join(';'),
        has_class_overlap: overlap.length > 0,
        applicants: app.applicants.join(' | '),
        image_urls: app.image_urls.join(' | '),
        compared_application_normalized: best.comparedA,
        compared_watch_normalized: best.comparedB,
      });
    }
  }

  return matches.sort((a, b) => {
    const riskOrder = {
      HIGH: 5,
      MEDIUM_HIGH: 4,
      MEDIUM: 3,
      LOW: 2,
      VERY_LOW: 1,
      VERY_LOW_NO_CLASS_OVERLAP: 0,
    };

    return (riskOrder[b.risk] ?? 0) - (riskOrder[a.risk] ?? 0)
      || b.score - a.score
      || a.app_number.localeCompare(b.app_number);
  });
}

function csvEscape(value) {
  const s = String(value ?? '');

  if (/[",\n\r;]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }

  return s;
}

function toCsv(rows) {
  const headers = [
    'risk',
    'score',
    'method',
    'watch_id',
    'watch_name',
    'watch_variant',
    'app_number',
    'app_date',
    'last_update',
    'application_text',
    'overlapping_classes',
    'app_classes',
    'watch_classes',
    'has_class_overlap',
    'applicants',
    'image_urls',
    'compared_application_normalized',
    'compared_watch_normalized',
  ];

  const lines = [headers.join(';')];

  for (const row of rows) {
    lines.push(headers.map((h) => csvEscape(row[h])).join(';'));
  }

  return lines.join('\n');
}

function escapeMd(value) {
  return String(value ?? '')
    .replace(/\|/g, '\\|')
    .replace(/\n/g, ' ');
}

function buildSummary({ args, fetchMeta, normalized, matches }) {
  const withoutWords = normalized.filter((x) => !x.has_word).length;
  const withWords = normalized.filter((x) => x.has_word).length;

  const withoutComparableWords = normalized.filter(
    (x) => x.has_word && !x.has_comparable_word,
  ).length;

  const withImages = normalized.filter((x) => x.has_image).length;
  const withApplicants = normalized.filter((x) => x.applicants.length > 0).length;

  const byRisk = matches.reduce((acc, m) => {
    acc[m.risk] = (acc[m.risk] || 0) + 1;
    return acc;
  }, {});

  const lines = [];

  lines.push('# TM Monitor Test — summary');
  lines.push('');
  lines.push(`- Date mode: **${args.dateMode}**`);
  lines.push(`- Date range: **${args.from} — ${args.to}**`);
  lines.push(`- API total count: **${fetchMeta.totalCount ?? 'n/a'}**`);
  lines.push(`- Fetched pages: **${fetchMeta.fetchedPages}**`);
  lines.push(`- Fetched applications: **${fetchMeta.fetchedCount}**`);

  if (fetchMeta.stoppedByMaxPages) {
    lines.push('- Stopped by max_pages: **yes**');
  }

  lines.push(`- Applications with word elements: **${withWords}**`);
  lines.push(`- Applications without word elements: **${withoutWords}**`);
  lines.push(`- Applications with only too-short/non-comparable word elements: **${withoutComparableWords}**`);
  lines.push(`- Applications with images: **${withImages}**`);
  lines.push(`- Applications with extracted applicants: **${withApplicants}**`);
  lines.push(`- Threshold: **${args.threshold}**`);
  lines.push(`- Require class overlap: **${args.requireClassOverlap ? 'true' : 'false'}**`);
  lines.push(`- Matches: **${matches.length}**`);
  lines.push(`- Risk breakdown: **${Object.entries(byRisk).map(([k, v]) => `${k}: ${v}`).join(', ') || 'none'}**`);
  lines.push('');

  if (!matches.length) {
    lines.push('No matches found above threshold.');
    return lines.join('\n');
  }

  lines.push('## Matches');
  lines.push('');
  lines.push('| Risk | Score | Watch TM | Application | App No | App Date | Overlap classes | Applicant |');
  lines.push('|---|---:|---|---|---|---|---|---|');

  for (const m of matches.slice(0, 100)) {
    lines.push(
      `| ${escapeMd(m.risk)} | ${m.score} | ${escapeMd(m.watch_name)} | ${escapeMd(m.application_text)} | ${escapeMd(m.app_number)} | ${escapeMd(m.app_date)} | ${escapeMd(m.overlapping_classes || '-')} | ${escapeMd(m.applicants).slice(0, 180)} |`,
    );
  }

  if (matches.length > 100) {
    lines.push(`\nShowing first 100 of ${matches.length} matches.`);
  }

  return lines.join('\n');
}

async function writeJson(file, data) {
  await fs.writeFile(file, JSON.stringify(data, null, 2), 'utf8');
}

async function main() {
  const args = parseArgs(process.argv);

  await fs.mkdir(OUT_DIR, { recursive: true });

  console.log('Run args:', JSON.stringify(args, null, 2));

  console.log('Loading watchlist...');
  const watchRaw = JSON.parse(await fs.readFile(WATCHLIST_PATH, 'utf8'));
  const watchlist = normalizeWatchlist(watchRaw);

  console.log('Fetching applications...');
  const fetchMeta = await fetchApplications(args);

  console.log('Normalizing applications...');
  const normalized = fetchMeta.applications.map(normalizeApplication);

  console.log('Comparing...');
  const matches = compareApplications(normalized, watchlist, args);

  await writeJson(path.join(OUT_DIR, 'raw_applications.json'), fetchMeta.applications);
  await writeJson(path.join(OUT_DIR, 'normalized_applications.json'), normalized);
  await writeJson(path.join(OUT_DIR, 'matches.json'), matches);

  await fs.writeFile(
    path.join(OUT_DIR, 'matches.csv'),
    toCsv(matches),
    'utf8',
  );

  await fs.writeFile(
    path.join(OUT_DIR, 'summary.md'),
    buildSummary({
      args,
      fetchMeta,
      normalized,
      matches,
    }),
    'utf8',
  );

  console.log(
    `Done. Fetched=${fetchMeta.fetchedCount}, normalized=${normalized.length}, matches=${matches.length}`,
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
