import fs from 'node:fs/promises';
import path from 'node:path';

const API_BASE = 'https://sis.nipo.gov.ua/api/v1/open-data/';
const OUT_DIR = 'out';
const WATCHLIST_PATH = 'watchlist.json';

const DEFAULTS = {
  from: '',
  to: '',
  dateMode: 'last_update',
  threshold: 70,
  maxPages: 10,
  requestDelayMs: 1100,
};

function parseArgs(argv) {
  const args = { ...DEFAULTS };
  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    const val = argv[i + 1];
    if (!key.startsWith('--')) continue;
    i += 1;
    switch (key) {
      case '--from': args.from = val; break;
      case '--to': args.to = val; break;
      case '--date-mode': args.dateMode = val; break;
      case '--threshold': args.threshold = Number(val); break;
      case '--max-pages': args.maxPages = Number(val); break;
      case '--request-delay-ms': args.requestDelayMs = Number(val); break;
      default:
        throw new Error(`Unknown argument: ${key}`);
    }
  }
  if (!/^\d{2}\.\d{2}\.\d{4}$/.test(args.from)) {
    throw new Error('Invalid --from. Expected format дд.мм.рррр, e.g. 01.04.2026');
  }
  if (!/^\d{2}\.\d{2}\.\d{4}$/.test(args.to)) {
    throw new Error('Invalid --to. Expected format дд.мм.рррр, e.g. 28.04.2026');
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
      'accept': 'application/json',
      'user-agent': 'tm-monitor-test/0.1 GitHub Actions',
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

  while (url) {
    page += 1;
    if (args.maxPages > 0 && page > args.maxPages) {
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

  return { totalCount, fetchedCount: all.length, applications: all };
}

function asArray(value) {
  if (value === null || value === undefined) return [];
  return Array.isArray(value) ? value : [value];
}

function getTextValue(value) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string' || typeof value === 'number') return String(value).trim();
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
  const values = findValuesByKeyDeep(item?.data || {}, ['MarkImageFilename', 'ImageFilename', 'FileName', 'filename'])
    .flatMap(asArray)
    .map(getTextValue)
    .filter(Boolean)
    .map((v) => v.startsWith('http') ? v : `https://sis.nipo.gov.ua${v.startsWith('/') ? '' : '/'}${v}`);
  return uniqueStrings(values);
}

function extractClasses(item) {
  const data = item?.data || {};
  const values = findValuesByKeyDeep(data, ['ClassNumber', 'class_number', 'ClassNo'])
    .flatMap(asArray)
    .map(getTextValue)
    .filter(Boolean)
    .map((s) => s.replace(/\D+/g, ''))
    .filter(Boolean);
  return uniqueStrings(values).sort((a, b) => Number(a) - Number(b));
}

function compactPartyName(party) {
  if (!party || typeof party !== 'object') return getTextValue(party);
  const candidates = [
    'ApplicantName', 'ApplicantFullName', 'HolderName', 'RepresentativeName',
    'Name', 'OrganizationName', 'LegalEntityName', 'IndividualName',
    'PersonName', 'EntitlementOwnerName'
  ];
  const values = [];
  for (const key of candidates) {
    const v = party[key];
    if (v !== undefined) values.push(getTextValue(v));
  }
  if (values.some(Boolean)) return uniqueStrings(values).join(' / ');

  const deepNames = findValuesByKeyDeep(party, candidates, 6)
    .flatMap(asArray)
    .map(getTextValue)
    .filter(Boolean);
  return uniqueStrings(deepNames).slice(0, 3).join(' / ');
}

function extractApplicants(item) {
  const data = item?.data || {};
  const direct = getByPath(data, ['ApplicantDetails', 'Applicant'])
    ?? getByPath(data, ['ApplicantDetails'])
    ?? null;
  const directNames = asArray(direct).map(compactPartyName).filter(Boolean);

  const deep = findValuesByKeyDeep(data, ['Applicant'])
    .flatMap(asArray)
    .map(compactPartyName)
    .filter(Boolean);

  return uniqueStrings([...directNames, ...deep]).slice(0, 10);
}

function normalizeAppNumber(value) {
  return String(value || '').trim();
}

function normalizeApplication(item) {
  const wordElements = extractWordElements(item);
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
    word_text: wordElements.join(' | '),
    classes: extractClasses(item),
    applicants: extractApplicants(item),
    image_urls: extractImageUrls(item),
    has_word: wordElements.length > 0,
    has_image: extractImageUrls(item).length > 0,
  };
}

const CYR_TO_LAT = {
  'а': 'a', 'б': 'b', 'в': 'v', 'г': 'h', 'ґ': 'g', 'д': 'd', 'е': 'e', 'є': 'ie',
  'ж': 'zh', 'з': 'z', 'и': 'y', 'і': 'i', 'ї': 'i', 'й': 'i', 'к': 'k', 'л': 'l',
  'м': 'm', 'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
  'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'shch', 'ь': '', 'ю': 'iu', 'я': 'ia',
  'ы': 'y', 'э': 'e', 'ё': 'e', 'ъ': ''
};

function transliterateCyrToLat(input) {
  return String(input || '').toLowerCase().split('').map((ch) => CYR_TO_LAT[ch] ?? ch).join('');
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

function sortedTokens(input) {
  return normalizeText(input).split(' ').filter(Boolean).sort().join(' ');
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
    for (let j = 0; j <= b.length; j += 1) prev[j] = curr[j];
  }
  return prev[b.length];
}

function ratio(a, b) {
  const x = normalizeText(a);
  const y = normalizeText(b);
  if (!x || !y) return 0;
  if (x === y) return 100;
  const dist = levenshtein(x, y);
  return Math.round((1 - dist / Math.max(x.length, y.length)) * 100);
}

function partialRatio(a, b) {
  const x = normalizeText(a);
  const y = normalizeText(b);
  if (!x || !y) return 0;
  if (x === y) return 100;
  const shorter = x.length <= y.length ? x : y;
  const longer = x.length <= y.length ? y : x;
  if (longer.includes(shorter)) return Math.min(100, Math.max(80, Math.round((shorter.length / longer.length) * 100)));
  if (shorter.length < 3) return 0;

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

function scoreTexts(a, b) {
  const variantsA = uniqueStrings([a, transliterateCyrToLat(a)]).map(normalizeText).filter(Boolean);
  const variantsB = uniqueStrings([b, transliterateCyrToLat(b)]).map(normalizeText).filter(Boolean);
  let best = { score: 0, method: '', comparedA: '', comparedB: '' };

  for (const va of variantsA) {
    for (const vb of variantsB) {
      const exact = va === vb ? 100 : 0;
      const contains = va.includes(vb) || vb.includes(va) ? partialRatio(va, vb) : 0;
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
        if (c.score > best.score) {
          best = { score: c.score, method: c.method, comparedA: va, comparedB: vb };
        }
      }
    }
  }
  return best;
}

function classOverlap(aClasses, bClasses) {
  const a = new Set((aClasses || []).map(String));
  const b = new Set((bClasses || []).map(String));
  const overlap = [...a].filter((x) => b.has(x));
  return overlap.sort((x, y) => Number(x) - Number(y));
}

function normalizeWatchlist(raw) {
  return raw.map((item, idx) => {
    const variants = uniqueStrings([
      item.name,
      ...(Array.isArray(item.variants) ? item.variants : []),
    ]);
    return {
      id: item.id || `WATCH_${idx + 1}`,
      name: item.name || variants[0] || '',
      variants,
      classes: (item.classes || []).map(String),
      notes: item.notes || '',
    };
  });
}

function classifyRisk(textScore, overlap) {
  if (textScore >= 92 && overlap.length > 0) return 'HIGH';
  if (textScore >= 85) return overlap.length > 0 ? 'HIGH' : 'MEDIUM';
  if (textScore >= 75) return overlap.length > 0 ? 'MEDIUM' : 'LOW';
  return overlap.length > 0 ? 'LOW' : 'VERY_LOW';
}

function compareApplications(normalizedApps, watchlist, threshold) {
  const matches = [];

  for (const app of normalizedApps) {
    const appTexts = app.word_elements.length ? app.word_elements : [];
    if (!appTexts.length) continue;

    for (const watched of watchlist) {
      let best = null;
      for (const appText of appTexts) {
        for (const variant of watched.variants) {
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

      if (!best || best.score < threshold) continue;
      const overlap = classOverlap(app.classes, watched.classes);
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
        applicants: app.applicants.join(' | '),
        image_urls: app.image_urls.join(' | '),
        compared_application_normalized: best.comparedA,
        compared_watch_normalized: best.comparedB,
      });
    }
  }

  return matches.sort((a, b) => b.score - a.score || a.app_number.localeCompare(b.app_number));
}

function csvEscape(value) {
  const s = String(value ?? '');
  if (/[",\n\r;]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function toCsv(rows) {
  const headers = [
    'risk', 'score', 'method', 'watch_id', 'watch_name', 'watch_variant',
    'app_number', 'app_date', 'last_update', 'application_text',
    'overlapping_classes', 'app_classes', 'watch_classes', 'applicants', 'image_urls',
    'compared_application_normalized', 'compared_watch_normalized'
  ];
  const lines = [headers.join(';')];
  for (const row of rows) {
    lines.push(headers.map((h) => csvEscape(row[h])).join(';'));
  }
  return lines.join('\n');
}

function escapeMd(value) {
  return String(value ?? '').replace(/\|/g, '\\|').replace(/\n/g, ' ');
}

function buildSummary({ args, fetchMeta, normalized, matches }) {
  const withoutWords = normalized.filter((x) => !x.has_word).length;
  const withWords = normalized.filter((x) => x.has_word).length;
  const withImages = normalized.filter((x) => x.has_image).length;
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
  lines.push(`- Fetched applications: **${fetchMeta.fetchedCount}**`);
  lines.push(`- Applications with word elements: **${withWords}**`);
  lines.push(`- Applications without word elements: **${withoutWords}**`);
  lines.push(`- Applications with images: **${withImages}**`);
  lines.push(`- Threshold: **${args.threshold}**`);
  lines.push(`- Matches: **${matches.length}**`);
  lines.push(`- Risk breakdown: **${Object.entries(byRisk).map(([k, v]) => `${k}: ${v}`).join(', ') || 'none'}**`);
  lines.push('');

  if (!matches.length) {
    lines.push('No matches found above threshold.');
    return lines.join('\n');
  }

  lines.push('## Matches');
  lines.push('');
  lines.push('| Risk | Score | Watch TM | Application | App No | App Date | Classes | Applicant |');
  lines.push('|---|---:|---|---|---|---|---|---|');
  for (const m of matches.slice(0, 100)) {
    lines.push(`| ${escapeMd(m.risk)} | ${m.score} | ${escapeMd(m.watch_name)} | ${escapeMd(m.application_text)} | ${escapeMd(m.app_number)} | ${escapeMd(m.app_date)} | ${escapeMd(m.overlapping_classes || m.app_classes)} | ${escapeMd(m.applicants).slice(0, 180)} |`);
  }
  if (matches.length > 100) lines.push(`\nShowing first 100 of ${matches.length} matches.`);
  return lines.join('\n');
}

async function writeJson(file, data) {
  await fs.writeFile(file, JSON.stringify(data, null, 2), 'utf8');
}

async function main() {
  const args = parseArgs(process.argv);
  await fs.mkdir(OUT_DIR, { recursive: true });

  console.log('Loading watchlist...');
  const watchRaw = JSON.parse(await fs.readFile(WATCHLIST_PATH, 'utf8'));
  const watchlist = normalizeWatchlist(watchRaw);

  console.log('Fetching applications...');
  const fetchMeta = await fetchApplications(args);

  console.log('Normalizing applications...');
  const normalized = fetchMeta.applications.map(normalizeApplication);

  console.log('Comparing...');
  const matches = compareApplications(normalized, watchlist, args.threshold);

  await writeJson(path.join(OUT_DIR, 'raw_applications.json'), fetchMeta.applications);
  await writeJson(path.join(OUT_DIR, 'normalized_applications.json'), normalized);
  await writeJson(path.join(OUT_DIR, 'matches.json'), matches);
  await fs.writeFile(path.join(OUT_DIR, 'matches.csv'), toCsv(matches), 'utf8');
  await fs.writeFile(path.join(OUT_DIR, 'summary.md'), buildSummary({ args, fetchMeta, normalized, matches }), 'utf8');

  console.log(`Done. Fetched=${fetchMeta.fetchedCount}, normalized=${normalized.length}, matches=${matches.length}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
