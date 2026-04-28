// scripts/tm_word_presence_check.mjs

import fs from 'node:fs/promises';
import path from 'node:path';

const API_BASE = 'https://sis.nipo.gov.ua/api/v1/open-data/';
const OUT_DIR = 'out';

const DEFAULTS = {
  from: '',
  to: '',
  dateMode: 'app_date',
  maxPages: 0,
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
      case '--from':
        args.from = val;
        break;

      case '--to':
        args.to = val;
        break;

      case '--date-mode':
        args.dateMode = val;
        break;

      case '--max-pages':
        args.maxPages = Number(val);
        break;

      case '--request-delay-ms':
        args.requestDelayMs = Number(val);
        break;

      default:
        throw new Error(`Unknown argument: ${key}`);
    }
  }

  if (!/^\d{2}\.\d{2}\.\d{4}$/.test(args.from)) {
    throw new Error('Invalid --from. Expected format дд.мм.рррр, e.g. 24.04.2026');
  }

  if (!/^\d{2}\.\d{2}\.\d{4}$/.test(args.to)) {
    throw new Error('Invalid --to. Expected format дд.мм.рррр, e.g. 24.04.2026');
  }

  if (!['app_date', 'last_update'].includes(args.dateMode)) {
    throw new Error('--date-mode must be app_date or last_update');
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

  url.searchParams.set('obj_state', '1'); // заявки
  url.searchParams.set('obj_type', '4');  // торговельні марки

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
      'user-agent': 'tm-word-presence-check/0.1 GitHub Actions',
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
  const applications = [];
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
    applications.push(...results);

    url = json.next || null;

    if (url) {
      await sleep(args.requestDelayMs);
    }
  }

  return {
    totalCount,
    fetchedPages: page - (stoppedByMaxPages ? 1 : 0),
    fetchedCount: applications.length,
    stoppedByMaxPages,
    applications,
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
  const data = item?.data || {};

  const values = findValuesByKeyDeep(data, [
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

function extractCurrentStage(item) {
  const stages = item?.data?.stages;

  if (!Array.isArray(stages)) return '';

  const current = stages.find((stage) => stage?.status === 'current');

  if (current?.title) return String(current.title).trim();

  const active = stages.find((stage) => stage?.status === 'active');

  if (active?.title) return String(active.title).trim();

  return '';
}

function extractApplicationStatus(item) {
  return String(item?.data?.application_status || '').trim();
}

function toShortDate(value) {
  if (!value) return '';

  const s = String(value);

  // API часто повертає ISO, наприклад 2026-04-24T00:00:00.000000Z.
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);

  if (m) return `${m[3]}.${m[2]}.${m[1]}`;

  return s;
}

function normalizeApplication(item) {
  const wordElements = extractWordElements(item);
  const imageUrls = extractImageUrls(item);

  return {
    app_number: String(item?.app_number || '').trim(),
    app_date: toShortDate(item?.app_date),
    last_update: String(item?.last_update || '').trim(),
    application_status: extractApplicationStatus(item),
    current_stage: extractCurrentStage(item),
    classes: extractClasses(item),
    has_word: wordElements.length > 0,
    word_count: wordElements.length,
    word_elements: wordElements,
    has_image: imageUrls.length > 0,
    image_count: imageUrls.length,
    image_urls: imageUrls,
  };
}

function csvEscape(value) {
  const s = Array.isArray(value)
    ? value.join(' | ')
    : String(value ?? '');

  if (/[",\n\r;]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }

  return s;
}

function toCsv(rows, headers) {
  const lines = [headers.join(';')];

  for (const row of rows) {
    lines.push(headers.map((h) => csvEscape(row[h])).join(';'));
  }

  return lines.join('\n');
}

function countBy(items, keyFn) {
  const map = new Map();

  for (const item of items) {
    const key = keyFn(item) || '(empty)';
    map.set(key, (map.get(key) || 0) + 1);
  }

  return [...map.entries()]
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count || a.key.localeCompare(b.key));
}

function countStageStats(items) {
  const map = new Map();

  for (const item of items) {
    const stage = item.current_stage || '(empty)';

    if (!map.has(stage)) {
      map.set(stage, {
        stage,
        total: 0,
        with_word: 0,
        without_word: 0,
        with_image: 0,
      });
    }

    const row = map.get(stage);

    row.total += 1;

    if (item.has_word) {
      row.with_word += 1;
    } else {
      row.without_word += 1;
    }

    if (item.has_image) {
      row.with_image += 1;
    }
  }

  return [...map.values()]
    .map((row) => ({
      ...row,
      word_percent: row.total ? Math.round((row.with_word / row.total) * 1000) / 10 : 0,
    }))
    .sort((a, b) => b.total - a.total || a.stage.localeCompare(b.stage));
}

function escapeMd(value) {
  return String(value ?? '')
    .replace(/\|/g, '\\|')
    .replace(/\n/g, ' ');
}

function buildSummary({ args, fetchMeta, normalized }) {
  const total = normalized.length;
  const withWord = normalized.filter((x) => x.has_word).length;
  const withoutWord = normalized.filter((x) => !x.has_word).length;
  const withImage = normalized.filter((x) => x.has_image).length;
  const withImageWithoutWord = normalized.filter((x) => x.has_image && !x.has_word).length;

  const wordPercent = total ? Math.round((withWord / total) * 1000) / 10 : 0;
  const noWordPercent = total ? Math.round((withoutWord / total) * 1000) / 10 : 0;

  const stageStats = countStageStats(normalized);
  const statusStats = countBy(normalized, (x) => x.application_status);

  const lines = [];

  lines.push('# TM Word Presence Check — summary');
  lines.push('');
  lines.push(`- Date mode: **${args.dateMode}**`);
  lines.push(`- Date range: **${args.from} — ${args.to}**`);
  lines.push(`- API total count: **${fetchMeta.totalCount ?? 'n/a'}**`);
  lines.push(`- Fetched pages: **${fetchMeta.fetchedPages}**`);
  lines.push(`- Fetched applications: **${fetchMeta.fetchedCount}**`);

  if (fetchMeta.stoppedByMaxPages) {
    lines.push('- Stopped by max_pages: **yes**');
  }

  lines.push('');
  lines.push(`- Applications with word elements: **${withWord} / ${total} (${wordPercent}%)**`);
  lines.push(`- Applications without word elements: **${withoutWord} / ${total} (${noWordPercent}%)**`);
  lines.push(`- Applications with images: **${withImage} / ${total}**`);
  lines.push(`- Applications with image but without word elements: **${withImageWithoutWord} / ${total}**`);
  lines.push('');

  lines.push('## Stage breakdown');
  lines.push('');
  lines.push('| Current stage | Total | With word | Without word | With image | Word % |');
  lines.push('|---|---:|---:|---:|---:|---:|');

  for (const row of stageStats) {
    lines.push(
      `| ${escapeMd(row.stage)} | ${row.total} | ${row.with_word} | ${row.without_word} | ${row.with_image} | ${row.word_percent}% |`,
    );
  }

  lines.push('');
  lines.push('## Application status breakdown');
  lines.push('');
  lines.push('| Status | Count |');
  lines.push('|---|---:|');

  for (const row of statusStats) {
    lines.push(`| ${escapeMd(row.key)} | ${row.count} |`);
  }

  const examplesWithWord = normalized.filter((x) => x.has_word).slice(0, 30);
  const examplesWithoutWord = normalized.filter((x) => !x.has_word).slice(0, 30);

  if (examplesWithWord.length) {
    lines.push('');
    lines.push('## Examples with word elements');
    lines.push('');
    lines.push('| App no | App date | Stage | Classes | Word elements |');
    lines.push('|---|---|---|---|---|');

    for (const item of examplesWithWord) {
      lines.push(
        `| ${escapeMd(item.app_number)} | ${escapeMd(item.app_date)} | ${escapeMd(item.current_stage)} | ${escapeMd(item.classes.join(', '))} | ${escapeMd(item.word_elements.join(' | ')).slice(0, 250)} |`,
      );
    }
  }

  if (examplesWithoutWord.length) {
    lines.push('');
    lines.push('## Examples without word elements');
    lines.push('');
    lines.push('| App no | App date | Stage | Classes | Has image |');
    lines.push('|---|---|---|---|---|');

    for (const item of examplesWithoutWord) {
      lines.push(
        `| ${escapeMd(item.app_number)} | ${escapeMd(item.app_date)} | ${escapeMd(item.current_stage)} | ${escapeMd(item.classes.join(', '))} | ${item.has_image ? 'yes' : 'no'} |`,
      );
    }
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

  console.log('Fetching applications...');
  const fetchMeta = await fetchApplications(args);

  console.log('Normalizing applications...');
  const normalized = fetchMeta.applications.map(normalizeApplication);

  const withWord = normalized.filter((x) => x.has_word);
  const withoutWord = normalized.filter((x) => !x.has_word);

  const headers = [
    'app_number',
    'app_date',
    'last_update',
    'application_status',
    'current_stage',
    'classes',
    'has_word',
    'word_count',
    'word_elements',
    'has_image',
    'image_count',
    'image_urls',
  ];

  await writeJson(path.join(OUT_DIR, 'raw_applications.json'), fetchMeta.applications);
  await writeJson(path.join(OUT_DIR, 'word_presence.json'), normalized);

  await fs.writeFile(
    path.join(OUT_DIR, 'word_presence.csv'),
    toCsv(normalized, headers),
    'utf8',
  );

  await fs.writeFile(
    path.join(OUT_DIR, 'with_word_applications.csv'),
    toCsv(withWord, headers),
    'utf8',
  );

  await fs.writeFile(
    path.join(OUT_DIR, 'without_word_applications.csv'),
    toCsv(withoutWord, headers),
    'utf8',
  );

  await fs.writeFile(
    path.join(OUT_DIR, 'summary.md'),
    buildSummary({
      args,
      fetchMeta,
      normalized,
    }),
    'utf8',
  );

  console.log(
    `Done. Total=${normalized.length}, with_word=${withWord.length}, without_word=${withoutWord.length}`,
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
