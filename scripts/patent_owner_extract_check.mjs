// scripts/patent_owner_extract_check.mjs

import fs from 'node:fs/promises';
import path from 'node:path';

const API_BASE = 'https://sis.nipo.gov.ua/api/v1/open-data/';
const OUT_DIR = 'out';

const DEFAULTS = {
  objType: '2',
  objState: '2',
  dateMode: 'reg_date',
  from: '',
  to: '',
  objectNumber: '',
  maxPages: 0,
  requestDelayMs: 1100,
};

const OBJ_TYPE_LABELS = {
  1: 'Винахід',
  2: 'Корисна модель',
  6: 'Промисловий зразок',
};

const TERMINATION_KEYWORDS = [
  'припин',
  'втратив чинність',
  'втратила чинність',
  'втрата чинності',
  'нечин',
  'скас',
  'анул',
  'недійс',
  'достроков',
  'terminated',
  'termination',
  'lapsed',
  'expired',
  'cancelled',
  'canceled',
  'annulled',
  'invalidated',
];

const NEGATIVE_STATUS_KEYWORDS = TERMINATION_KEYWORDS;

const SKIP_STATUS_SCAN_KEYS = new Set([
  'AB',
  'DE',
  'CL',
  'files',
]);

function parseArgs(argv) {
  const args = { ...DEFAULTS };

  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    const val = argv[i + 1];

    if (!key.startsWith('--')) continue;
    i += 1;

    switch (key) {
      case '--obj-type':
        args.objType = String(val || '').trim();
        break;
      case '--obj-state':
        args.objState = String(val || '').trim();
        break;
      case '--date-mode':
        args.dateMode = String(val || '').trim();
        break;
      case '--from':
        args.from = String(val || '').trim();
        break;
      case '--to':
        args.to = String(val || '').trim();
        break;
      case '--object-number':
        args.objectNumber = String(val || '').trim();
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

  if (!['1', '2', '6'].includes(args.objType)) {
    throw new Error('--obj-type must be 1, 2, or 6');
  }

  if (!['1', '2', 'all'].includes(args.objState)) {
    throw new Error('--obj-state must be 1, 2, or all');
  }

  if (!['app_date', 'reg_date', 'last_update'].includes(args.dateMode)) {
    throw new Error('--date-mode must be app_date, reg_date, or last_update');
  }

  if (!args.objectNumber) {
    if (!/^\d{2}\.\d{2}\.\d{4}$/.test(args.from)) {
      throw new Error('Invalid --from. Expected format дд.мм.рррр, e.g. 01.01.2026');
    }

    if (!/^\d{2}\.\d{2}\.\d{4}$/.test(args.to)) {
      throw new Error('Invalid --to. Expected format дд.мм.рррр, e.g. 31.01.2026');
    }
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
  if (args.objectNumber) {
    const url = new URL(`${API_BASE}${encodeURIComponent(args.objectNumber)}/`);
    url.searchParams.set('obj_type', args.objType);
    return url.toString();
  }

  const url = new URL(API_BASE);
  url.searchParams.set('obj_type', args.objType);

  if (args.objState !== 'all') {
    url.searchParams.set('obj_state', args.objState);
  }

  if (args.dateMode === 'app_date') {
    url.searchParams.set('app_date_from', args.from);
    url.searchParams.set('app_date_to', args.to);
  } else if (args.dateMode === 'reg_date') {
    url.searchParams.set('reg_date_from', args.from);
    url.searchParams.set('reg_date_to', args.to);
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
      'user-agent': 'patent-owner-extract-check/0.2 GitHub Actions',
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

async function fetchObjects(args) {
  if (args.objectNumber) {
    const url = buildInitialUrl(args);
    console.log(`Fetching one object: ${url}`);
    const json = await fetchJson(url);
    return {
      totalCount: 1,
      fetchedPages: 1,
      fetchedCount: 1,
      stoppedByMaxPages: false,
      objects: [json],
    };
  }

  let url = buildInitialUrl(args);
  const objects = [];
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
    objects.push(...results);

    url = json.next || null;

    if (url) {
      await sleep(args.requestDelayMs);
    }
  }

  return {
    totalCount,
    fetchedPages: page - (stoppedByMaxPages ? 1 : 0),
    fetchedCount: objects.length,
    stoppedByMaxPages,
    objects,
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

function firstNonEmpty(...values) {
  for (const value of values) {
    const s = getTextValue(value);
    if (s) return s;
  }
  return '';
}

function formatDate(value) {
  const s = String(value || '').trim();
  if (!s) return '';

  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (iso) return `${iso[3]}.${iso[2]}.${iso[1]}`;

  const dmY = s.match(/^(\d{2})\.(\d{2})\.(\d{4})/);
  if (dmY) return `${dmY[1]}.${dmY[2]}.${dmY[3]}`;

  return s;
}

function extractDatesFromText(value) {
  const s = String(value || '');
  const dates = [];

  for (const m of s.matchAll(/\b(\d{4})-(\d{2})-(\d{2})\b/g)) {
    dates.push(`${m[3]}.${m[2]}.${m[1]}`);
  }

  for (const m of s.matchAll(/\b(\d{2})\.(\d{2})\.(\d{4})\b/g)) {
    dates.push(`${m[1]}.${m[2]}.${m[3]}`);
  }

  return uniqueStrings(dates);
}

function findScalarsDeep(obj, options = {}) {
  const {
    maxDepth = 12,
    skipKeys = new Set(),
  } = options;

  const out = [];
  const stack = [{ value: obj, path: '', depth: 0 }];
  const seen = new WeakSet();

  while (stack.length) {
    const { value, path: p, depth } = stack.pop();
    if (value === null || value === undefined || depth > maxDepth) continue;

    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
      out.push({ path: p, value: String(value) });
      continue;
    }

    if (typeof value === 'object') {
      if (seen.has(value)) continue;
      seen.add(value);

      if (Array.isArray(value)) {
        for (let i = 0; i < value.length; i += 1) {
          stack.push({ value: value[i], path: `${p}[]`, depth: depth + 1 });
        }
      } else {
        for (const [k, v] of Object.entries(value)) {
          if (skipKeys.has(k)) continue;
          stack.push({ value: v, path: p ? `${p}.${k}` : k, depth: depth + 1 });
        }
      }
    }
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

function valuesByKeyRegex(obj, regex, maxDepth = 8) {
  const out = [];
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
          if (regex.test(k)) out.push(v);
          stack.push({ value: v, depth: depth + 1 });
        }
      }
    }
  }

  return out;
}

function extractTitle(data) {
  const titles = [];

  for (const row of asArray(data?.I_54)) {
    titles.push(row?.['I_54.U']);
  }
  for (const row of asArray(data?.I_54)) {
    titles.push(row?.['I_54.E']);
  }

  titles.push(...valuesByKeyRegex(data, /(^|\.)I_54\./, 4).map(getTextValue));
  titles.push(...valuesByKeyRegex(data, /Title|InventionTitle|DesignTitle|Name/i, 4).map(getTextValue));

  return uniqueStrings(titles)[0] || '';
}

function extractOwnerRows(data) {
  const rows = [];

  for (const owner of asArray(data?.I_73)) {
    if (!owner || typeof owner !== 'object') continue;

    const name = firstNonEmpty(
      owner['I_73.N'],
      owner['I_73.N.U'],
      owner['I_73.N.E'],
      owner['N'],
      owner['name'],
    );

    const country = firstNonEmpty(
      owner['I_73.C'],
      owner['I_73.C.U'],
      owner['I_73.C.E'],
      owner['C'],
    );

    const lang = firstNonEmpty(owner['I_73.L'], owner['L']);
    const edrpou = firstNonEmpty(owner.EDRPOU, owner.Edrpou, owner.edrpou);

    const address = uniqueStrings([
      owner['I_73.A'],
      owner['I_73.ADDR'],
      owner['I_73.Address'],
      owner.Address,
      owner.address,
      ...valuesByKeyRegex(owner, /Address|ADDR|A$/i, 4).map(getTextValue),
    ]).join(' | ');

    if (name || edrpou || country || address) {
      rows.push({ name, edrpou, country, lang, address });
    }
  }

  const holderBlocks = [
    data?.HolderDetails,
    data?.OwnerDetails,
    data?.PatentHolderDetails,
    data?.RightHolderDetails,
  ].filter(Boolean);

  for (const block of holderBlocks) {
    const scalarValues = findScalarsDeep(block, { maxDepth: 8 });
    const names = scalarValues
      .filter((x) => /Name|Holder|Owner|Applicant|FreeFormatNameLine/i.test(x.path))
      .map((x) => x.value);
    const ids = scalarValues
      .filter((x) => /EDRPOU|Identifier|RegistrationNumber/i.test(x.path))
      .map((x) => x.value);
    const addresses = scalarValues
      .filter((x) => /Address|Postcode|City|Street|Country/i.test(x.path))
      .map((x) => x.value);

    if (names.length || ids.length || addresses.length) {
      rows.push({
        name: uniqueStrings(names).join(' | '),
        edrpou: uniqueStrings(ids).join(' | '),
        country: '',
        lang: '',
        address: uniqueStrings(addresses).join(' | '),
      });
    }
  }

  return rows.filter((x) => x.name || x.edrpou || x.address);
}

function extractCorrespondenceAddress(data) {
  const direct = uniqueStrings([
    data?.I_98,
    data?.I_98_Index,
  ]).join(', ');

  if (direct) return direct;

  const blocks = [
    data?.CorrespondenceAddress,
    data?.CorrespondenceDetails,
  ].filter(Boolean);

  const parts = [];
  for (const block of blocks) {
    const scalarValues = findScalarsDeep(block, { maxDepth: 8 });
    parts.push(...scalarValues
      .filter((x) => /Address|Postcode|City|Street|Country|FreeFormatAddressLine/i.test(x.path))
      .map((x) => x.value));
  }

  return uniqueStrings(parts).join(' | ');
}

function extractAddress(data, ownerRows) {
  const ownerAddresses = uniqueStrings(ownerRows.map((x) => x.address).filter(Boolean));
  if (ownerAddresses.length) {
    return { address: ownerAddresses.join(' | '), source: 'owner' };
  }

  const correspondence = extractCorrespondenceAddress(data);
  if (correspondence) {
    return { address: correspondence, source: 'correspondence' };
  }

  return { address: '', source: '' };
}

function extractIpc(data) {
  return uniqueStrings(asArray(data?.IPC).map(getTextValue)).join(' | ');
}

function extractRegistrationDate(item) {
  return formatDate(firstNonEmpty(item?.data?.I_24, item?.registration_date));
}

function extractPatentNumber(item) {
  return firstNonEmpty(item?.data?.I_11, item?.registration_number);
}

function extractApplicationNumber(item) {
  return firstNonEmpty(item?.data?.I_21, item?.app_number);
}

function extractApplicationDate(item) {
  return formatDate(firstNonEmpty(item?.data?.I_22, item?.app_date));
}

function extractPatentKind(item) {
  return firstNonEmpty(item?.data?.I_12, OBJ_TYPE_LABELS[item?.obj_type_id] || item?.obj_type || '');
}

function extractStatusSignals(item) {
  const scanRoot = {
    data: item?.data || {},
    data_docs: item?.data_docs || {},
    data_payments: item?.data_payments || {},
  };

  const scalars = findScalarsDeep(scanRoot, {
    maxDepth: 12,
    skipKeys: SKIP_STATUS_SCAN_KEYS,
  });

  const signals = [];
  const dates = [];

  for (const { path: p, value } of scalars) {
    const haystack = `${p} ${value}`.toLowerCase();
    if (NEGATIVE_STATUS_KEYWORDS.some((kw) => haystack.includes(kw))) {
      const text = `${p}: ${String(value).replace(/\s+/g, ' ').trim()}`;
      signals.push(text.slice(0, 350));
      dates.push(...extractDatesFromText(value));
    }
  }

  return {
    signals: uniqueStrings(signals),
    dates: uniqueStrings(dates),
  };
}

function extractCurrentStage(data) {
  const stages = asArray(data?.stages);
  const current = stages.find((stage) => stage?.status === 'current' || stage?.status === 'active');
  if (current?.title) return String(current.title).trim();

  const lastDone = [...stages].reverse().find((stage) => stage?.status === 'done');
  if (lastDone?.title) return String(lastDone.title).trim();

  return '';
}

function inferLegalStatus(item) {
  const data = item?.data || {};
  const statusColor = String(data.registration_status_color || '').trim();
  const currentStage = extractCurrentStage(data);
  const { signals, dates } = extractStatusSignals(item);

  if (signals.length) {
    return {
      legal_status: 'TERMINATED_OR_CANCELLED_REVIEW',
      status_label_uk: 'є ознаки припинення/скасування/втрати чинності — перевірити',
      status_confidence: 'medium',
      status_color: statusColor,
      status_evidence: signals.join(' | '),
      termination_or_cancellation_date: dates.join(' | '),
      current_stage: currentStage,
    };
  }

  if (Number(item?.obj_state) === 2) {
    return {
      legal_status: 'REGISTERED_NO_NEGATIVE_SIGNAL',
      status_label_uk: 'зареєстрований; явних ознак скасування/втрати чинності у JSON не знайдено',
      status_confidence: statusColor === 'green' ? 'medium' : 'low',
      status_color: statusColor,
      status_evidence: uniqueStrings([
        statusColor ? `registration_status_color=${statusColor}` : '',
        currentStage ? `stage=${currentStage}` : '',
      ]).join(' | '),
      termination_or_cancellation_date: '',
      current_stage: currentStage,
    };
  }

  return {
    legal_status: 'APPLICATION_OR_UNKNOWN',
    status_label_uk: 'заявка або статус не визначено',
    status_confidence: 'low',
    status_color: statusColor,
    status_evidence: uniqueStrings([
      statusColor ? `registration_status_color=${statusColor}` : '',
      currentStage ? `stage=${currentStage}` : '',
    ]).join(' | '),
    termination_or_cancellation_date: '',
    current_stage: currentStage,
  };
}

function normalizeObject(item) {
  const data = item?.data || {};
  const ownerRows = extractOwnerRows(data);
  const { address, source: addressSource } = extractAddress(data, ownerRows);
  const status = inferLegalStatus(item);

  return {
    obj_type_id: item?.obj_type_id ?? '',
    object_type: item?.obj_type || OBJ_TYPE_LABELS[item?.obj_type_id] || '',
    obj_state: item?.obj_state ?? '',
    patent_kind: extractPatentKind(item),
    patent_number: extractPatentNumber(item),
    application_number: extractApplicationNumber(item),
    application_date: extractApplicationDate(item),
    registration_date: extractRegistrationDate(item),
    title: extractTitle(data),
    owner_patent_holder: uniqueStrings(ownerRows.map((x) => x.name)).join(' | '),
    owner_edrpou: uniqueStrings(ownerRows.map((x) => x.edrpou)).join(' | '),
    owner_country: uniqueStrings(ownerRows.map((x) => x.country)).join(' | '),
    address,
    address_source: addressSource,
    legal_status: status.legal_status,
    status_label_uk: status.status_label_uk,
    status_confidence: status.status_confidence,
    status_raw_color: status.status_color,
    status_evidence: status.status_evidence,
    termination_or_cancellation_date: status.termination_or_cancellation_date,
    current_stage: status.current_stage,
    ipc: extractIpc(data),
    last_update: item?.last_update || '',
  };
}

function extractPartyRows(item, record) {
  const rows = [];
  const ownerRows = extractOwnerRows(item?.data || {});

  for (const owner of ownerRows) {
    rows.push({
      patent_number: record.patent_number,
      application_number: record.application_number,
      role: 'OWNER_PATENT_HOLDER',
      name: owner.name,
      edrpou: owner.edrpou,
      country: owner.country,
      lang: owner.lang,
      address: owner.address || record.address,
      address_source: owner.address ? 'owner' : record.address_source,
    });
  }

  const applicantRows = [];
  for (const applicant of asArray(item?.data?.I_71)) {
    if (!applicant || typeof applicant !== 'object') continue;
    applicantRows.push({
      name: firstNonEmpty(applicant['I_71.N.U'], applicant['I_71.N.E'], applicant['I_71.N']),
      edrpou: firstNonEmpty(applicant.EDRPOU, applicant.Edrpou, applicant.edrpou),
      country: firstNonEmpty(applicant['I_71.C.U'], applicant['I_71.C.E'], applicant['I_71.C']),
    });
  }

  for (const applicant of applicantRows.filter((x) => x.name || x.edrpou)) {
    rows.push({
      patent_number: record.patent_number,
      application_number: record.application_number,
      role: 'APPLICANT',
      name: applicant.name,
      edrpou: applicant.edrpou,
      country: applicant.country,
      lang: '',
      address: '',
      address_source: '',
    });
  }

  return rows;
}

function csvEscape(value) {
  const s = Array.isArray(value) ? value.join(' | ') : String(value ?? '');
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

function countBy(items, key) {
  const map = new Map();
  for (const item of items) {
    const value = item[key] || '(empty)';
    map.set(value, (map.get(value) || 0) + 1);
  }
  return [...map.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
}

function escapeMd(value) {
  return String(value ?? '').replace(/\|/g, '\\|').replace(/\n/g, ' ');
}

function buildSummary({ args, fetchMeta, records }) {
  const withOwner = records.filter((x) => x.owner_patent_holder).length;
  const withAddress = records.filter((x) => x.address).length;
  const withTerminationDate = records.filter((x) => x.termination_or_cancellation_date).length;
  const statusStats = countBy(records, 'legal_status');
  const colorStats = countBy(records, 'status_raw_color');

  const lines = [];
  lines.push('# Patent Owner Extract Check — summary');
  lines.push('');
  lines.push(`- Object type: **${args.objType}**`);
  lines.push(`- Object state: **${args.objState}**`);
  lines.push(`- Date mode: **${args.dateMode}**`);

  if (args.objectNumber) {
    lines.push(`- Object number: **${args.objectNumber}**`);
  } else {
    lines.push(`- Date range: **${args.from} — ${args.to}**`);
  }

  lines.push(`- API total count: **${fetchMeta.totalCount ?? 'n/a'}**`);
  lines.push(`- Fetched pages: **${fetchMeta.fetchedPages}**`);
  lines.push(`- Fetched objects: **${fetchMeta.fetchedCount}**`);

  if (fetchMeta.stoppedByMaxPages) lines.push('- Stopped by max_pages: **yes**');

  lines.push('');
  lines.push(`- Records with owner/patent holder: **${withOwner} / ${records.length}**`);
  lines.push(`- Records with address: **${withAddress} / ${records.length}**`);
  lines.push(`- Records with detected termination/cancellation date: **${withTerminationDate} / ${records.length}**`);
  lines.push('');

  lines.push('## Status breakdown');
  lines.push('');
  lines.push('| Status | Count |');
  lines.push('|---|---:|');
  for (const [key, count] of statusStats) lines.push(`| ${escapeMd(key)} | ${count} |`);
  lines.push('');

  lines.push('## Raw status color breakdown');
  lines.push('');
  lines.push('| registration_status_color | Count |');
  lines.push('|---|---:|');
  for (const [key, count] of colorStats) lines.push(`| ${escapeMd(key)} | ${count} |`);
  lines.push('');

  lines.push('## First records');
  lines.push('');
  lines.push('| Patent no | Reg date | Title | Owner | Address source | Status | Termination date |');
  lines.push('|---|---|---|---|---|---|---|');

  for (const r of records.slice(0, 50)) {
    lines.push(
      `| ${escapeMd(r.patent_number)} | ${escapeMd(r.registration_date)} | ${escapeMd(r.title).slice(0, 120)} | ${escapeMd(r.owner_patent_holder).slice(0, 120)} | ${escapeMd(r.address_source)} | ${escapeMd(r.legal_status)} | ${escapeMd(r.termination_or_cancellation_date)} |`,
    );
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

  const fetchMeta = await fetchObjects(args);
  const records = fetchMeta.objects.map(normalizeObject);
  const partyRows = fetchMeta.objects.flatMap((item, idx) => extractPartyRows(item, records[idx]));

  const recordHeaders = [
    'object_type',
    'obj_type_id',
    'obj_state',
    'patent_kind',
    'patent_number',
    'application_number',
    'application_date',
    'registration_date',
    'title',
    'owner_patent_holder',
    'owner_edrpou',
    'owner_country',
    'address',
    'address_source',
    'legal_status',
    'status_label_uk',
    'status_confidence',
    'status_raw_color',
    'status_evidence',
    'termination_or_cancellation_date',
    'current_stage',
    'ipc',
    'last_update',
  ];

  const partyHeaders = [
    'patent_number',
    'application_number',
    'role',
    'name',
    'edrpou',
    'country',
    'lang',
    'address',
    'address_source',
  ];

  await writeJson(path.join(OUT_DIR, 'raw_objects.json'), fetchMeta.objects);
  await writeJson(path.join(OUT_DIR, 'patent_records.json'), records);
  await writeJson(path.join(OUT_DIR, 'patent_parties.json'), partyRows);

  await fs.writeFile(path.join(OUT_DIR, 'patent_records.csv'), toCsv(records, recordHeaders), 'utf8');
  await fs.writeFile(path.join(OUT_DIR, 'patent_parties.csv'), toCsv(partyRows, partyHeaders), 'utf8');
  await fs.writeFile(path.join(OUT_DIR, 'summary.md'), buildSummary({ args, fetchMeta, records }), 'utf8');

  console.log(`Done. Objects=${fetchMeta.fetchedCount}, records=${records.length}, parties=${partyRows.length}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
