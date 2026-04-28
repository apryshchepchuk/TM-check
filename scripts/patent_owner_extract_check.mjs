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
  1: 'Винаходи',
  2: 'Корисні моделі',
  6: 'Промислові зразки',
};

const OBJ_STATE_LABELS = {
  1: 'Заявка',
  2: 'Охоронний документ',
  all: 'Усі стани',
};

const PARTY_CONFIGS = [
  {
    role: 'HOLDER',
    label: 'Власник / володілець прав',
    detailKeys: [
      'HolderDetails',
      'OwnerDetails',
      'PatentHolderDetails',
      'RightHolderDetails',
      'RightOwnerDetails',
      'GranteeDetails',
    ],
    partyKeys: [
      'Holder',
      'Owner',
      'PatentHolder',
      'RightHolder',
      'RightOwner',
      'Grantee',
    ],
  },
  {
    role: 'APPLICANT',
    label: 'Заявник',
    detailKeys: [
      'ApplicantDetails',
      'ApplicationApplicantDetails',
    ],
    partyKeys: [
      'Applicant',
      'ApplicationApplicant',
    ],
  },
  {
    role: 'INVENTOR',
    label: 'Винахідник / автор',
    detailKeys: [
      'InventorDetails',
      'DesignerDetails',
      'CreatorDetails',
      'AuthorDetails',
    ],
    partyKeys: [
      'Inventor',
      'Designer',
      'Creator',
      'Author',
    ],
  },
  {
    role: 'REPRESENTATIVE',
    label: 'Представник',
    detailKeys: [
      'RepresentativeDetails',
      'AgentDetails',
      'AttorneyDetails',
    ],
    partyKeys: [
      'Representative',
      'Agent',
      'Attorney',
    ],
  },
  {
    role: 'CORRESPONDENCE',
    label: 'Адреса для листування',
    detailKeys: [
      'CorrespondenceAddress',
      'CorrespondenceDetails',
    ],
    partyKeys: [
      'CorrespondenceAddressBook',
      'CorrespondenceAddress',
    ],
  },
];

const NAME_KEYS = [
  'FreeFormatNameLine',
  'OrganizationName',
  'LegalEntityName',
  'CompanyName',
  'IndividualName',
  'PersonName',
  'FormattedName',
  'NameLine',
  'PartyName',
  'FullName',
  'LastName',
  'FirstName',
  'MiddleName',
  'ApplicantName',
  'HolderName',
  'OwnerName',
  'InventorName',
  'RepresentativeName',
];

const ADDRESS_LINE_KEYS = [
  'FreeFormatAddressLine',
  'AddressLine',
  'StreetAddress',
  'AddressStreet',
  'Street',
  'AddressCity',
  'City',
  'AddressRegion',
  'Region',
  'State',
  'District',
];

const COUNTRY_KEYS = [
  'AddressCountryCode',
  'CountryCode',
  'Country',
];

const POSTCODE_KEYS = [
  'AddressPostcode',
  'Postcode',
  'PostalCode',
  'ZipCode',
];

const ID_KEYS = [
  'EDRPOU',
  'EDRPOUCode',
  'Edrpou',
  'edrpou',
  'Identifier',
  'RegistrationNumber',
  'NationalNumber',
];

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

function asArray(value) {
  if (value === null || value === undefined) return [];
  return Array.isArray(value) ? value : [value];
}

function getTextValue(value) {
  if (value === null || value === undefined) return '';

  if (typeof value === 'string' || typeof value === 'number') {
    return String(value).replace(/\s+/g, ' ').trim();
  }

  if (typeof value === 'object') {
    if (typeof value['#text'] === 'string') return value['#text'].replace(/\s+/g, ' ').trim();
    if (typeof value.text === 'string') return value.text.replace(/\s+/g, ' ').trim();
    if (typeof value.value === 'string') return value.value.replace(/\s+/g, ' ').trim();
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

function findValuesByKeyDeep(obj, targetKeys, maxDepth = 14) {
  const target = new Set(targetKeys);
  const values = [];
  const stack = [{ value: obj, path: '', depth: 0 }];
  const seen = new WeakSet();

  while (stack.length) {
    const { value, path: curPath, depth } = stack.pop();

    if (value === null || value === undefined || depth > maxDepth) continue;

    if (typeof value === 'object') {
      if (seen.has(value)) continue;
      seen.add(value);

      if (Array.isArray(value)) {
        value.forEach((item, idx) => {
          stack.push({ value: item, path: `${curPath}[${idx}]`, depth: depth + 1 });
        });
      } else {
        for (const [k, v] of Object.entries(value)) {
          const nextPath = curPath ? `${curPath}.${k}` : k;

          if (target.has(k)) {
            values.push({ key: k, path: nextPath, value: v });
          }

          stack.push({ value: v, path: nextPath, depth: depth + 1 });
        }
      }
    }
  }

  return values;
}

function collectKeyPaths(obj, predicate, maxDepth = 14) {
  const matches = [];
  const stack = [{ value: obj, path: '', depth: 0 }];
  const seen = new WeakSet();

  while (stack.length) {
    const { value, path: curPath, depth } = stack.pop();

    if (value === null || value === undefined || depth > maxDepth) continue;

    if (typeof value === 'object') {
      if (seen.has(value)) continue;
      seen.add(value);

      if (Array.isArray(value)) {
        value.forEach((item, idx) => {
          stack.push({ value: item, path: `${curPath}[${idx}]`, depth: depth + 1 });
        });
      } else {
        for (const [k, v] of Object.entries(value)) {
          const nextPath = curPath ? `${curPath}.${k}` : k;

          if (predicate(k, nextPath, v)) {
            matches.push({ key: k, path: nextPath, sample_value: previewValue(v) });
          }

          stack.push({ value: v, path: nextPath, depth: depth + 1 });
        }
      }
    }
  }

  return matches;
}

function previewValue(value) {
  const text = getTextValue(value);
  if (text) return text.slice(0, 250);

  if (Array.isArray(value)) return `[array:${value.length}]`;
  if (value && typeof value === 'object') return `{object:${Object.keys(value).slice(0, 12).join(',')}}`;
  return String(value ?? '').slice(0, 250);
}

function buildInitialUrl(args) {
  if (args.objectNumber) {
    return `${API_BASE}${encodeURIComponent(args.objectNumber)}/`;
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
      'user-agent': 'patent-owner-extract-check/0.1 GitHub Actions',
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

function normalizeFetchedPayload(json) {
  if (Array.isArray(json?.results)) {
    return {
      totalCount: json.count ?? null,
      results: json.results,
      next: json.next || null,
    };
  }

  if (Array.isArray(json)) {
    return {
      totalCount: json.length,
      results: json,
      next: null,
    };
  }

  if (json && typeof json === 'object') {
    return {
      totalCount: 1,
      results: [json],
      next: null,
    };
  }

  return {
    totalCount: 0,
    results: [],
    next: null,
  };
}

async function fetchObjects(args) {
  let url = buildInitialUrl(args);
  const objects = [];
  let page = 0;
  let totalCount = null;
  let stoppedByMaxPages = false;

  while (url) {
    page += 1;

    if (!args.objectNumber && args.maxPages > 0 && page > args.maxPages) {
      stoppedByMaxPages = true;
      console.log(`Stopped by max_pages=${args.maxPages}`);
      break;
    }

    console.log(`Fetching page ${page}: ${url}`);

    const json = await fetchJson(url);
    const payload = normalizeFetchedPayload(json);

    if (totalCount === null) {
      totalCount = payload.totalCount;
    }

    objects.push(...payload.results);
    url = args.objectNumber ? null : payload.next;

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

function toShortDate(value) {
  if (!value) return '';

  const s = String(value);
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);

  if (m) return `${m[3]}.${m[2]}.${m[1]}`;

  return s;
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

function extractTitle(item) {
  const data = item?.data || {};

  const titleCandidates = [
    ...findValuesByKeyDeep(data, [
      'InventionTitle',
      'InventionTitleText',
      'TitleOfInvention',
      'UtilityModelTitle',
      'IndustrialDesignTitle',
      'DesignTitle',
      'PatentTitle',
      'Title',
      'Name',
    ], 10).map((x) => getTextValue(x.value)),
  ];

  // Не беремо очевидні службові назви стадій або типів, якщо є довший змістовний варіант.
  const cleaned = uniqueStrings(titleCandidates)
    .filter((s) => s.length > 2)
    .filter((s) => !['current', 'active', 'not-active'].includes(s.toLowerCase()));

  return cleaned.sort((a, b) => b.length - a.length)[0] || '';
}

function extractCountry(party) {
  return uniqueStrings(
    findValuesByKeyDeep(party, COUNTRY_KEYS, 8)
      .map((x) => getTextValue(x.value))
      .filter(Boolean),
  ).join(' | ');
}

function extractPostcode(party) {
  return uniqueStrings(
    findValuesByKeyDeep(party, POSTCODE_KEYS, 8)
      .map((x) => getTextValue(x.value))
      .filter(Boolean),
  ).join(' | ');
}

function extractIds(party) {
  const values = findValuesByKeyDeep(party, ID_KEYS, 10)
    .map((x) => getTextValue(x.value))
    .filter(Boolean);

  const deepEdrpou = collectKeyPaths(
    party,
    (key) => key.toLowerCase().includes('edrpou') || key.toLowerCase().includes('єдрпоу'),
    10,
  ).map((x) => x.sample_value).filter(Boolean);

  return uniqueStrings([...values, ...deepEdrpou]);
}

function extractNameLines(party) {
  if (typeof party === 'string' || typeof party === 'number') {
    return [String(party).trim()].filter(Boolean);
  }

  const values = findValuesByKeyDeep(party, NAME_KEYS, 10)
    .flatMap((x) => asArray(x.value))
    .map(getTextValue)
    .filter(Boolean);

  // Додатково пробуємо типові вкладені блоки AddressBook/Name.
  const addressBooks = findValuesByKeyDeep(party, [
    'ApplicantAddressBook',
    'HolderAddressBook',
    'OwnerAddressBook',
    'PatentHolderAddressBook',
    'RepresentativeAddressBook',
    'InventorAddressBook',
    'DesignerAddressBook',
    'AddressBook',
  ], 8);

  for (const ab of addressBooks) {
    const nameBlock =
      getByPath(ab.value, ['FormattedNameAddress', 'Name'])
      ?? getByPath(ab.value, ['Name'])
      ?? null;

    if (nameBlock) {
      values.push(
        ...findValuesByKeyDeep(nameBlock, NAME_KEYS, 8)
          .flatMap((x) => asArray(x.value))
          .map(getTextValue)
          .filter(Boolean),
      );
    }
  }

  return uniqueStrings(values);
}

function extractAddressLines(party) {
  const values = findValuesByKeyDeep(party, ADDRESS_LINE_KEYS, 10)
    .flatMap((x) => asArray(x.value))
    .map(getTextValue)
    .filter(Boolean);

  return uniqueStrings(values);
}

function extractPartyFromNode(node, role, sourceKey, sourcePath) {
  const names = extractNameLines(node);
  const addressLines = extractAddressLines(node);
  const ids = extractIds(node);
  const country = extractCountry(node);
  const postcode = extractPostcode(node);

  const hasUsefulData = names.length || addressLines.length || ids.length || country || postcode;

  if (!hasUsefulData) return null;

  const edrpou = ids.find((x) => /^\d{8,10}$/.test(x.replace(/\D+/g, '')))
    || ids.find((x) => x.toLowerCase().includes('edrpou') || x.toLowerCase().includes('єдрпоу'))
    || '';

  return {
    role,
    source_key: sourceKey,
    source_path: sourcePath,
    name: names.join(' | '),
    name_lines: names,
    edrpou,
    identifiers: ids,
    country,
    postcode,
    address: addressLines.join(' | '),
    address_lines: addressLines,
  };
}

function extractPartiesForRole(data, config) {
  const parties = [];

  const detailNodes = findValuesByKeyDeep(data, config.detailKeys, 8);
  const directPartyNodes = findValuesByKeyDeep(data, config.partyKeys, 8);

  // 1) Якщо є конкретні вузли Holder/Applicant/Inventor то беремо їх.
  for (const found of directPartyNodes) {
    for (const node of asArray(found.value)) {
      const party = extractPartyFromNode(node, config.role, found.key, found.path);
      if (party) parties.push(party);
    }
  }

  // 2) Якщо конкретних вузлів немає, але є HolderDetails/ApplicantDetails, пробуємо витягнути з них.
  if (!parties.length) {
    for (const found of detailNodes) {
      for (const node of asArray(found.value)) {
        const party = extractPartyFromNode(node, config.role, found.key, found.path);
        if (party) parties.push(party);
      }
    }
  }

  return dedupeParties(parties);
}

function partyDedupeKey(party) {
  return [
    party.role,
    party.name.toLowerCase(),
    party.edrpou.toLowerCase(),
    party.address.toLowerCase(),
    party.source_key,
  ].join('||');
}

function dedupeParties(parties) {
  const seen = new Set();
  const out = [];

  for (const party of parties) {
    const key = partyDedupeKey(party);
    if (seen.has(key)) continue;

    seen.add(key);
    out.push(party);
  }

  return out;
}

function extractAllParties(item) {
  const data = item?.data || {};
  const parties = [];

  for (const config of PARTY_CONFIGS) {
    parties.push(...extractPartiesForRole(data, config));
  }

  return dedupeParties(parties);
}

function getPartiesByRole(parties, role) {
  return parties.filter((p) => p.role === role);
}

function joinPartyNames(parties) {
  return uniqueStrings(parties.map((p) => p.name).filter(Boolean)).join(' | ');
}

function joinPartyAddresses(parties) {
  return uniqueStrings(parties.map((p) => p.address).filter(Boolean)).join(' | ');
}

function joinPartyEdrpou(parties) {
  return uniqueStrings(parties.map((p) => p.edrpou).filter(Boolean)).join(' | ');
}

function normalizeObject(item, index) {
  const parties = extractAllParties(item);
  const holders = getPartiesByRole(parties, 'HOLDER');
  const applicants = getPartiesByRole(parties, 'APPLICANT');
  const inventors = getPartiesByRole(parties, 'INVENTOR');
  const representatives = getPartiesByRole(parties, 'REPRESENTATIVE');
  const correspondence = getPartiesByRole(parties, 'CORRESPONDENCE');

  return {
    row_id: index + 1,
    obj_type: item?.obj_type || '',
    obj_type_id: item?.obj_type_id ?? '',
    obj_state: item?.obj_state || '',
    obj_state_id: item?.obj_state_id ?? '',
    app_number: String(item?.app_number || '').trim(),
    app_date: toShortDate(item?.app_date),
    registration_number: String(item?.registration_number || '').trim(),
    registration_date: toShortDate(item?.registration_date),
    last_update: String(item?.last_update || '').trim(),
    application_status: String(item?.data?.application_status || '').trim(),
    current_stage: extractCurrentStage(item),
    title: extractTitle(item),
    parties,
    holder_names: joinPartyNames(holders),
    holder_edrpou: joinPartyEdrpou(holders),
    holder_addresses: joinPartyAddresses(holders),
    applicant_names: joinPartyNames(applicants),
    applicant_edrpou: joinPartyEdrpou(applicants),
    applicant_addresses: joinPartyAddresses(applicants),
    inventor_names: joinPartyNames(inventors),
    representative_names: joinPartyNames(representatives),
    correspondence_names: joinPartyNames(correspondence),
    correspondence_addresses: joinPartyAddresses(correspondence),
    parties_count: parties.length,
    holders_count: holders.length,
    applicants_count: applicants.length,
    inventors_count: inventors.length,
    representatives_count: representatives.length,
  };
}

function buildPartyRows(normalized) {
  const rows = [];

  for (const obj of normalized) {
    for (const party of obj.parties) {
      rows.push({
        row_id: obj.row_id,
        obj_type: obj.obj_type,
        obj_type_id: obj.obj_type_id,
        obj_state: obj.obj_state,
        obj_state_id: obj.obj_state_id,
        app_number: obj.app_number,
        app_date: obj.app_date,
        registration_number: obj.registration_number,
        registration_date: obj.registration_date,
        last_update: obj.last_update,
        title: obj.title,
        current_stage: obj.current_stage,
        role: party.role,
        source_key: party.source_key,
        source_path: party.source_path,
        name: party.name,
        edrpou: party.edrpou,
        identifiers: party.identifiers.join(' | '),
        country: party.country,
        postcode: party.postcode,
        address: party.address,
      });
    }
  }

  return rows;
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

function escapeMd(value) {
  return String(value ?? '')
    .replace(/\|/g, '\\|')
    .replace(/\n/g, ' ');
}

function buildDetectedPartyKeys(rawObjects) {
  return rawObjects.slice(0, 20).map((item, idx) => {
    const data = item?.data || {};
    const keys = collectKeyPaths(
      data,
      (key) => {
        const k = key.toLowerCase();
        return k.includes('holder')
          || k.includes('owner')
          || k.includes('applicant')
          || k.includes('inventor')
          || k.includes('designer')
          || k.includes('representative')
          || k.includes('correspondence')
          || k.includes('addressbook')
          || k.includes('edrpou');
      },
      14,
    );

    return {
      sample_index: idx + 1,
      app_number: item?.app_number || '',
      registration_number: item?.registration_number || '',
      detected_keys: keys.slice(0, 400),
    };
  });
}

function buildSummary({ args, fetchMeta, normalized, partyRows }) {
  const total = normalized.length;
  const withAnyParty = normalized.filter((x) => x.parties_count > 0).length;
  const withHolder = normalized.filter((x) => x.holders_count > 0).length;
  const withApplicant = normalized.filter((x) => x.applicants_count > 0).length;
  const withInventor = normalized.filter((x) => x.inventors_count > 0).length;
  const withRepresentative = normalized.filter((x) => x.representatives_count > 0).length;

  const stateStats = countBy(normalized, (x) => x.obj_state || String(x.obj_state_id || ''));
  const roleStats = countBy(partyRows, (x) => x.role);

  const lines = [];

  lines.push('# Patent Owner Extract Check — summary');
  lines.push('');
  lines.push(`- Object type: **${args.objType} / ${OBJ_TYPE_LABELS[args.objType] || ''}**`);
  lines.push(`- Object state: **${args.objState} / ${OBJ_STATE_LABELS[args.objState] || ''}**`);

  if (args.objectNumber) {
    lines.push(`- Object number: **${args.objectNumber}**`);
  } else {
    lines.push(`- Date mode: **${args.dateMode}**`);
    lines.push(`- Date range: **${args.from} — ${args.to}**`);
  }

  lines.push(`- API total count: **${fetchMeta.totalCount ?? 'n/a'}**`);
  lines.push(`- Fetched pages: **${fetchMeta.fetchedPages}**`);
  lines.push(`- Fetched objects: **${fetchMeta.fetchedCount}**`);

  if (fetchMeta.stoppedByMaxPages) {
    lines.push('- Stopped by max_pages: **yes**');
  }

  lines.push('');
  lines.push(`- Objects with any extracted party: **${withAnyParty} / ${total}**`);
  lines.push(`- Objects with extracted holder/owner: **${withHolder} / ${total}**`);
  lines.push(`- Objects with extracted applicant: **${withApplicant} / ${total}**`);
  lines.push(`- Objects with extracted inventor/designer: **${withInventor} / ${total}**`);
  lines.push(`- Objects with extracted representative: **${withRepresentative} / ${total}**`);
  lines.push(`- Extracted party rows: **${partyRows.length}**`);
  lines.push('');

  lines.push('## Object state breakdown');
  lines.push('');
  lines.push('| State | Count |');
  lines.push('|---|---:|');
  for (const row of stateStats) {
    lines.push(`| ${escapeMd(row.key)} | ${row.count} |`);
  }
  lines.push('');

  lines.push('## Party role breakdown');
  lines.push('');
  lines.push('| Role | Count |');
  lines.push('|---|---:|');
  for (const row of roleStats) {
    lines.push(`| ${escapeMd(row.key)} | ${row.count} |`);
  }
  lines.push('');

  const examples = normalized.filter((x) => x.parties_count > 0).slice(0, 50);

  if (examples.length) {
    lines.push('## Examples');
    lines.push('');
    lines.push('| App no | Reg no | Reg date | Title | Holder / owner | Holder address | Applicant |');
    lines.push('|---|---|---|---|---|---|---|');

    for (const item of examples) {
      lines.push(
        `| ${escapeMd(item.app_number)} | ${escapeMd(item.registration_number)} | ${escapeMd(item.registration_date)} | ${escapeMd(item.title).slice(0, 140)} | ${escapeMd(item.holder_names).slice(0, 180)} | ${escapeMd(item.holder_addresses).slice(0, 180)} | ${escapeMd(item.applicant_names).slice(0, 180)} |`,
      );
    }
  } else {
    lines.push('No parties were extracted. Check `detected_party_keys.json` and `raw_objects.json` to adjust field mapping.');
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

  console.log('Fetching objects...');
  const fetchMeta = await fetchObjects(args);

  console.log('Normalizing objects and extracting parties...');
  const normalized = fetchMeta.objects.map(normalizeObject);
  const partyRows = buildPartyRows(normalized);
  const detectedPartyKeys = buildDetectedPartyKeys(fetchMeta.objects);

  const objectHeaders = [
    'row_id',
    'obj_type',
    'obj_type_id',
    'obj_state',
    'obj_state_id',
    'app_number',
    'app_date',
    'registration_number',
    'registration_date',
    'last_update',
    'application_status',
    'current_stage',
    'title',
    'holder_names',
    'holder_edrpou',
    'holder_addresses',
    'applicant_names',
    'applicant_edrpou',
    'applicant_addresses',
    'inventor_names',
    'representative_names',
    'correspondence_names',
    'correspondence_addresses',
    'parties_count',
    'holders_count',
    'applicants_count',
    'inventors_count',
    'representatives_count',
  ];

  const partyHeaders = [
    'row_id',
    'obj_type',
    'obj_type_id',
    'obj_state',
    'obj_state_id',
    'app_number',
    'app_date',
    'registration_number',
    'registration_date',
    'last_update',
    'title',
    'current_stage',
    'role',
    'source_key',
    'source_path',
    'name',
    'edrpou',
    'identifiers',
    'country',
    'postcode',
    'address',
  ];

  await writeJson(path.join(OUT_DIR, 'raw_objects.json'), fetchMeta.objects);
  await writeJson(path.join(OUT_DIR, 'patent_owner_extract.json'), normalized);
  await writeJson(path.join(OUT_DIR, 'patent_party_rows.json'), partyRows);
  await writeJson(path.join(OUT_DIR, 'detected_party_keys.json'), detectedPartyKeys);

  await fs.writeFile(
    path.join(OUT_DIR, 'patent_objects.csv'),
    toCsv(normalized, objectHeaders),
    'utf8',
  );

  await fs.writeFile(
    path.join(OUT_DIR, 'patent_parties.csv'),
    toCsv(partyRows, partyHeaders),
    'utf8',
  );

  await fs.writeFile(
    path.join(OUT_DIR, 'summary.md'),
    buildSummary({ args, fetchMeta, normalized, partyRows }),
    'utf8',
  );

  console.log(
    `Done. Objects=${normalized.length}, party_rows=${partyRows.length}`,
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
