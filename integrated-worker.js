import { createClient } from "@libsql/client/web";
let requestCounter = 0;
let subrequestCounter = 0;
const CHARACTER_FETCH_LIMIT = 40;
const MAX_REQUESTS = 49;
const SUBREQUEST_LIMIT = 49;
const COMMIT_THRESHOLD = 40;
const MAX_CHUNKS_PER_RUN = 8;
const GIT_BUFFER_KV_KEY = "git_buffer_files";
const GIT_BUFFER_COUNT_KEY = "git_buffer_count";
const GIT_BUFFER_FLUSH_THRESHOLD = 500;
const MAX_FLUSH_FILES = 500;
async function getBuffer(env) {
  try {
    const raw = await env.PROGRESS_KV.get(GIT_BUFFER_KV_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch (e) {
    console.error("getBuffer error:", e);
    return [];
  }
}
async function saveBuffer(env, arr) {
  try {
    await env.PROGRESS_KV.put(GIT_BUFFER_KV_KEY, JSON.stringify(arr));
    await env.PROGRESS_KV.put(GIT_BUFFER_COUNT_KEY, String(arr.length));
  } catch (e) {
    console.error("saveBuffer error:", e);
    throw e;
  }
}
function guard() {
  requestCounter++;
  if (requestCounter >= MAX_REQUESTS) {
    console.log("Request limit approaching. SAFE_STOP.");
    throw new Error("SAFE_STOP");
  }
}
function guardSub() {
  subrequestCounter++;
  if (subrequestCounter >= SUBREQUEST_LIMIT) {
    console.log("Subrequest limit approaching. SAFE_STOP.");
    throw new Error("SAFE_STOP");
  }
}
async function countedFetch(url, options) {
  guardSub();
  try {
    console.log(`[countedFetch] #${subrequestCounter} ->`, url);
    const response = await fetch(url, options); 
    return response;
  } catch (e) {
    console.error('[countedFetch] error:', e);
    throw e;
  }
}
function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }
export default {
async scheduled(event, env, ctx) {
  requestCounter = 0;
  subrequestCounter = 0;
  console.log("Scheduled run:", new Date().toISOString());
  const db = createClient({
    url: env.LIBSQL_DB_URL,
    authToken: env.LIBSQL_DB_AUTH_TOKEN
  });
  await runScheduled(env, db);
},
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);
      if (request.method === "POST" && url.pathname === "/run") {
        const db = createClient({
          url: env.LIBSQL_DB_URL,
          authToken: env.LIBSQL_DB_AUTH_TOKEN
        });
        ctx.waitUntil(runScheduled(env, db));
        return new Response("Run queued", { status: 202 });
      }
      return new Response("OK", { status: 200 });
    } catch (err) {
      console.error("fetch handler error:", err);
      return new Response("Internal error", { status: 500 });
    }
  }
};
async function getAnimeProgress(env) {
  const v = await env.PROGRESS_KV.get("anime_progress");
  return v ? parseInt(v) : 0;
}
async function saveAnimeProgress(env, id) {
  await env.PROGRESS_KV.put("anime_progress", String(id));
}
async function getProgress(env, animeId) {
  const v = await env.PROGRESS_KV.get(`progress:${animeId}`);
  return v ? parseInt(v) : 0;
}
async function saveProgress(env, animeId, idx) {
  await env.PROGRESS_KV.put(`progress:${animeId}`, String(idx));
}
async function runScheduled(env, db) {
  let processedChunks = 0;
let lastId = (await getAnimeProgress(env)) || 0;
try {
  const allFilesFull = await getBuffer(env);
  const totalBuffer = allFilesFull.length;
  const isFlushMode = totalBuffer >= GIT_BUFFER_FLUSH_THRESHOLD;
  console.log(`FLUSH MODE CHECK: buffer=${totalBuffer}, flush=${isFlushMode}`);
  if (isFlushMode) {
    console.log(`FLUSH MODE ACTIVATED`);
    const allFiles = allFilesFull.slice(0, MAX_FLUSH_FILES);
    const remainingFiles = allFilesFull.slice(MAX_FLUSH_FILES);
    if (allFiles.length === 0) {
      console.log("Buffer truly empty");
    }
    const CHUNK_SIZE = 100;
    for (let i = 0; i < allFiles.length; i += CHUNK_SIZE) {
      if (processedChunks >= MAX_CHUNKS_PER_RUN) {
        console.log("Stopping early (subrequest safety)");
        continue;
      }
      const chunk = allFiles.slice(i, i + CHUNK_SIZE);
      env.__exportFiles = chunk;
      try {
        await commitExport(env);
        console.log(`Committed chunk ${i} → ${i + chunk.length}`);
        processedChunks++;
        await sleep(450);
      } catch (e) {
        console.error("Chunk commit failed:", e);
        break;
      }
    }
    await saveBuffer(env, remainingFiles);
    console.log(`Flush partial. Remaining buffer: ${remainingFiles.length}`);
  }
  let skipCount = 0;
const MAX_SKIP = 12;
for (let iteration = 0; iteration < 10; iteration++) {
  let payload = undefined;
  guard();
  try {
    const res = await db.execute({
      sql: 'SELECT mal_id, id, title FROM anime_info WHERE id > ? AND mal_id IS NOT NULL ORDER BY id LIMIT 1',
      args: [lastId]
    });
    if (res.rows.length === 0) {
      console.log("No more anime to process. Exiting run.");
      continue;
    }
    const anime = res.rows[0];
    lastId = anime.id;
    console.log("Processing anime:", anime.id, anime.title, anime.mal_id);
    if (!anime.mal_id || isNaN(Number(anime.mal_id))) {
      console.log("Invalid MAL id, skipping:", anime.mal_id);
      await saveAnimeProgress(env, anime.id);
      skipCount++;
      if (skipCount >= MAX_SKIP) {
        console.log("Skip limit reached");
        break;
      }
      continue;
    }
    try {
      const url = `https://api.jikan.moe/v4/anime/${anime.mal_id}/characters`;
      const charRes = await countedFetch(url);
      if (charRes.status === 404) {
        console.log("404 anime not found:", anime.mal_id);
        await saveAnimeProgress(env, anime.id);
        skipCount++;
        if (skipCount >= MAX_SKIP) break;
        continue;
      }
      if (!charRes.ok) {
        console.error("Jikan API error status:", charRes.status);
        await sleep(2000);
        continue;
      }
      let charData;
      try {
        charData = await charRes.json();
      } catch (err) {
        console.error("Invalid JSON from Jikan", err);
        continue;
      }
      if (!charData || !Array.isArray(charData.data) || charData.data.length === 0) {
        console.log("No characters found for anime:", anime.mal_id);
        await saveAnimeProgress(env, anime.id);
        skipCount++;
        if (skipCount >= MAX_SKIP) break;
        continue;
      }
      skipCount = 0;
      const startIndex = (await getProgress(env, anime.id)) || 0;
      const endIndex = Math.min(startIndex + CHARACTER_FETCH_LIMIT, charData.data.length);
      const slice = charData.data.slice(startIndex, endIndex);
      const charIds = slice.map(en => en.character && en.character.mal_id).filter(Boolean);
      if (charIds.length === 0) {
        if (endIndex >= charData.data.length) {
          guard();
          await env.PROGRESS_KV.delete(`progress:${anime.id}`);
          await saveAnimeProgress(env, anime.id);
        } else {
          await saveProgress(env, anime.id, endIndex);
        }
        continue;
      }
      const payload = {
        animeId: anime.id,
        animeMalId: anime.mal_id,
        characterMalIds: charIds,
        startIndex,
        endIndex,
        title: anime.title,
        last: endIndex >= charData.data.length
      };
      let processedCount = 0;
      let safeStopped = false;
      try {
        processedCount = await processCharactersBatch(payload, env);
      } catch (e) {
        if (e && e.message === "SAFE_STOP") {
          safeStopped = true;
          processedCount = e.successCount || 0;
        } else {
          console.error("Error in processCharactersBatch:", e);
        }
      }
      const actualEndIndex = startIndex + processedCount;
if (actualEndIndex >= charData.data.length) {
  await env.PROGRESS_KV.delete(`progress:${anime.id}`);
  await saveAnimeProgress(env, anime.id);
} else {
  await saveProgress(env, anime.id, actualEndIndex);
}
      if (safeStopped) {
        console.log("SAFE_STOP triggered; breaking out of anime loop cleanly");
        break;
      }
    } catch (e) {
      console.error("Error in processCharactersBatch:", e);
    }
    break;
  } catch (err) {
    if (err && err.message === "SAFE_STOP") {
      console.log("SAFE_STOP triggered; aborting run");
      break;
    }
    console.error("runScheduled error:", err);
  }
}
} catch (err) {
  if (err && err.message === "SAFE_STOP") {
    console.log("SAFE_STOP triggered; aborting run");
  } else {
    console.error("runScheduled outer error:", err);
  }
} finally {
  if (env.__kvBuffer && env.__kvBuffer.length > 0) {
    try {
      await saveBuffer(env, env.__kvBuffer);
      console.log("Saved buffer to KV:", env.__kvBuffer.length);
    } catch (e) {
      console.error("Failed saving KV buffer:", e);
    }
  }
}
}
async function processCharactersBatch(body, env) {
  guard();
  if (!body) throw new Error("No body");
  const animeId = body.animeId;
  const animeMalId = body.animeMalId;
  const title = body.title || "";
  let charIds = [];
  if (Array.isArray(body.characterMalIds)) charIds = body.characterMalIds;
  else if (body.characterMalId) charIds = [body.characterMalId];
  if (!animeId || charIds.length === 0) {
    throw new Error("No characters to process");
  }
  let successCount = 0; 
  for (const characterId of charIds) {
    try {
const full = await fetchCharacterDetails(characterId, env);
if (!full || Object.keys(full).length === 0) {
  console.log("Skipping empty character:", characterId);
  continue;
}

const payload = {
  animeId,
  animeMalId,
  animeTitle: title,
  character: {
    mal_character_id: characterId,
    ...full
  }
};

await processSingleCharacterPayload(payload, env);
successCount++; 
      await sleep(400);
    } catch (e) {
      if (e.message === "SAFE_STOP") {
        console.log("SAFE_STOP in processCharactersBatch");
        e.successCount = successCount; // Attach count to the error
        throw e;
      }
      console.error("processCharactersBatch character error:", e);
    }
  }
  return successCount;
}
async function extractInformation(input, existing = {}, options = {}) {
  const opts = {
    preferExisting: true,   
    keepRaw: false,         
    ...options,
  };

  const isPlainObject = (v) =>
    v !== null && typeof v === "object" && !Array.isArray(v);

  const toText = (v) => {
    if (v == null) return "";
    if (typeof v === "string") return v.trim();
    if (typeof v === "number" || typeof v === "boolean") return String(v).trim();
    try {
      return JSON.stringify(v);
    } catch {
      return String(v).trim();
    }
  };

  const safeJsonParse = (value) => {
    if (value == null) return null;
    if (isPlainObject(value) || Array.isArray(value)) return value;
    if (typeof value !== "string") return value;

    const text = value.trim();
    if (!text) return null;

    try {
      return JSON.parse(text);
    } catch {
      return null;
    }
  };

  const deepClone = (v) => {
    if (v == null) return v;
    try {
      return structuredClone(v);
    } catch {
      try {
        return JSON.parse(JSON.stringify(v));
      } catch {
        return v;
      }
    }
  };

  const isUsableValue = (v) => {
    if (v == null) return false;
    if (typeof v === "string") return v.trim() !== "";
    if (Array.isArray(v)) return v.length > 0;
    if (isPlainObject(v)) return Object.keys(v).length > 0;
    return true;
  };

  const normalizeString = (v) => {
    if (v == null) return null;
    const s = toText(v);
    return s ? s : null;
  };

  const normalizeNumber = (v) => {
    if (v == null || v === "") return null;
    if (typeof v === "number" && Number.isFinite(v)) return v;
    const n = Number(String(v).replace(/[^\d.-]/g, ""));
    return Number.isFinite(n) ? n : null;
  };

  const normalizeArray = (v) => {
    if (v == null) return null;
    if (Array.isArray(v)) {
      const out = [...new Set(v.map((x) => normalizeString(x)).filter(Boolean))];
      return out.length ? out : null;
    }
    if (typeof v === "string") {
      const text = v.trim();
      if (!text) return null;
      
      const parsed = safeJsonParse(text);
      if (Array.isArray(parsed)) return normalizeArray(parsed);
      
      const parts = text
        .split(/\s*(?:,|;|\||\/|\n)\s*/g)
        .map((x) => normalizeString(x))
        .filter(Boolean);

      const out = [...new Set(parts)];
      return out.length ? out : null;
    }
    return [normalizeString(v)].filter(Boolean);
  };

  const mergeArrays = (a, b) => {
    const left = normalizeArray(a) || [];
    const right = normalizeArray(b) || [];
    const out = [...new Set([...left, ...right])];
    return out.length ? out : null;
  };

  const chooseValue = (existingValue, extractedValue) => {
    const hasExisting = isUsableValue(existingValue);
    const hasExtracted = isUsableValue(extractedValue);

    if (opts.preferExisting) {
      if (hasExisting) return deepClone(existingValue);
      if (hasExtracted) return deepClone(extractedValue);
      return null;
    }

    if (hasExtracted) return deepClone(extractedValue);
    if (hasExisting) return deepClone(existingValue);
    return null;
  };

  const parseAbout = (aboutText) => {
    const text = toText(aboutText);
    const clean = text
      .replace(/\r/g, "\n")
      .replace(/\n{3,}/g, "\n\n")
      .trim();

    if (!clean) {
      return {
        cleaned_about: null,
        gender: null,
        birth_date: null,
        height_cm: null,
        weight_kg: null,
        blood_type: null,
        affiliations: null,
        abilities: null,
      };
    }

    const lower = clean.toLowerCase();

    const findFirst = (patterns) => {
      for (const re of patterns) {
        const m = clean.match(re);
        if (m?.[1]) return normalizeString(m[1]);
      }
      return null;
    };

    const gender =
      findFirst([
        /(?:^|\n)\s*gender\s*[:：]\s*([^\n]+)/i,
        /\bmale\b/i,
        /\bfemale\b/i,
        /\bnon-binary\b/i,
        /\bintersex\b/i,
      ]) ||
      (/\bmale\b/i.test(lower) ? "Male" : null) ||
      (/\bfemale\b/i.test(lower) ? "Female" : null);

    const birth_date =
      findFirst([
        /(?:^|\n)\s*(?:birth\s*date|birthday|born)\s*[:：]\s*([^\n]+)/i,
      ]) || null;

    const heightRaw =
      findFirst([
        /(?:^|\n)\s*height\s*[:：]\s*([^\n]+)/i,
        /\b(\d+(?:\.\d+)?)\s*cm\b/i,
        /\b(\d+)\s*centimet(?:er|re)s?\b/i,
      ]) || null;

    const weightRaw =
      findFirst([
        /(?:^|\n)\s*weight\s*[:：]\s*([^\n]+)/i,
        /\b(\d+(?:\.\d+)?)\s*kg\b/i,
        /\b(\d+(?:\.\d+)?)\s*kilogram(?:s)?\b/i,
      ]) || null;

    const blood_type =
      findFirst([
        /(?:^|\n)\s*blood\s*type\s*[:：]\s*([^\n]+)/i,
        /\btype\s*([aboh][+-]?)\b/i,
      ]) || null;

    const affiliations =
      findFirst([
        /(?:^|\n)\s*affiliations?\s*[:：]\s*([^\n]+)/i,
        /(?:^|\n)\s*organization\s*[:：]\s*([^\n]+)/i,
        /(?:^|\n)\s*groups?\s*[:：]\s*([^\n]+)/i,
      ]) || null;

    const abilities =
      findFirst([
        /(?:^|\n)\s*abilities?\s*[:：]\s*([^\n]+)/i,
        /(?:^|\n)\s*powers?\s*[:：]\s*([^\n]+)/i,
        /(?:^|\n)\s*skills?\s*[:：]\s*([^\n]+)/i,
      ]) || null;

    return {
      cleaned_about: clean,
      gender,
      birth_date,
      height_cm: normalizeNumber(heightRaw),
      weight_kg: normalizeNumber(weightRaw),
      blood_type,
      affiliations: normalizeArray(affiliations),
      abilities: normalizeArray(abilities),
    };
  };

  const resolveSourceObject = (value) => {
    const parsed = safeJsonParse(value);

    if (parsed && isPlainObject(parsed)) {
      // Common shapes:
      // 1) { data: {...} }  Jikan
      // 2) { character: {...} }
      // 3) direct character object
      if (isPlainObject(parsed.data)) return parsed.data;
      if (isPlainObject(parsed.character)) return parsed.character;
      return parsed;
    }

    if (isPlainObject(value)) {
      if (isPlainObject(value.data)) return value.data;
      if (isPlainObject(value.character)) return value.character;
      return value;
    }

    return {};
  };

  const source = resolveSourceObject(input);
  const existingObj = isPlainObject(existing) ? existing : {};

  // Support both Jikan full payload and already-normalized objects.
  const c = source || {};
  const aboutFromSource = normalizeString(c.about);
  const parsedAbout = parseAbout(aboutFromSource);

  const extracted = {
    name: normalizeString(c.name) || null,
    mal_id: normalizeNumber(c.mal_id) ?? normalizeString(c.mal_id) ?? null,
    about: parsedAbout.cleaned_about || aboutFromSource || null,
    gender: normalizeString(c.gender) || parsedAbout.gender || null,
    birthday: normalizeString(c.birthday) || parsedAbout.birth_date || null,
    height_cm:
      normalizeNumber(c.height_cm) ??
      parsedAbout.height_cm ??
      normalizeNumber(c.height) ??
      null,
    weight_kg:
      normalizeNumber(c.weight_kg) ??
      parsedAbout.weight_kg ??
      normalizeNumber(c.weight) ??
      null,
    blood_type: normalizeString(c.blood_type) || parsedAbout.blood_type || null,
    hair_color: normalizeString(c.hair_color) || null,
    eye_color: normalizeString(c.eye_color) || null,
    occupation:
      normalizeArray(c.occupation) ||
      normalizeString(c.occupation) ||
      null,
    affiliations:
      mergeArrays(parsedAbout.affiliations, c.affiliations) ||
      normalizeArray(c.affiliations) ||
      null,
    abilities:
      mergeArrays(parsedAbout.abilities, c.abilities) ||
      normalizeArray(c.abilities) ||
      null,
    image:
      normalizeString(c.images?.jpg?.image_url) ||
      normalizeString(c.images?.webp?.image_url) ||
      normalizeString(c.image_url) ||
      null,
  };
const merged = {};
const put = (key, value) => {
  if (value === undefined || value === null) return;
  if (typeof value === "string" && !value.trim()) return;
  if (Array.isArray(value) && value.length === 0) return;
  merged[key] = value;
};
put("name", chooseValue(existingObj.name, extracted.name));
put("mal_id", chooseValue(existingObj.mal_id, extracted.mal_id));
put("about", chooseValue(existingObj.about, extracted.about));
put("gender", chooseValue(existingObj.gender, extracted.gender));
put("birthday", chooseValue(existingObj.birthday, extracted.birthday));
put("height_cm", chooseValue(existingObj.height_cm, extracted.height_cm));
put("weight_kg", chooseValue(existingObj.weight_kg, extracted.weight_kg));
put("blood_type", chooseValue(existingObj.blood_type, extracted.blood_type));
put("hair_color", chooseValue(existingObj.hair_color, extracted.hair_color));
put("eye_color", chooseValue(existingObj.eye_color, extracted.eye_color));
put("occupation", chooseValue(existingObj.occupation, extracted.occupation));
put("affiliations", mergeArrays(existingObj.affiliations, extracted.affiliations));
put("abilities", mergeArrays(existingObj.abilities, extracted.abilities));
put("image", chooseValue(existingObj.image, extracted.image));
  for (const [key, value] of Object.entries(existingObj)) {
  if (key in merged) continue;
  if (value === undefined || value === null) continue;
  if (typeof value === "string" && !value.trim()) continue;
  if (Array.isArray(value) && value.length === 0) continue;
  merged[key] = deepClone(value);
}
  // Attach raw source only if explicitly requested.
  if (opts.keepRaw) {
    merged._raw = deepClone(input);
    merged._extracted = deepClone(extracted);
  }

  // Final cleanup: remove only undefined, keep nulls for explicit schema stability.
  for (const key of Object.keys(merged)) {
    if (merged[key] === undefined) merged[key] = null;
  }

  return merged;
}
function parseAbout(text){
  const result = { height_cm: null, weight_kg: null };
  const attributes = {};
  const lines = String(text || "").split("\n");
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const kv = trimmed.match(/^([A-Za-z\s]+)\s*:\s*(.+)$/);
    if (kv) {
      let key = kv[1].toLowerCase().trim();
      let value = kv[2].trim();
      key = key.replace(/\s+/g, "").replace(/[^a-z0-9]/g, "");
      if (key === "height") {
        let m = value.match(/(\d+)\s*cm/i);
        if (m) result.height_cm = parseInt(m[1]);
        else {
          let ftm = value.match(/(\d+)'\s*(\d+)/);
          if (ftm) {
            const ft = parseInt(ftm[1]), inch = parseInt(ftm[2]);
            result.height_cm = Math.round(ft * 30.48 + inch * 2.54);
          }
        }
      }
      if (key === "weight") {
        let m = value.match(/(\d+)\s*kg/i);
        if (m) result.weight_kg = parseInt(m[1]);
        else {
          let lbs = value.match(/(\d+)\s*lb/i);
          if (lbs) result.weight_kg = Math.round(parseInt(lbs[1]) * 0.453592);
        }
      }
      attributes[key] = value;
    }
  }
  return { ...attributes, ...result, cleaned_about: String(text || "").trim() };
}
async function commitExport(env, opts = {}) {
  const MAX_ATTEMPTS = opts.maxAttempts ?? 4;
  const BATCH_SIZE = Number(opts.batchSize ?? env.EXPORT_BATCH_SIZE ?? 20);
  const BRANCH = env.GITHUB_BRANCH || "main";

  if (!Array.isArray(env.__exportFiles) || env.__exportFiles.length === 0) return;
  if (!env.GITHUB_REPO) throw new Error("GITHUB_REPO env var missing");
  if (!env.GITHUB_TOKEN) throw new Error("GITHUB_TOKEN env var missing");

  const repo = env.GITHUB_REPO.includes("/")
    ? env.GITHUB_REPO
    : `${env.GITHUB_OWNER}/${env.GITHUB_REPO}`;

  const headersBase = {
    Authorization: `Bearer ${env.GITHUB_TOKEN}`,
    Accept: "application/vnd.github+json",
    "User-Agent": "Integrated-worker",
    "Content-Type": "application/json",
    "X-GitHub-Api-Version": "2026-03-10",
  };

  const hasGuard = typeof guard === "function";
  const hasGuardSub = typeof guardSub === "function";
  const hasCountedFetch = typeof countedFetch === "function";
  const hasSleep = typeof sleep === "function";

  const pause = (ms) => (hasSleep ? sleep(ms) : new Promise((r) => setTimeout(r, ms)));

  const safeGuard = () => {
    if (hasGuard) guard();
  };

  const safeGuardSub = () => {
    if (hasGuardSub) guardSub();
  };

  const fetchFn = async (url, init) => {
    if (hasCountedFetch) return countedFetch(url, init);
    return fetch(url, init);
  };

  const apiUrl = (path) => `https://api.github.com/repos/${repo}${path}`;

  const githubPathToApiPath = (path) =>
    String(path).split("/").map((seg) => encodeURIComponent(seg)).join("/");

  const normalizeFilePath = (path) => String(path || "").replace(/^\/+/, "");

  const parseMaybeJson = (value) => {
    if (value == null) return null;
    if (typeof value === "object") return value;
    const text = String(value).trim();
    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch {
      return null;
    }
  };

  const normalizeCf = (value) => {
    if (Array.isArray(value)) {
      return [...new Set(value.map((v) => Number(v)).filter(Number.isFinite))];
    }
    if (typeof value === "string") {
      const parsed = parseMaybeJson(value);
      if (Array.isArray(parsed)) {
        return [...new Set(parsed.map((v) => Number(v)).filter(Number.isFinite))];
      }
      return [];
    }
    if (value == null) return [];
    const n = Number(value);
    return Number.isFinite(n) ? [n] : [];
  };

  const normalizeNdjsonRecord = (obj) => {
    if (!obj || typeof obj !== "object") return null;
    const ai = obj.ai ?? obj.id ?? obj.animeId ?? obj.slug;
    if (ai == null || String(ai).trim() === "") return null;

    const out = { ...obj, ai: String(ai) };
    out.cf = normalizeCf(out.cf);
    return out;
  };

  const parseNdjsonText = (text) => {
    const lines = String(text || "")
      .split(/\r?\n/)
      .map((l) => l.trim())
      .filter(Boolean);

    const records = [];
    for (const line of lines) {
      try {
        const obj = JSON.parse(line);
        const rec = normalizeNdjsonRecord(obj);
        if (rec) records.push(rec);
      } catch {
        // ignore malformed lines
      }
    }
    return records;
  };

  const serializeNdjson = (records) =>
    records.map((r) => JSON.stringify(r)).join("\n") + (records.length ? "\n" : "");

  const mergeExistingAndIncoming = (existingRecords, incomingRecords) => {
    const order = [];
    const map = new Map();

    for (const rec of existingRecords) {
      const key = String(rec.ai);
      if (!map.has(key)) order.push(key);
      map.set(key, normalizeNdjsonRecord(rec));
    }

    for (const incoming of incomingRecords) {
      const rec = normalizeNdjsonRecord(incoming);
      if (!rec) continue;

      const key = String(rec.ai);
      if (!map.has(key)) {
        order.push(key);
        map.set(key, rec);
        continue;
      }

      const oldRec = normalizeNdjsonRecord(map.get(key)) || { ai: key, cf: [] };
      const mergedCf = [...new Set([...(oldRec.cf || []), ...(rec.cf || [])])];

      map.set(key, {
        ...oldRec,
        ...rec,
        ai: key,
        cf: mergedCf,
      });
    }

    return order.map((k) => map.get(k)).filter(Boolean);
  };

  const resolveIndexPathFromAnimeId = (animeId) => {
    const id = String(animeId || "").trim();
    if (!id) throw new Error("Cannot resolve NDJSON path without anime id");
    const first = id[0].toLowerCase();
    const bucket = /^[a-z]$/.test(first) ? first : "_";
    return `index/${bucket}.ndjson`;
  };

  const isLikelyCharacterTask = (file) => {
    const p = String(file?.path || "");
    return (
      file?.kind === "character" ||
      file?.type === "character" ||
      p.startsWith("characters/") ||
      p.includes("/characters/")
    );
  };

  const isLikelyAnimeTask = (file) => {
    const p = String(file?.path || "");
    return (
      file?.kind === "anime" ||
      file?.type === "anime" ||
      p.endsWith(".ndjson") ||
      !!file?.animeId ||
      !!file?.id
    );
  };

  const extractInputRecord = (file) => {
    const raw = file?.content ?? file?.raw ?? file?.data ?? file?.json ?? file?.value;
    const parsed = parseMaybeJson(raw);
    return parsed ?? raw;
  };

  const getRawGithubText = async (path) => {
    const url = `${apiUrl(`/contents/${githubPathToApiPath(path)}`)}?ts=${Date.now()}`;
    const res = await fetchFn(url, {
      headers: {
        Authorization: `Bearer ${env.GITHUB_TOKEN}`,
        Accept: "application/vnd.github.raw+json",
        "User-Agent": "Integrated-worker",
        "X-GitHub-Api-Version": "2026-03-10",
      },
    });

    if (res.status === 404) return "";
    if (!res.ok) {
      const txt = await res.text().catch(() => "<no-body>");
      throw new Error(`GET raw failed for ${path} (status ${res.status}): ${txt}`);
    }
    return await res.text();
  };

  const ghJson = async (method, path, body) => {
    const res = await fetchFn(apiUrl(path), {
      method,
      headers: headersBase,
      body: body == null ? undefined : JSON.stringify(body),
    });

    const text = await res.text().catch(() => "");
    if (!res.ok) {
      const status = res.status || "no-status";
      throw new Error(`${method} ${path} failed (status ${status}): ${text.slice(0, 2000)}`);
    }

    return text ? JSON.parse(text) : {};
  };

  const getHeadCommitAndTree = async () => {
    const ref = await ghJson("GET", `/git/ref/heads/${encodeURIComponent(BRANCH)}`);
    const headSha = ref?.object?.sha;
    if (!headSha) throw new Error(`Could not resolve head commit for branch ${BRANCH}`);

    const commit = await ghJson("GET", `/git/commits/${headSha}`);
    const treeSha = commit?.tree?.sha;
    if (!treeSha) throw new Error(`Could not resolve tree SHA for branch ${BRANCH}`);

    return { headSha, treeSha };
  };

  const createBlobUtf8 = async (contentText) => {
    const blob = await ghJson("POST", `/git/blobs`, {
      content: String(contentText ?? ""),
      encoding: "utf-8",
    });
    if (!blob?.sha) throw new Error("Blob creation returned no sha");
    return blob.sha;
  };

  const createTree = async (baseTreeSha, entries) => {
    const tree = await ghJson("POST", `/git/trees`, {
      base_tree: baseTreeSha,
      tree: entries,
    });
    if (!tree?.sha) throw new Error("Tree creation returned no sha");
    return tree.sha;
  };

  const createCommit = async (message, treeSha, parentSha) => {
    const commit = await ghJson("POST", `/git/commits`, {
      message,
      tree: treeSha,
      parents: [parentSha],
    });
    if (!commit?.sha) throw new Error("Commit creation returned no sha");
    return commit.sha;
  };

  const updateRef = async (newSha) => {
    await ghJson("PATCH", `/git/refs/heads/${encodeURIComponent(BRANCH)}`, {
      sha: newSha,
      force: false,
    });
  };

  const commitFilesAtomic = async (writesByPath, message) => {
    const paths = [...writesByPath.keys()];
    if (paths.length === 0) return;

    let lastErr;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        safeGuard();

        const { headSha, treeSha: baseTreeSha } = await getHeadCommitAndTree();

        const treeEntries = [];
        for (const path of paths) {
          const contentText = writesByPath.get(path);
          const blobSha = await createBlobUtf8(contentText);
          treeEntries.push({
            path,
            mode: "100644",
            type: "blob",
            sha: blobSha,
          });
        }

        const newTreeSha = await createTree(baseTreeSha, treeEntries);
        const newCommitSha = await createCommit(message, newTreeSha, headSha);

        safeGuardSub();
        safeGuard();
        await updateRef(newCommitSha);
        return;
      } catch (err) {
        lastErr = err;
        const msg = String(err?.message || err);
        console.warn(`commit attempt ${attempt} failed:`, msg);

        const retryable =
          /409|422|conflict|sha|reference|ref/i.test(msg);

        if (attempt === MAX_ATTEMPTS || !retryable) throw err;
        await pause(500 * attempt);
      }
    }

    throw lastErr;
  };

  const allFiles = env.__exportFiles.filter(Boolean);
  const characterTasks = [];
  const animeTasks = [];
  const leftoverTasks = [];

  for (const file of allFiles) {
    if (isLikelyCharacterTask(file)) {
      characterTasks.push(file);
    } else if (isLikelyAnimeTask(file)) {
      animeTasks.push(file);
    } else {
      leftoverTasks.push(file);
    }
  }

  const animeBatch = animeTasks.slice(0, BATCH_SIZE);
  const remainingAnime = animeTasks.slice(BATCH_SIZE);

  const groupedByIndexPath = new Map();
  for (const file of animeBatch) {
    const animeId =
      file?.animeId ??
      file?.id ??
      file?.ai ??
      file?.meta?.animeId ??
      file?.meta?.id;

    const targetPath = normalizeFilePath(
      file?.indexPath ||
        file?.ndjsonPath ||
        file?.targetPath ||
        file?.path ||
        resolveIndexPathFromAnimeId(animeId)
    );

    if (!groupedByIndexPath.has(targetPath)) {
      groupedByIndexPath.set(targetPath, []);
    }

    groupedByIndexPath.get(targetPath).push({
      ...file,
      animeId,
      targetPath,
      input: extractInputRecord(file),
    });
  }

  const pendingWrites = new Map();

  for (const [ndjsonPath, items] of groupedByIndexPath.entries()) {
    const batchForGenerator = items.map((x) => ({
      id: x.animeId,
      animeId: x.animeId,
      input: x.input,
      raw: x.input,
      source: x,
    }));

    if (typeof generateNdjson !== "function") {
      throw new Error("generateNdjson is not defined");
    }

    const result = await generateNdjson(batchForGenerator);

    let generatedRecords = [];
    if (Array.isArray(result)) {
      for (const item of result) {
        const rec = normalizeNdjsonRecord(parseMaybeJson(item) ?? item);
        if (rec) generatedRecords.push(rec);
      }
    } else if (typeof result === "string") {
      generatedRecords = parseNdjsonText(result);
    } else if (result && typeof result === "object") {
      const rec = normalizeNdjsonRecord(result);
      if (rec) generatedRecords = [rec];
    }

    const existingText = await getRawGithubText(ndjsonPath);
    const existingRecords = parseNdjsonText(existingText);
    const mergedRecords = mergeExistingAndIncoming(existingRecords, generatedRecords);

    pendingWrites.set(ndjsonPath, serializeNdjson(mergedRecords));
    await pause(100);
  }

  for (const file of characterTasks) {
    if (typeof extractInfornation === "function") {
      const extracted = await extractInformation(
  file?.content ?? file?.raw ?? file?.data ?? file?.json ?? file?.value ?? file,
  file?.existing ?? {},
  { preferExisting: true, keepRaw: false }
);
      const rawText =
        typeof extracted === "string"
          ? extracted
          : JSON.stringify(extracted ?? {}, null, 2);

      const path = normalizeFilePath(
        file?.path ||
          `characters/${String(file?.characterId ?? file?.id ?? file?.name ?? crypto.randomUUID())}.json`
      );

      pendingWrites.set(path, rawText);
    } else {
      const raw = file?.content ?? file?.raw ?? file?.data ?? file?.json ?? file?.value;
      const text = typeof raw === "string" ? raw : JSON.stringify(raw ?? {}, null, 2);
      const path =
        normalizeFilePath(file?.path) ||
        `characters/${String(file?.characterId ?? file?.id ?? file?.name ?? crypto.randomUUID())}.json`;

      pendingWrites.set(path, text);
    }

    await pause(50);
  }

  if (pendingWrites.size > 0) {
    await commitFilesAtomic(
      pendingWrites,
      `Auto export dataset batch (${pendingWrites.size} files)`
    );
  }

  env.__exportFiles = [...remainingAnime, ...leftoverTasks];
}
async function processSingleCharacterPayload(body, env) {
  guard();
  if (!body || !body.animeId) {
    throw new Error("Missing animeId");
  }
  if (!env.PROGRESS_KV) {
    throw new Error("Missing KV namespace: PROGRESS_KV");
  }
  const animeId = body.animeId;
  const baseUrl = `https://api.jikan.moe/v4/anime/${animeId}/characters`;
  let allCharacters = [];
  let page = 1;
  let hasNextPage = true;
  async function fetchWithRetry(url, retries = 3) {
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        const res = await fetch(url);
        if (res.status === 429) {
          const delay = 500 * Math.pow(2, attempt);
          await new Promise(r => setTimeout(r, delay));
          continue;
        }
        if (!res.ok) {
          throw new Error(`Jikan API error: ${res.status}`);
        }
        return await res.json();
      } catch (err) {
        if (attempt === retries) throw err;
        const delay = 300 * attempt;
        await new Promise(r => setTimeout(r, delay));
      }
    }
  }
  try {
    while (hasNextPage) {
      const url = `${baseUrl}?page=${page}`;
      const json = await fetchWithRetry(url);
      const data = json?.data || [];
      const mapped = data.map((item) => ({
        mal_character_id: item.character?.mal_id,
        name: item.character?.name,
        image: item.character?.images?.jpg?.image_url,
        role: item.role,
        favorites: item.favorites || 0
      }));
      allCharacters.push(...mapped);
      hasNextPage = json?.pagination?.has_next_page || false;
      page++;
       if (page > 50) {
        console.warn("Pagination exceeded safe limit, stopping early");
        break;
      }
    }
    const payload = {
      animeId,
      count: allCharacters.length,
      characters: allCharacters
    };
    const jsonString = JSON.stringify(payload);
    const kvKey = `anime:${animeId}`;
    await env.PROGRESS_KV.put(kvKey, jsonString);
    console.log(`Stored ${allCharacters.length} characters for anime ${animeId}`);
    return {
      success: true,
      animeId,
      count: allCharacters.length,
      kvKey
    };
  } catch (err) {
    if (err?.message?.includes("SAFE_STOP")) throw err;
    console.error("Error processing anime characters:", err);
    return {
      success: false,
      animeId,
      error: err.message
    };
  }
}
async function uploadChunk(env, index, rows) {
  const content = JSON.stringify(rows);
  if (!env.__exportFiles) env.__exportFiles = [];
  env.__exportFiles.push({
    path: `anime/part_${index}.json`,
    content: toBase64Utf8(content)
  });
}
function toBase64Utf8(str) {
  const bytes = new TextEncoder().encode(str);
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}
