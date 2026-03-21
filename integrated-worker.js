import { createClient } from "@libsql/client/web";
let requestCounter = 0;
let subrequestCounter = 0; 
const ANIME_COUNT_KEY = "number_of_anime";
const CHARACTER_FETCH_LIMIT = 20;
const MAX_REQUESTS = 49;
const SUBREQUEST_LIMIT = 49;
const MAX_CHUNKS_PER_RUN = 8;
const GIT_BUFFER_COUNT_KEY = "git_buffer_count";
const GIT_BUFFER_FLUSH_THRESHOLD = 100;
function deepClone(value) {
  if (value === null || value === undefined) return value;
  if (typeof structuredClone === "function") {
    try {
      return structuredClone(value);
    } catch {}
  }
  return JSON.parse(JSON.stringify(value));
}
function toPositiveInt(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const i = Math.trunc(n);
  return i > 0 ? i : null;
}
async function saveProgressId(env, memory = null) {
  const KV = env?.PROGRESS_KV;
  if (!KV || typeof KV.get !== "function" || typeof KV.put !== "function") {
    console.warn("saveProgressId: PROGRESS_KV is unavailable, skipping KV sync");
    return {
      ok: false,
      reason: "kv_unavailable",
    };
  }
  const source =
    memory ??
    env.__progressMemory ??
    env.__importCharactersPayload ??
    env.__saveProgressMemory ??
    {};
  const normalizeAnimeId = (value) => {
  if (value === null || value === undefined) return null;
  const s = String(value).trim();
  return s ? s : null;
};
  const uniqueIntArray = (input) => {
    const out = [];
    const seen = new Set();
    if (!Array.isArray(input)) return out;
    for (const item of input) {
      const n = toPositiveInt(item);
      if (n !== null && !seen.has(n)) {
        seen.add(n);
        out.push(n);
      }
    }
    return out;
  };
  const parseArrayValue = (raw) => {
    if (raw == null) return [];
    if (Array.isArray(raw)) {
      return uniqueIntArray(raw);
    }
    if (typeof raw === "object") {
      if (Array.isArray(raw.processed_characters)) return uniqueIntArray(raw.processed_characters);
      if (Array.isArray(raw.total_characters)) return uniqueIntArray(raw.total_characters);
      if (Array.isArray(raw.ids)) return uniqueIntArray(raw.ids);
      if (Array.isArray(raw.data)) return uniqueIntArray(raw.data);
    }
    if (typeof raw !== "string") return [];
    const text = raw.trim();
    if (!text) return [];
    try {
      const parsed = JSON.parse(text);
      if (Array.isArray(parsed)) return uniqueIntArray(parsed);
      if (parsed && typeof parsed === "object") {
        if (Array.isArray(parsed.processed_characters)) return uniqueIntArray(parsed.processed_characters);
        if (Array.isArray(parsed.total_characters)) return uniqueIntArray(parsed.total_characters);
        if (Array.isArray(parsed.ids)) return uniqueIntArray(parsed.ids);
        if (Array.isArray(parsed.data)) return uniqueIntArray(parsed.data);
      }
    } catch (_) {
    }
    return uniqueIntArray(
      text
        .split(/[\s,]+/g)
        .map((s) => s.trim())
        .filter(Boolean)
    );
  };
  const currentAnimeId = normalizeAnimeId(source.id);
if (currentAnimeId === null) {
  throw new Error("saveProgressId: invalid or missing source.id");
}
  const incomingTotalCharacters = uniqueIntArray(
    Array.isArray(source.total_characters) ? source.total_characters : []
  );
  const incomingProcessedCharacters = uniqueIntArray(
    Array.isArray(source.processed_characters)
      ? source.processed_characters
      : Array.isArray(source.mal_character_id)
        ? source.mal_character_id
        : []
  );
  const KV_KEYS = {
    animeId: "anime_id_progress",
    totalCharacters: "total_characters",
    processedCharacters: "processed_characters",
  };
  let existing = null;
  let readFailed = false;
  try {
    const [existingAnimeIdRaw, existingTotalRaw, existingProcessedRaw] = await Promise.all([
      KV.get(KV_KEYS.animeId),
      KV.get(KV_KEYS.totalCharacters),
      KV.get(KV_KEYS.processedCharacters),
    ]);
    existing = {
      animeId: normalizeAnimeId(existingAnimeIdRaw),
      totalCharacters: parseArrayValue(existingTotalRaw),
      processedCharacters: parseArrayValue(existingProcessedRaw),
    };
  } catch (err) {
    readFailed = true;
    console.warn("saveProgressId: KV read failed, will do upload/replace step:", err);
  }
  const shouldReplace =
    readFailed ||
    !existing ||
    existing.animeId !== currentAnimeId;
  try {
    if (shouldReplace) {
      // Full replace: anime id + total chars + processed chars
      await KV.put(KV_KEYS.animeId, String(currentAnimeId));
      await KV.put(KV_KEYS.totalCharacters, JSON.stringify(incomingTotalCharacters));
      await KV.put(KV_KEYS.processedCharacters, JSON.stringify(incomingProcessedCharacters));
      return {
        ok: true,
        mode: readFailed ? "upload" : "replace",
        anime_id_progress: currentAnimeId,
        total_characters: incomingTotalCharacters,
        processed_characters: incomingProcessedCharacters,
      };
    }
    // Same anime: keep anime_id_progress and total_characters unchanged.
    // Only append processed characters, dedupe, and write back.
    const mergedProcessed = uniqueIntArray([
      ...(existing.processedCharacters || []),
      ...incomingProcessedCharacters,
    ]);
    await KV.put(KV_KEYS.processedCharacters, JSON.stringify(mergedProcessed));
    return {
      ok: true,
      mode: "append",
      anime_id_progress: currentAnimeId,
      total_characters: existing.totalCharacters,
      processed_characters: mergedProcessed,
    };
  } catch (err) {
    console.error("saveProgressId: KV write failed:", err);
    return {
      ok: false,
      reason: "kv_write_failed",
      error: String(err?.message || err),
      anime_id_progress: currentAnimeId,
    };
  }
} 
async function clearKv(env, opts = {}) {
  const KEEP_KEY = "anime_id_progress";
  const kvCandidates = [
    env?.PROGRESS_KV,
    env?.progressKV,
    env?.KV,
    env?.kv,
  ];
  const KV = kvCandidates.find(
    (store) =>
      store &&
      typeof store.get === "function" &&
      typeof store.put === "function" &&
      typeof store.delete === "function" &&
      typeof store.list === "function"
  );
  if (!KV) {
    console.warn("clearKv: KV binding is unavailable");
    return {
      ok: false,
      reason: "kv_unavailable",
      kept_key: KEEP_KEY,
      deleted_count: 0,
    };
  }
  const fallbackIdSources = [
    () => env?.__progressMemory?.id,
    () => env?.__importCharactersPayload?.id,
    () => env?.__saveProgressMemory?.id,
  ];
  const readKeptValue = async () => {
    try {
      const raw = await KV.get(KEEP_KEY);
      if (raw !== null && raw !== undefined && raw !== "") {
        return String(raw);
      }
    } catch (err) {
      console.warn("clearKv: failed to read preserved key from KV:", err);
    }
    for (const pick of fallbackIdSources) {
      try {
        const value = pick();
        if (value !== null && value !== undefined && value !== "") {
          return String(value);
        }
      } catch (_) {}
    }
    return null;
  };
  const preservedValue = await readKeptValue();
  const listLimit = Math.min(
    Math.max(Number(opts.listLimit ?? 1000), 1),
    1000
  );
  const deleteBatchSize = Math.min(
    Math.max(Number(opts.deleteBatchSize ?? 50), 1),
    100
  );
  const maxPasses = Math.min(Math.max(Number(opts.maxPasses ?? 2), 1), 5);
  let deletedCount = 0;
  let scannedCount = 0;
  let lastRemaining = [];
  for (let pass = 1; pass <= maxPasses; pass++) {
    const keysToDelete = [];
    let cursor = undefined;
    try {
      do {
        const page = await KV.list({ limit: listLimit, cursor });
        const keys = Array.isArray(page?.keys) ? page.keys : [];
        for (const entry of keys) {
          const keyName = entry?.name;
          if (!keyName || keyName === KEEP_KEY) continue;
          keysToDelete.push(keyName);
        }
        cursor = page?.list_complete ? undefined : page?.cursor;
      } while (cursor);
    } catch (err) {
      console.error("clearKv: KV.list failed:", err);
      return {
        ok: false,
        reason: "kv_list_failed",
        error: String(err?.message || err),
        kept_key: KEEP_KEY,
        deleted_count: deletedCount,
      };
    }
    scannedCount += keysToDelete.length;
    lastRemaining = keysToDelete;
    if (keysToDelete.length === 0) break;
    for (let i = 0; i < keysToDelete.length; i += deleteBatchSize) {
      const batch = keysToDelete.slice(i, i + deleteBatchSize);
      const results = await Promise.allSettled(
        batch.map((key) => KV.delete(key))
      );
      for (const result of results) {
        if (result.status === "fulfilled") {
          deletedCount++;
        }
      }
    }
    if (preservedValue !== null) {
      try {
        await KV.put(KEEP_KEY, preservedValue);
      } catch (err) {
        console.error("clearKv: failed to restore anime_id_progress:", err);
        return {
          ok: false,
          reason: "kv_restore_failed",
          error: String(err?.message || err),
          kept_key: KEEP_KEY,
          deleted_count: deletedCount,
        };
      }
    }
  }
  return {
    ok: true,
    kept_key: KEEP_KEY,
    kept_value: preservedValue,
    deleted_count: deletedCount,
    scanned_count: scannedCount,
    remaining_sample: lastRemaining.slice(0, 10),
  };
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
async function indexPipeline(env, db, opts = {}) {
  requestCounter = 0;
  subrequestCounter = 0;
  const safeNumber = (value, fallback) => {
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
  };
  const safeInt = (value, fallback) => {
    const n = Number(value);
    return Number.isInteger(n) ? n : fallback;
  };
  const THRESHOLD = Number.isFinite(Number(opts.threshold))
    ? Math.max(1, Math.trunc(Number(opts.threshold)))
    : 100;
  const MAX_CHUNKS = Number.isFinite(Number(opts.maxChunks))
    ? Math.max(1, Math.trunc(Number(opts.maxChunks)))
    : MAX_CHUNKS_PER_RUN;
  const CHARACTER_LIMIT = Number.isFinite(Number(opts.characterLimit))
    ? Math.max(1, Math.trunc(Number(opts.characterLimit)))
    : CHARACTER_FETCH_LIMIT;
  const toInt = (value) => {
    const n = Number(value);
    return Number.isInteger(n) && n >= 0 ? n : null;
  };
  const readFirstInt = async (keys) => {
    const kv = env?.PROGRESS_KV;
    if (!kv || typeof kv.get !== "function") return null;
    const parseCount = (raw) => {
      const direct = toInt(raw);
      if (direct !== null) return direct;
      if (raw && typeof raw === "object") {
        return toInt(raw.number_of_anime ?? raw.count ?? raw.total);
      }
      if (typeof raw === "string") {
        try {
          const parsed = JSON.parse(raw);
          if (parsed && typeof parsed === "object") {
            return toInt(parsed.number_of_anime ?? parsed.count ?? parsed.total);
          }
        } catch {}
      }
      return null;
    };
    for (const key of keys) {
      try {
        const raw = await kv.get(key);
        const n = parseCount(raw);
        if (n !== null) return n;
      } catch (err) {
        console.warn(`indexPipeline: failed reading KV key "${key}"`, err);
      }
    }
    return null;
  };
  const number_of_anime = await readFirstInt([
    "number_of_anime",
    "anime_count",
    "buffer_anime_count",
    GIT_BUFFER_COUNT_KEY,
  ]);
  const count = number_of_anime ?? 0;
  const mode = count >= THRESHOLD ? "flush" : "process";
  console.log(
    `[indexPipeline] number_of_anime=${count}, threshold=${THRESHOLD}, mode=${mode}`
  );
  const baseResult = {
    ok: true,
    mode,
    threshold: THRESHOLD,
    number_of_anime: count,
    source: number_of_anime === null ? "default_zero" : "kv",
  };
  if (!db || typeof db.execute !== "function") {
    return {
      ...baseResult,
      ok: false,
      action: "process",
      error: "db_unavailable",
    };
  }
let chunksDone = 0;
  const chunkResults = [];
  let payload = null;
  let fetched = null;
  let uploadResult = null;
  let saveResult = null;
  try {
    payload = await importCharacters(env, db, {
      limit: CHARACTER_LIMIT,
    });
    if (
      !payload ||
      payload.skipped === true ||
      !Array.isArray(payload.mal_character_id) ||
      payload.mal_character_id.length === 0
    ) {
      console.log("indexPipeline: empty/skipped anime payload");
      return {
        ...baseResult,
        action: "process",
        done: true,
        chunksDone: 0,
        chunkResults,
      };
    }
  } catch (err) {
    console.error("indexPipeline: importCharacters failed:", err);
    return {
      ...baseResult,
      ok: false,
      action: "process",
      step: "importCharacters",
      chunksDone: 0,
      error: String(err?.message || err),
    };
  }
  try {
    fetched = await fetchSingleCharacter(
      {
        animeId: payload.id,
        characterMalIds: payload.mal_character_id,
        limit: CHARACTER_LIMIT,
      },
      env
    );
  } catch (err) {
    if (err?.message === "SAFE_STOP") {
      console.warn("indexPipeline: SAFE_STOP during fetchSingleCharacter");
      return {
        ...baseResult,
        action: "process",
        stopped: true,
        reason: "SAFE_STOP",
        chunksDone: 0,
        chunkResults,
        animeId: payload?.id ?? null,
      };
    }
    console.error("indexPipeline: fetchSingleCharacter failed:", err);
    return {
      ...baseResult,
      ok: false,
      action: "process",
      step: "fetchSingleCharacter",
      chunksDone: 0,
      animeId: payload?.id ?? null,
      error: String(err?.message || err),
    };
  }
  try {
    uploadResult = await uploadBuffer(fetched, env);
  } catch (err) {
    if (err?.message === "SAFE_STOP") {
      console.warn("indexPipeline: SAFE_STOP during uploadBuffer");
      return {
        ...baseResult,
        action: "process",
        stopped: true,
        reason: "SAFE_STOP",
        chunksDone: 0,
        chunkResults,
        animeId: payload?.id ?? null,
      };
    }
    console.error("indexPipeline: uploadBuffer failed:", err);
    return {
      ...baseResult,
      ok: false,
      action: "process",
      step: "uploadBuffer",
      chunksDone: 0,
      animeId: payload?.id ?? null,
      error: String(err?.message || err),
    };
  }
  const processedCharacters = Array.isArray(fetched?.characters_buffer)
    ? fetched.characters_buffer
        .map((item) => item?.mal_character_id)
        .filter((v) => Number.isInteger(v) && v > 0)
    : [];
  try {
    saveResult = await saveProgressId(env, {
      id: payload.id,
      total_characters: payload.total_characters,
      processed_characters: processedCharacters,
    });
  } catch (err) {
    console.error("indexPipeline: saveProgressId threw:", err);
    saveResult = {
      ok: false,
      reason: "exception",
      error: String(err?.message || err),
    };
  }
  chunkResults.push({
    animeId: payload.id,
    malId: payload.mal_id ?? null,
    fetchedCount: Array.isArray(fetched?.characters_buffer)
      ? fetched.characters_buffer.length
      : 0,
    processedCharacters: processedCharacters.length,
    uploadResult,
    saveResult,
  });
  console.log("indexPipeline chunk result:", {
    animeId: payload.id,
    uploadResult,
    saveResult,
    processedCharacters: processedCharacters.length,
  });
  return {
    ...baseResult,
    action: "process",
    chunksDone: 1,
    chunkResults,
  };
}
async function runScheduled(env, db, opts = {}) {
  requestCounter = 0;
  subrequestCounter = 0;
  const progressState = await readProgressState(env);
  const readCount = async () => {
    const kv = env?.PROGRESS_KV;
    if (!kv || typeof kv.get !== "function") return 0;
    const raw = await kv.get(GIT_BUFFER_COUNT_KEY).catch(() => null);
    const n = Number(raw);
    return Number.isInteger(n) && n >= 0 ? n : 0;
  };
  const count = Number.isFinite(Number(opts.count))
    ? Math.max(0, Math.trunc(Number(opts.count)))
    : await readCount();
  const mode =
    String(opts.mode || "").toLowerCase() === "flush" || count >= GIT_BUFFER_FLUSH_THRESHOLD
      ? "flush"
      : "process";
  console.log(`[runScheduled] mode=${mode}, count=${count}`);
  if (mode === "flush") {
    return await commitExport(env, {
      ...opts,
      progressState,
    });
  }
  return await indexPipeline(env, db, {
    ...opts,
    progressState,
    mode: "process",
  });
}
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
async function readProgressState(env) {
  const kv = env?.PROGRESS_KV;
  if (!kv || typeof kv.get !== "function") {
    return {
      animeId: null,
      totalCharacters: [],
      processedCharacters: [],
    };
  }
  const normalizeAnimeId = (value) => {
  if (value === null || value === undefined) return null;
  const s = String(value).trim();
  return s ? s : null;
};
  const parseArrayValue = (raw) => {
    if (raw == null) return [];
    if (Array.isArray(raw)) {
      return raw
        .map((v) => toPositiveInt(v))
        .filter((v) => v !== null);
    }
    if (typeof raw === "string") {
      const text = raw.trim();
      if (!text) return [];
      try {
        const parsed = JSON.parse(text);
        if (Array.isArray(parsed)) {
          return parsed.map((v) => toPositiveInt(v)).filter((v) => v !== null);
        }
      } catch (_) {}
      return text
        .split(/[\s,]+/g)
        .map((v) => toPositiveInt(v))
        .filter((v) => v !== null);
    }
    if (typeof raw === "object") {
      if (Array.isArray(raw.total_characters)) return parseArrayValue(raw.total_characters);
      if (Array.isArray(raw.processed_characters)) return parseArrayValue(raw.processed_characters);
      if (Array.isArray(raw.ids)) return parseArrayValue(raw.ids);
      if (Array.isArray(raw.data)) return parseArrayValue(raw.data);
    }
    return [];
  };
  const [animeIdRaw, totalRaw, processedRaw] = await Promise.all([
    kv.get("anime_id_progress"),
    kv.get("total_characters"),
    kv.get("processed_characters"),
  ]);
  return {
    animeId: normalizeAnimeId(animeIdRaw),
    totalCharacters: parseArrayValue(totalRaw),
    processedCharacters: parseArrayValue(processedRaw),
  };
}
async function importCharacters(env, db, opts = {}) {
  const LIMIT = Number.isFinite(Number(opts.limit)) ? Number(opts.limit) : 20;
  const MAX_RETRIES = Number.isFinite(Number(opts.maxRetries)) ? Number(opts.maxRetries) : 3;
  const RETRY_DELAY_MS = Number.isFinite(Number(opts.retryDelayMs)) ? Number(opts.retryDelayMs) : 1500;
  const MAX_SCAN_ATTEMPTS = Number.isFinite(Number(opts.maxScanAttempts))
    ? Math.max(1, Math.trunc(Number(opts.maxScanAttempts)))
    : Math.max(LIMIT * 20, 100);
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const toIntId = (value) => {
    const n = Number(value);
    return Number.isInteger(n) && n > 0 ? n : null;
  };
  const normalizeAnimeId = (value) => {
    if (value === null || value === undefined) return null;
    const s = String(value).trim();
    return s ? s : null;
  };
  const uniqueInts = (values) => {
    const out = [];
    const seen = new Set();
    for (const value of values || []) {
      const n = toIntId(value);
      if (n !== null && !seen.has(n)) {
        seen.add(n);
        out.push(n);
      }
    }
    return out;
  };
  const parseProcessedCharacters = (raw) => {
    if (raw == null) return [];
    if (Array.isArray(raw)) {
      return uniqueInts(raw);
    }
    if (typeof raw === "object") {
      if (Array.isArray(raw.processed_characters)) return uniqueInts(raw.processed_characters);
      if (Array.isArray(raw.characters)) return uniqueInts(raw.characters);
      if (Array.isArray(raw.ids)) return uniqueInts(raw.ids);
    }
    if (typeof raw !== "string") return [];
    const text = raw.trim();
    if (!text) return [];
    try {
      const parsed = JSON.parse(text);
      if (Array.isArray(parsed)) return uniqueInts(parsed);
      if (parsed && typeof parsed === "object") {
        if (Array.isArray(parsed.processed_characters)) return uniqueInts(parsed.processed_characters);
        if (Array.isArray(parsed.characters)) return uniqueInts(parsed.characters);
        if (Array.isArray(parsed.ids)) return uniqueInts(parsed.ids);
      }
    } catch (_) {}
    return uniqueInts(
      text
        .split(/[\s,]+/g)
        .map((s) => s.trim())
        .filter(Boolean)
    );
  };
  const readProcessedCharacterSet = async (animeId) => {
    const kv = env?.PROGRESS_KV;
    if (!kv || typeof kv.get !== "function") return new Set();
    const keysToTry = [
      `processed_characters:${animeId}`,
      `processed_characters_${animeId}`,
      "processed_characters",
    ];
    for (const key of keysToTry) {
      try {
        const raw = await kv.get(key);
        if (raw == null) continue;
        const ids = parseProcessedCharacters(raw);
        return new Set(ids);
      } catch (err) {
        console.error(`Failed reading KV key "${key}":`, err);
      }
    }
    return new Set();
  };
  const markSkippedAnime = async (animeId) => {
    const kv = env?.PROGRESS_KV;
    if (!kv || typeof kv.put !== "function") return;
    try {
      await Promise.all([
        kv.put("anime_id_progress", String(animeId)),
        kv.put("total_characters", "[]"),
        kv.put("processed_characters", "[]"),
      ]);
    } catch (err) {
      console.warn("markSkippedAnime failed:", err);
    }
  };
  const fetchJikanJson = async (url) => {
    let lastError = null;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        const res = await countedFetch(url, {
          headers: {
            Accept: "application/json",
          },
        });
        if (res.status === 429) {
          lastError = new Error(`Jikan rate limited (429)`);
          if (attempt < MAX_RETRIES) {
            await sleep(RETRY_DELAY_MS * attempt);
            continue;
          }
          throw lastError;
        }
        if (res.status === 404) {
          return { notFound: true, data: null };
        }
        if (!res.ok) {
          lastError = new Error(`Jikan returned status ${res.status}`);
          if (attempt < MAX_RETRIES) {
            await sleep(RETRY_DELAY_MS * attempt);
            continue;
          }
          throw lastError;
        }
        let json;
        try {
          json = await res.json();
        } catch (err) {
          lastError = new Error("Invalid JSON from Jikan");
          if (attempt < MAX_RETRIES) {
            await sleep(RETRY_DELAY_MS * attempt);
            continue;
          }
          throw err;
        }
        return { notFound: false, data: json };
      } catch (err) {
        lastError = err;
        if (attempt < MAX_RETRIES) {
          await sleep(RETRY_DELAY_MS * attempt);
          continue;
        }
      }
    }
    throw lastError || new Error("Jikan fetch failed");
  };
  const extractCharacterIds = (jikanJson) => {
    const data = Array.isArray(jikanJson?.data) ? jikanJson.data : [];
    const ids = [];
    for (const item of data) {
      const id = toIntId(item?.character?.mal_id);
      if (id !== null) ids.push(id);
    }
    return uniqueInts(ids);
  };
  const progressState = opts.progressState ?? await readProgressState(env);
  let cursorId = toIntId(progressState.animeId) ?? 0;
  let tryCurrentProgressFirst =
    progressState.animeId !== null &&
    progressState.totalCharacters.length > 0 &&
    progressState.processedCharacters.length < progressState.totalCharacters.length;
  let scanAttempts = 0;
  while (scanAttempts < MAX_SCAN_ATTEMPTS) {
    scanAttempts++;
    let animeRes = null;
    if (tryCurrentProgressFirst && progressState.animeId !== null) {
      animeRes = await db.execute({
        sql: `
          SELECT id, mal_id, title
          FROM anime_info
          WHERE id = ? AND mal_id IS NOT NULL
          LIMIT 1
        `,
        args: [progressState.animeId],
      });
    }
    if (!animeRes?.rows?.length) {
      animeRes = await db.execute({
        sql: `
          SELECT id, mal_id, title
          FROM anime_info
          WHERE id > ? AND mal_id IS NOT NULL
          ORDER BY id ASC
          LIMIT 1
        `,
        args: [cursorId],
      });
      tryCurrentProgressFirst = false;
    }
    if (!animeRes?.rows?.length) {
      console.log("No anime row found after cursor:", cursorId);
      return null;
    }
    const anime = animeRes.rows[0];
    const id = normalizeAnimeId(anime?.id);
    const rowCursor = toIntId(anime?.id);
    const mal_id = toIntId(anime?.mal_id);
    const title = typeof anime?.title === "string" ? anime.title : "";
    if (rowCursor !== null) {
      cursorId = rowCursor;
    } else {
      cursorId += 1;
    }
    if (id === null) {
      console.log("Invalid anime id, skipping row:", anime);
      tryCurrentProgressFirst = false;
      continue;
    }
    if (mal_id === null) {
      console.log("Invalid mal_id in SQL row, skipping row:", anime);
      await markSkippedAnime(id);
      tryCurrentProgressFirst = false;
      continue;
    }
    const url = `https://api.jikan.moe/v4/anime/${mal_id}/characters`;
    let notFound = false;
    let jikanJson = null;
    try {
      ({ notFound, data: jikanJson } = await fetchJikanJson(url));
    } catch (err) {
      if (err?.message === "SAFE_STOP") throw err;
      console.warn(`Jikan fetch failed for anime id=${id}, mal_id=${mal_id}. Skipping anime.`, err);
      await markSkippedAnime(id);
      tryCurrentProgressFirst = false;
      continue;
    }
    if (notFound) {
      console.log(`Jikan 404 for anime mal_id=${mal_id}`);
      await markSkippedAnime(id);
      tryCurrentProgressFirst = false;
      continue;
    }
    const total_characters = extractCharacterIds(jikanJson);
    if (total_characters.length === 0) {
      console.log(`No character IDs returned by Jikan for anime id=${id}, mal_id=${mal_id}`);
      await markSkippedAnime(id);
      tryCurrentProgressFirst = false;
      continue;
    }
    const processedSet = await readProcessedCharacterSet(id);
    const remainingCharacters = total_characters.filter((charId) => !processedSet.has(charId));
    if (remainingCharacters.length === 0) {
      console.log(`All characters already processed for anime id=${id}, mal_id=${mal_id}`);
      await markSkippedAnime(id);
      tryCurrentProgressFirst = false;
      continue;
    }
    const mal_character_id = remainingCharacters.slice(0, LIMIT);
    if (mal_character_id.length === 0) {
      await markSkippedAnime(id);
      tryCurrentProgressFirst = false;
      continue;
    }
    const payload = {
      id,
      mal_id,
      mal_character_id,
      total_characters,
      title,
    };
    env.__importCharactersPayload = payload;
    console.log(
      `importCharacters ready: anime id=${id}, mal_id=${mal_id}, total=${total_characters.length}, remaining=${remainingCharacters.length}, batch=${mal_character_id.length}`
    );
    return payload;
  }
  return null;
}
async function fetchSingleCharacter(body = {}, env) {
  guard();
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const safeJsonParse = (value) => {
    if (typeof value !== "string") return value;
    const trimmed = value.trim();
    if (!trimmed) return null;
    try {
      return JSON.parse(trimmed);
    } catch {
      return value;
    }
  };
  const toIdArray = (value) => {
    if (value === null || value === undefined) return [];
    const parsed = safeJsonParse(value);
    if (typeof parsed === "number" && Number.isFinite(parsed)) {
      return [Math.trunc(parsed)];
    }
    if (typeof parsed === "string") {
      return parsed
        .split(/[\s,]+/)
        .map((v) => Number(v.trim()))
        .filter((v) => Number.isInteger(v) && v > 0);
    }
    if (Array.isArray(parsed)) {
      const out = [];
      for (const item of parsed) {
        out.push(...toIdArray(item));
      }
      return out;
    }
    if (typeof parsed === "object") {
      if (Array.isArray(parsed.mal_character_ids)) return toIdArray(parsed.mal_character_ids);
      if (parsed.mal_character_id !== undefined) return toIdArray(parsed.mal_character_id);
      if (parsed.ids !== undefined) return toIdArray(parsed.ids);
      if (parsed.value !== undefined) return toIdArray(parsed.value);
    }
    return [];
  };
  const readMemoryValue = async (keys) => {
    const kv = env?.PROGRESS_KV || env?.progressKV || env?.KV || env?.kv || null;
    if (!kv || typeof kv.get !== "function") return null;
    for (const key of keys) {
      try {
        const value = await kv.get(key);
        if (value !== null && value !== undefined && value !== "") {
          return value;
        }
      } catch (err) {
        console.warn("readMemoryValue failed for key:", key, err);
      }
    }
    return null;
  };
  const fetchJsonWithRetry = async (url, fetchOptions = {}, maxRetries = 3) => {
    let lastError = null;
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        const res = await countedFetch(url, fetchOptions);
        if (!res.ok) {
          const text = await res.text().catch(() => "");
          const err = new Error(`HTTP ${res.status} for ${url}${text ? ` :: ${text.slice(0, 200)}` : ""}`);
          err.status = res.status;
          throw err;
        }
        return await res.json();
      } catch (err) {
        lastError = err;
        if (attempt < maxRetries) {
          const backoff = 300 * Math.pow(2, attempt);
          await delay(backoff);
          continue;
        }
      }
    }
    throw lastError || new Error(`Failed to fetch ${url}`);
  };
  const hasMeaningfulCharacterData = (data) => {
    if (!data || typeof data !== "object") return false;
    const meaningfulKeys = [
      "name",
      "url",
      "images",
      "about",
      "nicknames",
      "favorites",
      "family_name",
      "given_name",
      "name_kanji",
      "anime",
      "voice_actors",
    ];
    return meaningfulKeys.some((key) => {
      const v = data[key];
      if (Array.isArray(v)) return v.length > 0;
      if (v && typeof v === "object") return Object.keys(v).length > 0;
      return v !== null && v !== undefined && v !== "";
    });
  };
  const animeIdFromMemory = await readMemoryValue(["animeId", "anime_id", "id"]);
  const animeId = body?.animeId ?? animeIdFromMemory;
  if (!animeId) {
    throw new Error("No anime id available");
  }
  const idsFromMemory = await readMemoryValue([
    "mal_character_ids",
    "characterMalIds",
    "character_mal_ids",
  ]);
  let charIds = toIdArray(idsFromMemory);
  if (charIds.length === 0) {
    charIds = toIdArray(body?.characterMalIds ?? body?.characterMalId);
  }
  charIds = [...new Set(charIds)].filter((id) => Number.isInteger(id) && id > 0);
const characterLimit = Number.isFinite(Number(body?.limit))
    ? Math.max(1, Math.trunc(Number(body.limit)))
    : null;
  if (characterLimit !== null) {
    charIds = charIds.slice(0, characterLimit);
  }
  if (charIds.length === 0) {
    throw new Error("No character ids found");
  }
  const characters_buffer = [];
  let successCount = 0;
  for (const characterId of charIds) {
    try {
      const url = `https://api.jikan.moe/v4/characters/${characterId}/full`;
      const response = await fetchJsonWithRetry(url, { method: "GET" }, 3);
      const full = response?.data ?? null;
      if (!full || typeof full !== "object") {
        console.log("Skipping invalid character response:", characterId);
        continue;
      }
      if (!hasMeaningfulCharacterData(full)) {
        console.log("Skipping minimal/empty character:", characterId);
        continue;
      }
      let extracted = null;
      try {
        if (typeof extractInformation === "function") {
          extracted = await extractInformation(full, {}, { preferExisting: true });
        } else {
          extracted = full;
        }
      } catch (err) {
        console.warn("extractInformation failed for character:", characterId, err);
        extracted = full;
      }
      if (!extracted || typeof extracted !== "object" || Object.keys(extracted).length === 0) {
        console.log("Skipping character after extraction produced nothing:", characterId);
        continue;
      }
      const record = {
        mal_character_id: characterId,
        ...extracted,
      };
      characters_buffer.push(record);
      successCount++;
      await delay(250);
    } catch (e) {
      if (e?.message === "SAFE_STOP") {
        console.log("SAFE_STOP in fetchSingleCharacter");
        e.successCount = successCount;
        e.characters_buffer = characters_buffer;
        e.id = animeId;
        throw e;
      }
      console.error("fetchSingleCharacter character error:", characterId, e);
    }
  }
  return {
    id: animeId,
    characters_buffer,
  };
}
async function uploadBuffer(result = {}, env) {
  guard();
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const safeJsonParse = (value) => {
    if (typeof value !== "string") return value;
    const trimmed = value.trim();
    if (!trimmed) return null;
    try {
      return JSON.parse(trimmed);
    } catch {
      return value;
    }
  };
  const getKV = () => {
    return (
      env?.PROGRESS_KV ||
      env?.progressKV ||
      env?.KV ||
      env?.kv ||
      env?.PROGRESS_KV_CHARACTERS ||
      null
    );
  };
  const normalizeBuffer = (value) => {
    const parsed = safeJsonParse(value);
    if (parsed === null || parsed === undefined) return [];
    if (Array.isArray(parsed)) {
      return parsed.filter((item) => item !== null && item !== undefined);
    }
    if (typeof parsed === "object") {
      if (Array.isArray(parsed.characters_buffer)) {
        return parsed.characters_buffer.filter((item) => item !== null && item !== undefined);
      }
      return [parsed];
    }
    return [];
  };
  const normalizeId = (value) => {
    const parsed = safeJsonParse(value);
    if (typeof parsed === "number" && Number.isFinite(parsed)) {
      return String(Math.trunc(parsed));
    }
    if (typeof parsed === "string") {
      const trimmed = parsed.trim();
      return trimmed ? trimmed : null;
    }
    if (parsed && typeof parsed === "object") {
      if (parsed.id !== undefined && parsed.id !== null) return normalizeId(parsed.id);
      if (parsed.animeId !== undefined && parsed.animeId !== null) return normalizeId(parsed.animeId);
      if (parsed.anime_id !== undefined && parsed.anime_id !== null) return normalizeId(parsed.anime_id);
    }
    return null;
  }; 
  const normalizeAnimeCount = (value) => {
    const parsed = safeJsonParse(value);
    if (typeof parsed === "number" && Number.isFinite(parsed)) {
      return Math.trunc(parsed);
    }
    if (typeof parsed === "string") {
      const trimmed = parsed.trim();
      if (!trimmed) return null;
      const n = Number(trimmed);
      return Number.isFinite(n) ? Math.trunc(n) : null;
    }
    if (parsed && typeof parsed === "object") {
      if (parsed.number_of_anime !== undefined && parsed.number_of_anime !== null) {
        return normalizeAnimeCount(parsed.number_of_anime);
      }
      if (parsed.count !== undefined && parsed.count !== null) {
        return normalizeAnimeCount(parsed.count);
      }
      if (parsed.total !== undefined && parsed.total !== null) {
        return normalizeAnimeCount(parsed.total);
      }
    }
    return null;
  };
  const dedupeBuffer = (items) => {
    const out = [];
    const seen = new Set();
    for (const item of items) {
      if (!item || typeof item !== "object") {
        const key = `raw:${String(item)}`;
        if (seen.has(key)) continue;
        seen.add(key);
        out.push(item);
        continue;
      }
      const id =
        item.mal_character_id ??
        item.mal_id ??
        item.id ??
        null;
      const key =
        id !== null && id !== undefined
          ? `id:${String(id)}`
          : `obj:${JSON.stringify(item)}`;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(item);
    }
    return out;
  };
  const kv = getKV();
  if (!kv || typeof kv.get !== "function" || typeof kv.put !== "function") {
    throw new Error("KV binding is missing or invalid");
  }
  const id = normalizeId(result?.id);
  const incomingBuffer = normalizeBuffer(result?.characters_buffer);
  if (!id) {
    throw new Error("uploadBuffer: missing anime id");
  }
  if (!Array.isArray(incomingBuffer)) {
    throw new Error("uploadBuffer: invalid characters_buffer");
  }
  const key = id;
let existingRaw = null;
  let countRaw = null;
  let existingParsed = null;
  let existingBuffer = [];
  let slotFound = false;
  try {
    [existingRaw, countRaw] = await Promise.all([
      kv.get(key),
      kv.get(ANIME_COUNT_KEY),
    ]);
    slotFound =
      existingRaw !== null &&
      existingRaw !== undefined &&
      existingRaw !== "";
  } catch (err) {
    console.warn("uploadBuffer: KV get failed, continuing with fresh slot:", err);
    slotFound = false;
  }
  const existingAnimeCount = normalizeAnimeCount(countRaw);
  const nextAnimeCount = slotFound
    ? (existingAnimeCount ?? 1)
    : (existingAnimeCount ?? 0) + 1;
  if (slotFound) {
    existingParsed = safeJsonParse(existingRaw);
    if (Array.isArray(existingParsed)) {
      existingBuffer = existingParsed;
    } else if (
      existingParsed &&
      typeof existingParsed === "object" &&
      Array.isArray(existingParsed.characters_buffer)
    ) {
      existingBuffer = existingParsed.characters_buffer;
    } else if (existingParsed && typeof existingParsed === "object" && existingParsed.characters_buffer) {
      existingBuffer = normalizeBuffer(existingParsed.characters_buffer);
    } else {
      existingBuffer = [];
    }
  }
  const mergedBuffer = dedupeBuffer([...existingBuffer, ...incomingBuffer]);
  const payload = {
    id,
    number_of_anime: nextAnimeCount,
    characters_buffer: mergedBuffer,
  };
  const countPayload = String(nextAnimeCount);
  try {
    await kv.put(key, JSON.stringify(payload));
  } catch (err) {
    console.error("uploadBuffer: anime KV put failed on first attempt:", err);
    await delay(250);
    try {
      await kv.put(key, JSON.stringify(payload));
    } catch (retryErr) {
      console.error("uploadBuffer: anime KV put failed on retry:", retryErr);
      throw retryErr;
    }
  }
  try {
    await kv.put(ANIME_COUNT_KEY, countPayload);
  } catch (err) {
    console.error("uploadBuffer: count KV put failed on first attempt:", err);
    await delay(250);
    try {
      await kv.put(ANIME_COUNT_KEY, countPayload);
    } catch (retryErr) {
      console.error("uploadBuffer: count KV put failed on retry:", retryErr);
      throw retryErr;
    }
  }
  return {
    id,
    number_of_anime: nextAnimeCount,
    created: !slotFound,
    previous_count: existingBuffer.length,
    added_count: incomingBuffer.length,
    final_count: mergedBuffer.length,
    characters_buffer: mergedBuffer,
  };
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
  if (opts.keepRaw) {
    merged._raw = deepClone(input);
    merged._extracted = deepClone(extracted);
  }
  for (const key of Object.keys(merged)) {
    if (merged[key] === undefined) merged[key] = null;
  }
  return merged;
}
async function generateNdjson(batch = []) {
  const items = Array.isArray(batch) ? batch : [batch];
  const isPlainObject = (v) =>
    v !== null && typeof v === "object" && !Array.isArray(v);
  const parseMaybeJson = (value) => {
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
  const normalizeAi = (value) => {
    if (value == null) return null;
    const s = String(value).trim();
    return s ? s : null;
  };
  const normalizeCharId = (value) => {
    if (value == null || value === "") return null;
    if (typeof value === "number") {
      return Number.isFinite(value) ? Math.trunc(value) : null;
    }
    if (typeof value === "string") {
      const t = value.trim();
      if (!t) return null;
      const n = Number(t.replace(/[^\d.-]/g, ""));
      return Number.isFinite(n) ? Math.trunc(n) : null;
    }
    if (isPlainObject(value)) {
      const candidates = [
        value.mal_character_id,
        value.characterMalId,
        value.character_mal_id,
        value.character?.mal_character_id,
        value.character?.character_mal_id,
        value.character?.mal_id,
        value.character?.id,
      ];
      for (const c of candidates) {
        const n = normalizeCharId(c);
        if (n !== null) return n;
      }
    }
    return null;
  };
  const addCharId = (set, value) => {
    const n = normalizeCharId(value);
    if (n !== null) set.add(n);
  };
  const collectCharIds = (value, set) => {
    if (value == null) return;
    if (Array.isArray(value)) {
      for (const v of value) collectCharIds(v, set);
      return;
    }
    if (typeof value === "string") {
      const t = value.trim();
      if (!t) return;
      const parsed = parseMaybeJson(t);
      if (parsed !== null) {
        collectCharIds(parsed, set);
        return;
      }
      const parts = t.split(/[\s,;|/]+/g).filter(Boolean);
      for (const part of parts) addCharId(set, part);
      return;
    }
    if (typeof value === "number") {
      addCharId(set, value);
      return;
    }
    if (!isPlainObject(value)) return;
    const explicitFields = [
      value.cf,
      value.characterMalId,
      value.characterMalIds,
      value.mal_character_id,
      value.mal_character_ids,
      value.character?.mal_character_id,
      value.character?.mal_character_ids,
      value.character?.mal_id,
      value.characters,
      value.data,
      value.items,
      value.rows,
    ];
    for (const field of explicitFields) collectCharIds(field, set);
    for (const [key, val] of Object.entries(value)) {
      if (/(^cf$|character.*mal.*id|mal.*character.*id)/i.test(key)) {
        collectCharIds(val, set);
      }
    }
  };
  const extractAi = (value, fallback = null) => {
    const candidates = [
      value?.ai,
      value?.animeId,
      value?.id,
      value?.slug,
      fallback,
    ];
    for (const c of candidates) {
      const ai = normalizeAi(c);
      if (ai) return ai;
    }
    return null;
  };
  const records = new Map(); 
  const order = [];
  const ensureRecord = (ai) => {
    if (!records.has(ai)) {
      records.set(ai, new Set());
      order.push(ai);
    }
    return records.get(ai);
  };
  const ingest = (candidate, fallbackAi = null) => {
    if (candidate == null) return;
    if (Array.isArray(candidate)) {
      for (const item of candidate) ingest(item, fallbackAi);
      return;
    }
    const parsed = typeof candidate === "string" ? parseMaybeJson(candidate) : candidate;
    if (parsed == null) return;
    const ai = extractAi(parsed, fallbackAi);
    const cfSet = ai ? ensureRecord(ai) : null;
    const ids = new Set();
    collectCharIds(parsed, ids);
    if (cfSet) {
      for (const id of ids) cfSet.add(id);
    }
  };
  for (const item of items) {
    if (item == null) continue;
    const baseAi = extractAi(item, null);
    ingest(item, baseAi);
    ingest(item?.input, baseAi);
    ingest(item?.raw, baseAi);
    ingest(item?.source, baseAi);
    ingest(item?.source?.extracted?._raw, baseAi);
  }
  const lines = [];
  for (const ai of order) {
    const cf = [...records.get(ai)];
    if (!ai || cf.length === 0) continue;
    lines.push(JSON.stringify({ ai, cf }));
  }
  return lines.length ? `${lines.join("\n")}\n` : "";
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
const batchForGenerator = [];
    for (const x of items) {
      const extracted =
        typeof extractInformation === "function"
          ? await extractInformation(x.input, {}, { preferExisting: true, keepRaw: true })
          : { _raw: x.input };
      batchForGenerator.push({
        id: x.animeId,
        animeId: x.animeId,
        input: x.input,
        raw: x.raw ?? x.input,
        source: {
          ...x,
          extracted,
        },
      });
    }
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
  const raw = file?.content ?? file?.raw ?? file?.data ?? file?.json ?? file?.value;
  const text = typeof raw === "string" ? raw : JSON.stringify(raw ?? {}, null, 2);
  const path =
    normalizeFilePath(file?.path) ||
    `characters/${String(file?.characterId ?? file?.id ?? file?.name ?? crypto.randomUUID())}.json`;
  pendingWrites.set(path, text);
  await pause(50);
}
if (pendingWrites.size > 0) {
    await commitFilesAtomic(
      pendingWrites,
      `Auto export dataset batch (${pendingWrites.size} files)`
    );
  }
  const processedFiles = new Set([...animeBatch, ...characterTasks]);
  env.__exportFiles = allFiles.filter((f) => !processedFiles.has(f));
  await clearKv(env, { maxPasses: 3 });
  return {
    ok: true,
    committed: pendingWrites.size,
    remaining_files: env.__exportFiles.length,
    cleared: true,
  };
}
//Pura Dimag Chut Gya 🥀