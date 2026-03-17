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
async function appendToBuffer(env, fileObj) {
  if (!env.__kvBuffer) {
    env.__kvBuffer = await getBuffer(env);
  }
  env.__kvBuffer.push(fileObj);
if (env.__kvBuffer.length % 10 === 0) {
  await saveBuffer(env, env.__kvBuffer);
}
}
async function getBufferCount(env) {
  try {
    const v = await env.PROGRESS_KV.get(GIT_BUFFER_COUNT_KEY);
    return v ? parseInt(v, 10) : 0;
  } catch (e) {
    console.error("getBufferCount error:", e);
    return 0;
  }
}
async function clearBuffer(env) {
  try {
    await env.PROGRESS_KV.delete(GIT_BUFFER_KV_KEY);
    await env.PROGRESS_KV.put(GIT_BUFFER_COUNT_KEY, "0");
  } catch (e) {
    console.error("clearBuffer error:", e);
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
  const totalBuffer = env.__kvBuffer
  ? env.__kvBuffer.length
  : await getBufferCount(env);
  const isFlushMode = totalBuffer >= GIT_BUFFER_FLUSH_THRESHOLD;
  console.log(`FLUSH MODE CHECK: buffer=${totalBuffer}, flush=${isFlushMode}`);
  if (isFlushMode) {
    console.log(`FLUSH MODE ACTIVATED`);
    const allFilesFull = await getBuffer(env);
    const allFiles = allFilesFull.slice(0, MAX_FLUSH_FILES);
    const remainingFiles = allFilesFull.slice(MAX_FLUSH_FILES);
    if (allFiles.length === 0) {
      console.log("Buffer empty unexpectedly");
      return;
    }
    const CHUNK_SIZE = 100;
    for (let i = 0; i < allFiles.length; i += CHUNK_SIZE) {
      if (processedChunks >= MAX_CHUNKS_PER_RUN) {
        console.log("Stopping early (subrequest safety)");
        break;
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
    return; 
  }
} catch (e) {
  console.error("Flush check failed:", e);
}
  let skipCount = 0;
const MAX_SKIP = 12;
for (let iteration = 0; iteration < 3; iteration++) {
  let payload = undefined;
  guard();
  try {
    const res = await db.execute({
      sql: 'SELECT mal_id, id, title FROM anime_info WHERE id > ? AND mal_id IS NOT NULL ORDER BY id LIMIT 1',
      args: [lastId]
    });
    if (res.rows.length === 0) {
      console.log("No more anime to process. Exiting run.");
      break;
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
      if (payload.last && !safeStopped && actualEndIndex >= charData.data.length) {
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
if (env.__kvBuffer && env.__kvBuffer.length > 0) {
  try {
    await saveBuffer(env, env.__kvBuffer);
    console.log("Saved buffer to KV:", env.__kvBuffer.length);
  } catch (e) {
    console.error("Failed saving KV buffer:", e);
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
async function fetchCharacterDetails(characterId, env) {
  try {
    let res = await countedFetch(`https://api.jikan.moe/v4/characters/${characterId}/full`);
    if (res.status === 404) return {};
    if (res.status === 429) {
      console.log("Jikan rate limit hit in details fetch, waiting 3s...");
      await sleep(3000);
      res = await countedFetch(`https://api.jikan.moe/v4/characters/${characterId}/full`);
      if (res.status === 429) {
        console.log("Still rate limited; skipping character:", characterId);
        return {};
      }
    }
    const text = await res.text();
    let data;
    try {
  data = JSON.parse(text);
} catch {
  console.log("Invalid JSON for character:", characterId);
  return null;
}
    const c = data.data || {};
    const parsed = parseAbout(c.about || "");
    return {
      name: c.name || null,
      mal_id: c.mal_id || characterId,
      about: parsed.cleaned_about,
      gender: c.gender || parsed.gender || null,
      birthday: c.birthday || parsed.birth_date || null,
      height_cm: parsed.height_cm || (c.height_cm || null),
      weight_kg: parsed.weight_kg || (c.weight_kg || null),
      blood_type: c.blood_type || null,
      hair_color: c.hair_color || null,
      eye_color: c.eye_color || null,
      occupation: c.occupation || null,
      affiliations: parsed.affiliations || c.affiliations || null,
      abilities: parsed.abilities || c.abilities || null,
      image: c.images?.jpg?.image_url || null
    };
  } catch (err) {
    if (err.message === "SAFE_STOP") throw err;
    console.error("fetchCharacterDetails error:", err);
    return {};
  }
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
async function commitExport(env) {
  if (!env.__exportFiles || env.__exportFiles.length === 0) return;

  const files = env.__exportFiles.filter(f => f?.path && f?.content);
  if (files.length === 0) {
    env.__exportFiles = [];
    return;
  }
  if (!env.GITHUB_REPO) throw new Error("GITHUB_REPO env var missing");
  if (!env.GITHUB_TOKEN) throw new Error("GITHUB_TOKEN env var missing");
  const repo = env.GITHUB_REPO;
  const headers = {
    Authorization: `Bearer ${env.GITHUB_TOKEN}`,
    Accept: "application/vnd.github+json",
    "Content-Type": "application/json",
    "User-Agent": "Integrated-worker"
  };
  const MAX_ATTEMPTS = 3;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      const refRes = await countedFetch(`https://api.github.com/repos/${repo}/git/ref/heads/main?ts=${Date.now()}`, {
        headers: { ...headers, "Cache-Control": "no-cache" }
      });
      if (!refRes.ok) throw new Error(`GitHub ref fetch failed: ${await refRes.text()}`);
      const refData = await refRes.json();
      const latestCommitSha = refData?.object?.sha;
      if (!latestCommitSha) throw new Error("GitHub ref response missing object.sha");
      const commitRes = await countedFetch(`https://api.github.com/repos/${repo}/git/commits/${latestCommitSha}`, { headers });
      if (!commitRes.ok) throw new Error(`GitHub commit fetch failed: ${await commitRes.text()}`);
      const commitData = await commitRes.json();
      const baseTree = commitData?.tree?.sha;
      if (!baseTree) throw new Error("GitHub commit response missing tree.sha");
      const treeItems = files.map(f => ({
        path: f.path,
        mode: "100644",
        type: "blob",
        content: decodeBase64Utf8(f.content)
      }));
      const treeRes = await countedFetch(`https://api.github.com/repos/${repo}/git/trees`, {
        method: "POST",
        headers,
        body: JSON.stringify({ base_tree: baseTree, tree: treeItems })
      });
      if (!treeRes.ok) throw new Error(`GitHub tree creation failed: ${await treeRes.text()}`);
      const treeData = await treeRes.json();
      if (!treeData?.sha) throw new Error("GitHub tree creation missing sha");
      const newCommitRes = await countedFetch(`https://api.github.com/repos/${repo}/git/commits`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          message: `Auto export dataset (Chunk attempt ${attempt})`,
          tree: treeData.sha,
          parents: [latestCommitSha]
        })
      });
      if (!newCommitRes.ok) throw new Error(`GitHub new commit failed: ${await newCommitRes.text()}`);
      const newCommit = await newCommitRes.json();
      if (!newCommit?.sha) throw new Error("GitHub new commit missing sha");
      const refUpdate = await countedFetch(`https://api.github.com/repos/${repo}/git/refs/heads/main`, {
        method: "PATCH",
        headers,
        body: JSON.stringify({ sha: newCommit.sha })
      });
      if (refUpdate.ok) {
        console.log(`commitExport success: ${files.length} files committed on attempt ${attempt}`);
        env.__exportFiles = []; 
        return;
      }
      throw new Error(`GitHub ref update failed: ${await refUpdate.text()}`);
    } catch (err) {
      console.warn(`commitExport error on attempt ${attempt}:`, err.message);
      if (attempt === MAX_ATTEMPTS) {
        env.__exportFiles = []; 
        throw err;
      }
      await new Promise(r => setTimeout(r, 600 * attempt));
    }
  }
}
async function processSingleCharacterPayload(body, env) {
  const repo =
    env.GITHUB_REPO && env.GITHUB_REPO.includes("/")
      ? env.GITHUB_REPO
      : `${env.GITHUB_OWNER}/${env.GITHUB_REPO}`;
  if (!env.GITHUB_TOKEN) throw new Error("Missing env.GITHUB_TOKEN");
if (!repo) {
  throw new Error(
    "Missing GITHUB_REPO (owner/repo) or GITHUB_OWNER + GITHUB_REPO"
  );
}
  guard();
  if (!body) throw new Error("No body provided to processSingleCharacterPayload");
  const animeId = body.animeId;
  const character = body.character;
  const animeTitle = body.animeTitle || "";
  if (!animeId || !character) throw new Error("Bad payload");
  const charPath = `characters/${character.mal_character_id}.json`;
  const charContent = JSON.stringify(removeNulls(character), null, 2);
  const letter = getIndexLetter(animeTitle || (character.name || ""));
  const indexPath = `index/${letter}.ndjson`;
let existingRows = [];
  try {
    const res = await countedFetch(
      `https://api.github.com/repos/${repo}/contents/${indexPath}`,
      {
  headers: {
    Authorization: `Bearer ${env.GITHUB_TOKEN}`,
    Accept: "application/vnd.github+json",
    "User-Agent": "Integrated-worker"
  }
}
    );
    if (res.status === 200) {
      const data = await res.json();
      const content = data.content
  ? decodeBase64Utf8(data.content.replace(/\s/g, ""))
  : "";
      existingRows = content
        .split("\n")
        .filter(Boolean)
        .map(line => {
          try { return JSON.parse(line); } catch { return null; }
        })
        .filter(Boolean);
    } else if (res.status !== 404) {
      const txt = await res.text();
      console.error("GitHub fetch error:", txt);
    }
  } catch (e) {
    console.error("Index fetch failed:", e);
  }
  const map = new Map();
for (const row of existingRows) {
  if (!row || !row.ai) continue;
  map.set(row.ai, new Set(row.cf || []));
}
if (!map.has(animeId)) {
  map.set(animeId, new Set());
}
map.get(animeId).add(character.mal_character_id);
existingRows = Array.from(map.entries()).map(([ai, cfSet]) => ({
  ai,
  cf: Array.from(cfSet)
}));
  const indexContent = existingRows.map(r => JSON.stringify(r)).join("\n") + "\n";
try {
await appendToBuffer(env, {
  path: charPath,
  content: toBase64Utf8(charContent)
});
await appendToBuffer(env, {
  path: indexPath,
  content: toBase64Utf8(indexContent)
});
  try {
    const count = env.__kvBuffer ? env.__kvBuffer.length : await getBufferCount(env);
if (count % 50 === 0) {
  console.log(`Buffered files count: ${count}`);
}
  } catch (e) {
  }
} catch (err) {
  if (err && err.message && err.message.includes("SAFE_STOP")) throw err;
  console.error("Error buffering character files to PROGRESS_KV:", err);
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
function removeNulls(obj) {
  const clean = {};
  for (const k in obj) if (obj[k] !== null && obj[k] !== undefined) clean[k] = obj[k];
  return clean;
}
function getIndexLetter(title) {
  if (!title) return "#";
  const first = String(title)[0]?.toLowerCase();
  if (/[a-z]/.test(first)) return first;
  return "#";
}
async function ensureGithubOk(res, step) {
  if (!res.ok) {
    const txt = await res.text().catch(() => "<no-body>");
    console.error(`GitHub ${step} failed: status=${res.status} body=${txt.substring(0,2000)}`);
    throw new Error(`GitHub ${step} failed: ${res.status} ${txt}`);
  }
}
function toBase64Utf8(str) {
  const bytes = new TextEncoder().encode(str);
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}
function decodeBase64Utf8(base64) {
  try {
    const binary = atob(base64);
    const bytes = Uint8Array.from(binary, c => c.charCodeAt(0));
    return new TextDecoder().decode(bytes);
  } catch (e) {
    console.error("decodeBase64Utf8 failed:", e);
    return "";
  }
}