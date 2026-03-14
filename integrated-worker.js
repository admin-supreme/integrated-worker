import { createClient } from "@libsql/client/web";
/* ---------- Config & globals ---------- */
const CHARACTER_FETCH_LIMIT = 30;
const MAX_REQUESTS = 49;   
const SUBREQUEST_LIMIT = 60;
let requestCounter = 0;
let subrequestCounter = 0;
let GIT_FILES = [];
let BUFFER_COUNT = 0;
const INDEX_CACHE = {};
const COMMIT_LIMIT = 150;
const KV_BUFFER_KEY = "git_buffer_files";
const KV_COUNT_KEY = "git_buffer_count";
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
  return fetch(url, options);
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
    ctx.waitUntil(runScheduled(env, db));
  }
};
/* ---------- Worker1 logic (scheduled DB scan) ---------- */
async function getAnimeProgress(env) {
  guard();
  const v = await env.PROGRESS_KV.get("anime_progress");
  return v ? parseInt(v) : 0;
}
async function saveAnimeProgress(env, id) {
  guard();
  await env.PROGRESS_KV.put("anime_progress", String(id));
}
async function getProgress(env, animeId) {
  guard();
  const v = await env.PROGRESS_KV.get(`progress:${animeId}`);
  return v ? parseInt(v) : 0;
}
async function saveProgress(env, animeId, idx) {
  guard();
  await env.PROGRESS_KV.put(`progress:${animeId}`, String(idx));
}

async function runScheduled(env, db) {
  let lastId = await getAnimeProgress(env) || 0;
  let skipCount = 0, MAX_SKIP = 12;
  for (let iteration = 0; iteration < 3; iteration++) {
    guard();
    const res = await db.execute({
      sql: `SELECT mal_id, id, title FROM anime_info WHERE id > ? AND mal_id IS NOT NULL ORDER BY id LIMIT 1`,
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
      if (skipCount >= MAX_SKIP) { console.log("Skip limit reached"); break; }
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
      try { charData = await charRes.json(); } catch {
        console.error("Invalid JSON from Jikan");
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
      const startIndex = await getProgress(env, anime.id) || 0;
      const endIndex = Math.min(startIndex + CHARACTER_FETCH_LIMIT, charData.data.length);
      const slice = charData.data.slice(startIndex, endIndex);
      const charIds = slice.map(en => en.character.mal_id).filter(Boolean);
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

      // Build payload similar to previous worker1 -> worker2 POST
      const payload = {
        animeId: anime.id,
        animeMalId: anime.mal_id,
        characterMalIds: charIds,
        startIndex,
        endIndex,
        title: anime.title,
        last: endIndex >= charData.data.length
      };

      // Instead of env.WORKER2.fetch, call processCharactersBatch directly
      try {
        await processCharactersBatch(payload, env);
      } catch (e) {
        console.error("Error in processCharactersBatch:", e);
      }

      if (payload.last) {
        guard();
        await env.PROGRESS_KV.delete(`progress:${anime.id}`);
        await saveAnimeProgress(env, anime.id);
      } else {
        await saveProgress(env, anime.id, endIndex);
      }

      break; // original loop breaks after successful dispatch
    } catch (err) {
      if (err.message === "SAFE_STOP") {
        console.log("SAFE_STOP triggered; aborting run");
        break;
      }
      console.error("runScheduled error:", err);
    }
  }
}

/* ---------- Worker2 logic (fetchCharacterDetails + batch processing) ---------- */
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

  for (const characterId of charIds) {
    try {
      const full = await fetchCharacterDetails(characterId);
      const payload = {
        animeId,
        animeMalId,
        animeTitle: title,
        character: {
          mal_character_id: characterId,
          name: full.name || full.mal_name || null,
          role: full.role || null,
          ...full
        },
        last: body.last === true
      };

      // Instead of posting to Worker3, call its processor directly
      try {
        await processSingleCharacterPayload(payload, env);
      } catch (e) {
        console.error("Error processing single character payload:", e);
      }

      await sleep(400);
    } catch (e) {
      if (e.message === "SAFE_STOP") {
        console.log("SAFE_STOP in processCharactersBatch");
        throw e;
      }
      console.error("processCharactersBatch character error:", e);
    }
  }
}

async function fetchCharacterDetails(characterId) {
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
    try { data = JSON.parse(text); } catch { return {}; }
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
/* ---------- Worker3 logic (buffer + commit) ---------- */
async function processSingleCharacterPayload(body, env) {
  guard();
  // Ensure buffer is loaded from KV on first use
  if (GIT_FILES.length === 0) {
    const saved = await env.PROGRESS_KV.get(KV_BUFFER_KEY);
    const savedCount = await env.PROGRESS_KV.get(KV_COUNT_KEY);
    if (saved) GIT_FILES = JSON.parse(saved);
    if (savedCount) BUFFER_COUNT = parseInt(savedCount);
  }
  const animeId = body.animeId;
  const character = body.character;
  const animeTitle = body.animeTitle || "";

  if (!animeId || !character) throw new Error("Bad payload");

  const charPath = `characters/${character.mal_character_id}.json`;
  const charContent = JSON.stringify(removeNulls(character), null, 2);

  GIT_FILES.push({ path: charPath, content: charContent });
  BUFFER_COUNT++;
  await env.PROGRESS_KV.put(KV_BUFFER_KEY, JSON.stringify(GIT_FILES));
  await env.PROGRESS_KV.put(KV_COUNT_KEY, String(BUFFER_COUNT));

  // update index ndjson file
  const letter = getIndexLetter(animeTitle || (character.name || ""));
  const indexPath = `index/${letter}.ndjson`;
  try {
    const url = `https://api.github.com/repos/${env.GITHUB_OWNER}/${env.GITHUB_REPO}/contents/${indexPath}`;
    let existingRows = INDEX_CACHE[indexPath];
if (!existingRows) {
  const existing = await countedFetch(url, {
    headers: {
  Authorization: `token ${env.GITHUB_TOKEN}`,
  "Content-Type": "application/json",
  "User-Agent": "cf-worker"
}
  });
  let existingContent = "";
  if (existing.status === 200) {
    const data = await existing.json();
    if (data.content) existingContent = atob(data.content.replace(/\s/g, ""));
  }
  existingRows = existingContent
    ? existingContent.split("\n").filter(Boolean).map(l => JSON.parse(l))
    : [];
  INDEX_CACHE[indexPath] = existingRows;
}
    const idx = existingRows.findIndex(r => r.ai === animeId);
    if (idx >= 0) {
      const row = existingRows[idx];
      const cf = Array.isArray(row.cf) ? row.cf : [];
      if (!cf.includes(character.mal_character_id)) cf.push(character.mal_character_id);
      existingRows[idx] = { ai: animeId, cf };
      INDEX_CACHE[indexPath] = existingRows;
    } else {
      existingRows.push({ ai: animeId, cf: [character.mal_character_id] });
      INDEX_CACHE[indexPath] = existingRows;
    }
    INDEX_CACHE[indexPath] = existingRows;
    await env.PROGRESS_KV.put(KV_BUFFER_KEY, JSON.stringify(GIT_FILES));
    await env.PROGRESS_KV.put(KV_COUNT_KEY, String(BUFFER_COUNT));
  } catch (e) {
    if (e && e.message && e.message.includes("SAFE_STOP")) throw e;
    const newContentStr = JSON.stringify({ ai: animeId, cf: [character.mal_character_id] }) + "\n";
    GIT_FILES.push({ path: indexPath, content: newContentStr });
    BUFFER_COUNT++;
    await env.PROGRESS_KV.put(KV_BUFFER_KEY, JSON.stringify(GIT_FILES));
    await env.PROGRESS_KV.put(KV_COUNT_KEY, String(BUFFER_COUNT));
  }

  if (BUFFER_COUNT >= COMMIT_LIMIT) {
    try {
      await commitGitFiles(env);
      GIT_FILES = [];
      BUFFER_COUNT = 0;
      await env.PROGRESS_KV.delete(KV_BUFFER_KEY);
      await env.PROGRESS_KV.delete(KV_COUNT_KEY);
    } catch (err) {
      console.error("commitGitFiles error:", err);
    }
  }

  if (body.last === true) {
    await env.PROGRESS_KV.delete(`progress:${animeId}`);
    await env.PROGRESS_KV.put("anime_progress", String(animeId));
  }
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
    const txt = await res.text();
    throw new Error(`GitHub ${step} failed: ${res.status} ${txt}`);
  }
}
async function commitGitFiles(env) {
  if (!GIT_FILES || GIT_FILES.length === 0) {
    console.log("No files to commit");
    GIT_FILES = [];
    BUFFER_COUNT = 0;
    await env.PROGRESS_KV.delete(KV_BUFFER_KEY);
    await env.PROGRESS_KV.delete(KV_COUNT_KEY);
    return;
  }
  const repoUrl = `https://api.github.com/repos/${env.GITHUB_OWNER}/${env.GITHUB_REPO}`;
  const branchRes = await countedFetch(`${repoUrl}/branches/main`, {
  headers: {
  Authorization: `token ${env.GITHUB_TOKEN}`,
  "Content-Type": "application/json",
  "User-Agent": "cf-worker"
}
});
  await ensureGithubOk(branchRes, "get branch");
  const branchData = await branchRes.json();
  const baseSha = branchData.commit.sha;
  const commitRes = await countedFetch(`${repoUrl}/git/commits/${baseSha}`, {
  headers: {
    Authorization: `token ${env.GITHUB_TOKEN}`,
    "User-Agent": "cf-worker"
  }
});
  await ensureGithubOk(commitRes, "get commit");
  const commitData = await commitRes.json();
for (const indexPath in INDEX_CACHE) {
  const rows = INDEX_CACHE[indexPath];
  const content = rows.map(r => JSON.stringify(r)).join("\n") + "\n";
  GIT_FILES.push({
    path: indexPath,
    content
  });
}
  const treeRes = await countedFetch(`${repoUrl}/git/trees`, {
    method: "POST",
    headers: {
  Authorization: `token ${env.GITHUB_TOKEN}`,
  "Content-Type": "application/json",
  "User-Agent": "cf-worker"
},
    body: JSON.stringify({
      base_tree: commitData.tree.sha,
      tree: GIT_FILES.map(f => ({
        path: f.path,
        mode: "100644",
        type: "blob",
        content: f.content
      }))
    })
  });
  await ensureGithubOk(treeRes, "create tree");
  const treeData = await treeRes.json();
  const newCommitRes = await countedFetch(`${repoUrl}/git/commits`, {
    method: "POST",
    headers: {
  Authorization: `token ${env.GITHUB_TOKEN}`,
  "Content-Type": "application/json",
  "User-Agent": "cf-worker"
},
    body: JSON.stringify({
      message: `Upload ${GIT_FILES.length} files`,
      tree: treeData.sha,
      parents: [baseSha]
    })
  });
  await ensureGithubOk(newCommitRes, "create commit");
  const newCommitData = await newCommitRes.json();
  const refRes = await countedFetch(`${repoUrl}/git/refs/heads/main`, {
    method: "PATCH",
    headers: {
  Authorization: `token ${env.GITHUB_TOKEN}`,
  "Content-Type": "application/json",
  "User-Agent": "cf-worker"
},
    body: JSON.stringify({ sha: newCommitData.sha })
  });
  await ensureGithubOk(refRes, "update ref");
  const verifyRes = await countedFetch(`${repoUrl}/branches/main`, {
    headers: {
  Authorization: `token ${env.GITHUB_TOKEN}`,
  "User-Agent": "cf-worker"
}
  });
  await ensureGithubOk(verifyRes, "verify branch");
  const verifyData = await verifyRes.json();
  if (verifyData.commit.sha !== newCommitData.sha) {
    throw new Error("GitHub commit verification failed");
  }
  console.log("Committed", GIT_FILES.length, "files to repo.");
GIT_FILES = [];
BUFFER_COUNT = 0;
for (const k in INDEX_CACHE) delete INDEX_CACHE[k];
}