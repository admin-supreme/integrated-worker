// worker3.js
let GIT_FILES = [];
let BUFFER_COUNT = 0;
const COMMIT_LIMIT = 150;
const KV_BUFFER_KEY = "git_buffer_files";
const KV_COUNT_KEY = "git_buffer_count";
let subrequestCounter = 0;
function countedFetch(url, options){
  subrequestCounter++;
  if (subrequestCounter >= 80) {
    console.log("SAFE_STOP Worker3");
    throw new Error("SAFE_STOP");
  }
  return fetch(url, options);
}
function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }
export default {
  async fetch(request, env) {
    return handle(request, env);
  }
};
async function handle(req, env){
if (GIT_FILES.length === 0) {
  const saved = await env.PROGRESS_KV.get(KV_BUFFER_KEY);
  const savedCount = await env.PROGRESS_KV.get(KV_COUNT_KEY);
  if (saved) GIT_FILES = JSON.parse(saved);
  if (savedCount) BUFFER_COUNT = parseInt(savedCount);
}
  try {
    if (req.method !== "POST") return new Response("Method not allowed", { status: 405 });
    const auth = req.headers.get("authorization") || "";
    if (auth !== `Bearer ${env.WORKER_SHARED_AUTH}`) return new Response("Unauthorized", { status: 401 });
    const body = await req.json();
    const animeId = body.animeId;
    const character = body.character;
    const animeTitle = body.animeTitle || "";
    if (!animeId || !character) return new Response("Bad payload", { status: 400 });
    const charPath = `characters/${character.mal_character_id}.json`;
    const charContent = JSON.stringify(removeNulls(character), null, 2);
    GIT_FILES.push({ path: charPath, content: charContent });
BUFFER_COUNT++;
await env.PROGRESS_KV.put(KV_BUFFER_KEY, JSON.stringify(GIT_FILES));
await env.PROGRESS_KV.put(KV_COUNT_KEY, String(BUFFER_COUNT));
    const letter = getIndexLetter(animeTitle || (character.name || ""));
    const indexPath = `index/${letter}.ndjson`;
    try {
      const url = `https://api.github.com/repos/${env.GITHUB_OWNER}/${env.GITHUB_REPO}/contents/${indexPath}`;
      const existing = await countedFetch(url, {
        headers: {
          Authorization: `Bearer ${env.GITHUB_TOKEN}`,
          "User-Agent": "worker3"
        }
      });
      let existingContent = "";
      let existingRows = [];
      if (existing.status === 200) {
        const data = await existing.json();
        if (data.content) existingContent = atob(data.content.replace(/\s/g, ""));
        existingRows = existingContent.split("\n").filter(Boolean).map(l => JSON.parse(l));
      }
      const idx = existingRows.findIndex(r => r.ai === animeId);
      if (idx >= 0) {
        const row = existingRows[idx];
        const cf = Array.isArray(row.cf) ? row.cf : [];
        if (!cf.includes(character.mal_character_id)) cf.push(character.mal_character_id);
        existingRows[idx] = { ai: animeId, cf };
      } else {
        existingRows.push({ ai: animeId, cf: [character.mal_character_id] });
      }
      const newContentStr = existingRows.map(r => JSON.stringify(r)).join("\n") + "\n";
      GIT_FILES.push({ path: indexPath, content: newContentStr });
BUFFER_COUNT++;
await env.PROGRESS_KV.put(KV_BUFFER_KEY, JSON.stringify(GIT_FILES));
await env.PROGRESS_KV.put(KV_COUNT_KEY, String(BUFFER_COUNT));
    } catch (e) {
  if (e && e.message && e.message.includes("SAFE_STOP")) throw e;
  const newContentStr =
    JSON.stringify({ ai: animeId, cf: [character.mal_character_id] }) + "\n";
  GIT_FILES.push({ path: indexPath, content: newContentStr });
  BUFFER_COUNT++;
  await env.PROGRESS_KV.put(KV_BUFFER_KEY, JSON.stringify(GIT_FILES));
  await env.PROGRESS_KV.put(KV_COUNT_KEY, String(BUFFER_COUNT));
}
    if (BUFFER_COUNT >= COMMIT_LIMIT) {
  await commitGitFiles(env);
  GIT_FILES = [];
  BUFFER_COUNT = 0;
  await env.PROGRESS_KV.delete(KV_BUFFER_KEY);
  await env.PROGRESS_KV.delete(KV_COUNT_KEY);
}
    if (body.last === true) {
      await env.PROGRESS_KV.delete(`progress:${animeId}`);
      await env.PROGRESS_KV.put("anime_progress", String(animeId));
    }
    return new Response("OK", { status: 200 });
  } catch (err) {
    if (err.message === "SAFE_STOP") {
      console.log("SAFE_STOP in Worker3");
      return new Response("SAFE_STOP", { status: 500 });
    }
    console.error("Worker3 error:", err);
    return new Response("Internal error", { status: 500 });
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
    headers: { Authorization: `Bearer ${env.GITHUB_TOKEN}` }
  });
  const branchData = await branchRes.json();
  const baseSha = branchData.commit.sha;
  const commitRes = await countedFetch(`${repoUrl}/git/commits/${baseSha}`, {
    headers: { Authorization: `Bearer ${env.GITHUB_TOKEN}` }
  });
  const commitData = await commitRes.json();
  const treeRes = await countedFetch(`${repoUrl}/git/trees`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.GITHUB_TOKEN}`,
      "Content-Type": "application/json"
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
  const treeData = await treeRes.json();
  const newCommitRes = await countedFetch(`${repoUrl}/git/commits`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.GITHUB_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      message: `Upload ${GIT_FILES.length} files`,
      tree: treeData.sha,
      parents: [baseSha]
    })
  });
  const newCommitData = await newCommitRes.json();
  await countedFetch(`${repoUrl}/git/refs/heads/main`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${env.GITHUB_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ sha: newCommitData.sha })
  });
  console.log("Committed", GIT_FILES.length, "files to repo.");
  GIT_FILES = [];
BUFFER_COUNT = 0;
}