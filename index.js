import express from "express";
import cors from "cors";
import morgan from "morgan";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "dotenv/config";

const app = express();
app.use(cors());
app.use(morgan("dev"));
app.use(express.json());

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ===== Local paths =====
const DATA_PATH = path.join(__dirname, "data", "videos.json");
const VIDEO_DIR = path.join(__dirname, "media", "videos");
const THUMB_DIR = path.join(__dirname, "media", "thumbs");

// ===== Mode switch =====
const VIDEO_SOURCE = process.env.VIDEO_SOURCE || "local"; // "local" | "aws"

app.get("/__whoami", (req, res) => {
  res.json({
    ok: true,
    server: "mytube-index-js",
    file: __filename,
    cwd: process.cwd(),
    port: process.env.PORT || "(default in code)",
    time: new Date().toISOString(),
  });
});


app.get("/", (req, res) => {
  res.send("MYTUBE INDEX.JS ✅ (if you see this, you're on the right server)");
});

// Read videos.json (local mode)
function readVideosLocal() {
  return JSON.parse(fs.readFileSync(DATA_PATH, "utf-8"));
}

// Build base URL for absolute links
function baseUrl(req) {
  return `${req.protocol}://${req.get("host")}`;
}

// Normalize the API response shape (so React never cares where files live)
function toApiVideo(req, v) {
  const b = baseUrl(req);

  if (VIDEO_SOURCE === "local") {
    return {
      id: v.id,
      title: v.title,
      description: v.description || "",
      category: v.category || "Other",
      visibility: v.visibility || "public",
      channel: v.channel || "MyTube",
      creator: v.creator || v.author || v.channel || null,
      views: v.views ?? null,
      durationText: v.durationText || null,

      // frontend-friendly URLs
      thumbUrl: v.thumb ? `${b}/thumbs/${v.thumb}` : `${b}/thumbs/${v.id}`,
      playbackUrl: `${b}/videos/${v.id}/stream`,
    };
  }

  // AWS mode placeholder
  return {
    id: v.id,
    title: v.title,
    description: v.description || "",
    category: v.category || "Other",
    visibility: v.visibility || "public",
    channel: v.channel || "MyTube",
    creator: v.creator || v.author || v.channel || null,
    views: v.views ?? null,
    durationText: v.durationText || null,
    thumbUrl: v.thumbUrl,
    playbackUrl: v.playbackUrl,
  };
}

// =====================================================
// Handlers (we mount them at BOTH /videos and /api/videos)
// =====================================================

function handleListVideos(req, res) {
  res.set("X-MyTube-Server", "search-title-only-v1");
  const { q, category } = req.query;

  let videos = readVideosLocal();
  const before = videos.length;

  if (category && String(category).trim()) {
    const cat = String(category).trim().toLowerCase();
    videos = videos.filter((v) => (v.category || "").toLowerCase() === cat);
  }

  // ✅ TITLE-ONLY SEARCH
  if (q && String(q).trim()) {
    const query = String(q).trim().toLowerCase();
    videos = videos.filter((v) => (v.title || "").toLowerCase().includes(query));
  }

  console.log(`[LIST] q="${q ?? ""}" category="${category ?? ""}" ${before} -> ${videos.length}`);

  res.json(videos.map((v) => toApiVideo(req, v)));
}

function handleCategories(req, res) {
  const videos = readVideosLocal();
  const categories = [...new Set(videos.map((v) => v.category || "Other"))].sort();
  res.json(categories);
}

function handleGetVideo(req, res) {
  const videos = readVideosLocal();
  const video = videos.find((v) => v.id === req.params.id);
  if (!video) return res.status(404).json({ error: "Not found" });
  res.json(toApiVideo(req, video));
}

// Mount both route styles
app.get("/videos", handleListVideos);
app.get("/api/videos", handleListVideos);

app.get("/categories", handleCategories);
app.get("/api/categories", handleCategories);

app.get("/videos/:id", handleGetVideo);
app.get("/api/videos/:id", handleGetVideo);

// Serve thumbnails (local mode)
app.use("/thumbs", express.static(THUMB_DIR));

// Stream videos with Range support (important!)
app.get("/videos/:id/stream", (req, res) => {
  if (VIDEO_SOURCE !== "local") {
    return res.status(404).json({ error: "Streaming endpoint not used in this mode" });
  }

  const videos = readVideosLocal();
  const video = videos.find((v) => v.id === req.params.id);
  if (!video) return res.status(404).end("Not found");

  const filePath = path.join(VIDEO_DIR, video.filename);
  if (!fs.existsSync(filePath)) return res.status(404).end("Missing file");

  const stat = fs.statSync(filePath);
  const fileSize = stat.size;
  const range = req.headers.range;

  const ext = path.extname(filePath).toLowerCase();
  const contentType =
    ext === ".mp4" ? "video/mp4" :
    ext === ".webm" ? "video/webm" :
    "application/octet-stream";

  if (!range) {
    res.writeHead(200, {
      "Content-Length": fileSize,
      "Content-Type": contentType,
    });
    fs.createReadStream(filePath).pipe(res);
    return;
  }

  const parts = range.replace(/bytes=/, "").split("-");
  const start = parseInt(parts[0], 10);
  const end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1;

  if (start >= fileSize) {
    res.status(416).set("Content-Range", `bytes */${fileSize}`).end();
    return;
  }

  const chunkSize = end - start + 1;
  res.writeHead(206, {
    "Content-Range": `bytes ${start}-${end}/${fileSize}`,
    "Accept-Ranges": "bytes",
    "Content-Length": chunkSize,
    "Content-Type": contentType,
  });

  fs.createReadStream(filePath, { start, end }).pipe(res);
});

// ===== Start =====
const PORT = process.env.PORT ? Number(process.env.PORT) : 3001;
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT} (VIDEO_SOURCE=${VIDEO_SOURCE})`);
});
