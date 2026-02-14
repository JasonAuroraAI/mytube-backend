import express from "express";
import cors from "cors";
import morgan from "morgan";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "dotenv/config";
import cookieParser from "cookie-parser";

import authRouter from "./auth.js";
import profileRouter from "./profile.js";
import { pool } from "./db.js";

import multer from "multer";
import crypto from "crypto";
import { spawn } from "child_process";
import os from "os";
import dotenv from "dotenv";
dotenv.config();

console.log("Using DB:", process.env.DATABASE_URL);



// ✅ S3 helpers (single import, consistent exports)
import {
  uploadDirToS3,
  uploadFileToS3,
  deletePrefixFromS3,
  deleteFromS3,
} from "./aws/s3Helpers.js";

const app = express();
const ORIGIN = process.env.CLIENT_ORIGIN || "http://localhost:5173";

const allowed = [
  "http://localhost:5173",
  "http://127.0.0.1:5173",
];

app.use(cors({
  origin: (origin, cb) => {
    if (!origin) return cb(null, true); // curl/postman
    if (allowed.includes(origin) || origin.endsWith(".vercel.app")) return cb(null, true);
    return cb(new Error("Not allowed by CORS"));
  },
  credentials: true,
}));

app.use(morgan("dev"));
app.use(express.json());
app.use(cookieParser());

// -------------------------
// Paths / storage
// -------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const THUMB_DIR = path.join(__dirname, "data", "thumbs");
const VIDEO_DIR = path.join(__dirname, "data", "videos");

fs.mkdirSync(THUMB_DIR, { recursive: true });
fs.mkdirSync(VIDEO_DIR, { recursive: true });

const VIDEO_SOURCE = process.env.VIDEO_SOURCE || "local"; // "local" | "aws"

// CloudFront base URLs (optional)
const CDN_UPLOADS_BASE_URL = (process.env.CDN_UPLOADS_BASE_URL || "").replace(/\/$/, "");
const CDN_ASSETS_BASE_URL = (process.env.CDN_ASSETS_BASE_URL || "").replace(/\/$/, "");

// -------------------------
// Session -> req.user
// -------------------------
async function getUserFromSession(req) {
  const sid = req.cookies?.session_id;
  if (!sid) return null;

  const result = await pool.query(
    `
    SELECT u.id, u.username, u.tokens, u.rating, u.review_count
    FROM sessions s
    JOIN users u ON u.id = s.user_id
    WHERE s.id = $1 AND s.expires_at > now()
    `,
    [sid]
  );

  if (!result.rows[0]) return null;

  const u = result.rows[0];
  return {
    id: u.id,
    username: u.username,
    tokens: u.tokens,
    rating: u.rating,
    reviewCount: u.review_count,
  };
}

// attach req.user early
app.use(async (req, _res, next) => {
  try {
    req.user = await getUserFromSession(req);
  } catch {
    req.user = null;
  }
  next();
});

async function requireAuth(req, res, next) {
  const user = req.user ?? (await getUserFromSession(req));
  if (!user) return res.status(401).json({ error: "Not logged in" });
  req.user = user;
  next();
}

app.use(cors({
  origin: "http://localhost:5173",
  credentials: true
}));


// routers
app.use("/auth", authRouter);
app.use("/api/profile", profileRouter);

// -------------------------
// Upload (multer) -> always to local disk first
// -------------------------
const upload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, VIDEO_DIR),
    filename: (_req, file, cb) => {
      const ext = path.extname(file.originalname).toLowerCase() || ".mp4";
      const name = `${Date.now()}-${crypto.randomBytes(8).toString("hex")}${ext}`;
      cb(null, name);
    },
  }),
  limits: { fileSize: 1024 * 1024 * 1024 }, // 1GB
  fileFilter: (_req, file, cb) => {
    const ok =
      file.mimetype === "video/mp4" ||
      path.extname(file.originalname).toLowerCase() === ".mp4";
    cb(ok ? null : new Error("Only .mp4 allowed"), ok);
  },
});

// -------------------------
// FFMPEG helpers
// -------------------------
function runCmd(cmd, args) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { windowsHide: true });

    let out = "";
    let err = "";

    p.stdout.on("data", (d) => (out += d.toString()));
    p.stderr.on("data", (d) => (err += d.toString()));

    p.on("error", reject);
    p.on("close", (code) => {
      if (code === 0) resolve({ out, err });
      else reject(new Error(err || `${cmd} exited with code ${code}`));
    });
  });
}

async function getVideoDurationSeconds(videoPath) {
  try {
    const { out } = await runCmd("ffprobe", [
      "-v",
      "error",
      "-show_entries",
      "format=duration",
      "-of",
      "default=noprint_wrappers=1:nokey=1",
      videoPath,
    ]);
    const dur = Number(String(out).trim());
    if (!Number.isFinite(dur) || dur <= 0) return null;
    return dur;
  } catch {
    return null;
  }
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

async function generateThumbnailAtSecond(videoPath, thumbPath, seconds) {
  await runCmd("ffmpeg", [
    "-y",
    "-hide_banner",
    "-loglevel",
    "error",
    "-ss",
    String(seconds),
    "-i",
    videoPath,
    "-frames:v",
    "1",
    "-vf",
    "scale=640:-1",
    "-q:v",
    "3",
    thumbPath,
  ]);
}

async function generateThumbnailHalfwayWithFallback(videoPath, thumbPath) {
  const dur = await getVideoDurationSeconds(videoPath);

  let candidates = [];
  if (dur && dur > 0) {
    const half = clamp(Math.floor(dur * 0.5), 1, Math.max(1, Math.floor(dur - 1)));
    candidates = [half, 30, 10, 3, 1]
      .map((t) => clamp(Number(t), 0, Math.max(0, Math.floor(dur - 0.25))))
      .filter((t) => Number.isFinite(t) && t >= 0);
    candidates = Array.from(new Set(candidates));
  } else {
    candidates = [30, 10, 3, 1];
  }

  let lastErr = null;
  for (const t of candidates) {
    try {
      await generateThumbnailAtSecond(videoPath, thumbPath, t);
      return { ok: true, usedSecond: t, duration: dur };
    } catch (e) {
      lastErr = e;
      try {
        if (fs.existsSync(thumbPath)) fs.unlinkSync(thumbPath);
      } catch {}
    }
  }
  throw lastErr || new Error("Thumbnail generation failed");
}

// -------------------------
// HLS generation (single VOD rendition)
// -------------------------
async function generateHlsVOD(inputPath, outDir) {
  fs.mkdirSync(outDir, { recursive: true });

  const args = [
    "-y",
    "-hide_banner",
    "-loglevel",
    "error",
    "-i",
    inputPath,

    "-c:v",
    "libx264",
    "-preset",
    "veryfast",
    "-crf",
    "22",
    "-c:a",
    "aac",
    "-b:a",
    "128k",
    "-ac",
    "2",

    "-f",
    "hls",
    "-hls_time",
    "6",
    "-hls_playlist_type",
    "vod",
    "-hls_flags",
    "independent_segments",
    "-hls_segment_type",
    "mpegts",
    "-hls_segment_filename",
    path.join(outDir, "seg_%03d.ts"),

    path.join(outDir, "master.m3u8"),
  ];

  await runCmd("ffmpeg", args);
}

// -------------------------
// Rating stats helper (resilient)
// -------------------------
async function getRatingStats(videoId) {
  try {
    const result = await pool.query(
      `
      SELECT rating_avg, rating_count
      FROM video_rating_stats
      WHERE video_id::text = $1::text
      `,
      [String(videoId)]
    );

    if (result.rows.length === 0) return { ratingAvg: null, ratingCount: 0 };

    return {
      ratingAvg: Number(result.rows[0].rating_avg),
      ratingCount: Number(result.rows[0].rating_count),
    };
  } catch (e) {
    console.warn("getRatingStats failed (fallback):", e.message);
    return { ratingAvg: null, ratingCount: 0 };
  }
}

// -------------------------
// DB fetches
// -------------------------
async function fetchVideosFromDb() {
  const result = await pool.query(
    `
    SELECT
      v.id,
      v.user_id,
      v.title,
      v.description,
      v.category,
      v.visibility,
      v.filename,
      v.thumb,
      v.duration_text,
      v.views,
      v.tags,
      v.created_at AS "createdAt",
      v.updated_at AS "updatedAt",
      u.username AS channel_username,
      COALESCE(p.display_name, '') AS channel_display_name
    FROM videos v
    JOIN users u ON u.id = v.user_id
    LEFT JOIN user_profiles p ON p.user_id = u.id
    WHERE v.visibility = 'public'
    ORDER BY v.created_at DESC
    LIMIT 200
    `
  );
  return result.rows;
}

async function fetchVideoById(videoId) {
  const result = await pool.query(
    `
    SELECT
      v.id,
      v.user_id,
      v.title,
      v.description,
      v.category,
      v.visibility,
      v.filename,
      v.thumb,
      v.duration_text,
      v.views,
      v.tags,
      v.created_at AS "createdAt",
      v.updated_at AS "updatedAt",
      u.username AS channel_username,
      COALESCE(p.display_name, '') AS channel_display_name
    FROM videos v
    JOIN users u ON u.id = v.user_id
    LEFT JOIN user_profiles p ON p.user_id = u.id
    WHERE v.id::text = $1::text
    LIMIT 1
    `,
    [String(videoId)]
  );

  return result.rows[0] || null;
}

function baseUrl(req) {
  return `${req.protocol}://${req.get("host")}`;
}

// ✅ FIXED: declare playbackUrl properly
async function toApiVideo(req, v) {
  const b = baseUrl(req);
  const { ratingAvg, ratingCount } = await getRatingStats(v.id);

  let playbackUrl = `${b}/videos/${v.id}/stream`;

  if (VIDEO_SOURCE === "aws") {
    // v.filename is expected to be the S3 key (e.g. hls/user/token/master.m3u8)
    if (CDN_UPLOADS_BASE_URL) {
      playbackUrl = `${CDN_UPLOADS_BASE_URL}/${v.filename}`;
    } else if (process.env.S3_UPLOADS_BUCKET && process.env.AWS_REGION) {
      playbackUrl = `https://${process.env.S3_UPLOADS_BUCKET}.s3.${process.env.AWS_REGION}.amazonaws.com/${v.filename}`;
    }
  }

  const thumbUrl =
    VIDEO_SOURCE === "aws" &&
    v.thumb &&
    v.thumb !== "placeholder.jpg" &&
    CDN_ASSETS_BASE_URL
      ? `${CDN_ASSETS_BASE_URL}/${v.thumb}`
      : v.thumb
      ? `${b}/thumbs/${v.thumb}`
      : `${b}/thumbs/placeholder.jpg`;

  return {
    id: v.id,
    title: v.title,
    description: v.description || "",
    category: v.category || "Other",
    visibility: v.visibility || "public",

    channelUsername: v.channel_username,
    channelDisplayName: v.channel_display_name || v.channel_username,

    createdAt: v.createdAt,
    updatedAt: v.updatedAt,
    views: v.views ?? null,
    durationText: v.duration_text || null,
    tags: Array.isArray(v.tags) ? v.tags : [],

    ratingAvg,
    ratingCount,

    thumbUrl,
    playbackUrl,
  };
}

// -------------------------
// COMMENTS API (list)
// -------------------------
app.get("/api/videos/:videoId/comments", async (req, res) => {
  const videoId = req.params.videoId;

  const limit = Math.min(Number(req.query.limit || 50), 200);
  const offset = Math.max(Number(req.query.offset || 0), 0);

  const myUserId = req.user?.id ?? null;

  try {
    const top = await pool.query(
      `
      SELECT
        c.id,
        c.video_id,
        c.user_id,
        c.body,
        c.created_at,
        c.updated_at,

        u.username,
        COALESCE(p.display_name, '') AS display_name,

        COALESCE(cls.like_count, 0) AS like_count,

        CASE
          WHEN $3::bigint IS NULL THEN false
          ELSE EXISTS (
            SELECT 1
            FROM comment_likes cl
            WHERE cl.comment_id = c.id
              AND cl.user_id = $3::bigint
          )
        END AS liked_by_me
      FROM video_comments c
      JOIN users u ON u.id = c.user_id
      LEFT JOIN user_profiles p ON p.user_id = u.id
      LEFT JOIN comment_like_stats cls ON cls.comment_id = c.id
      WHERE c.video_id = $1
        AND c.parent_comment_id IS NULL
      ORDER BY c.created_at DESC
      LIMIT $2 OFFSET $4
      `,
      [videoId, limit, myUserId, offset]
    );

    const topItems = top.rows.map((r) => ({
      id: Number(r.id),
      videoId: r.video_id,
      userId: Number(r.user_id),
      username: r.username,
      displayName: r.display_name || r.username,
      body: r.body,
      createdAt: r.created_at,
      updatedAt: r.updated_at,
      likeCount: Number(r.like_count),
      likedByMe: !!r.liked_by_me,
      replies: [],
    }));

    const parentIds = topItems.map((c) => c.id);

    if (parentIds.length) {
      const replies = await pool.query(
        `
        SELECT
          c.id,
          c.video_id,
          c.user_id,
          c.parent_comment_id,
          c.body,
          c.created_at,
          c.updated_at,

          u.username,
          COALESCE(p.display_name, '') AS display_name,

          COALESCE(cls.like_count, 0) AS like_count,

          CASE
            WHEN $3::bigint IS NULL THEN false
            ELSE EXISTS (
              SELECT 1
              FROM comment_likes cl
              WHERE cl.comment_id = c.id
                AND cl.user_id = $3::bigint
            )
          END AS liked_by_me
        FROM video_comments c
        JOIN users u ON u.id = c.user_id
        LEFT JOIN user_profiles p ON p.user_id = u.id
        LEFT JOIN comment_like_stats cls ON cls.comment_id = c.id
        WHERE c.video_id = $1
          AND c.parent_comment_id = ANY($2::bigint[])
        ORDER BY c.created_at ASC
        `,
        [videoId, parentIds, myUserId]
      );

      const byParent = new Map();
      for (const r of replies.rows) {
        const parentId = Number(r.parent_comment_id);
        if (!byParent.has(parentId)) byParent.set(parentId, []);
        byParent.get(parentId).push({
          id: Number(r.id),
          videoId: r.video_id,
          userId: Number(r.user_id),
          username: r.username,
          displayName: r.display_name || r.username,
          body: r.body,
          createdAt: r.created_at,
          updatedAt: r.updated_at,
          likeCount: Number(r.like_count),
          likedByMe: !!r.liked_by_me,
        });
      }

      for (const c of topItems) {
        c.replies = byParent.get(c.id) || [];
      }
    }

    return res.json({ videoId, items: topItems, limit, offset });
  } catch (e) {
    console.error("GET /api/videos/:videoId/comments error:", e);
    return res.status(500).json({ error: "Failed to load comments" });
  }
});

// =========================
// COMMENTS API (top-level + one-level replies)
// =========================
app.get("/api/videos/:videoId/comments", async (req, res) => {
  try {
    const videoId = req.params.videoId;

    const limit = Math.min(Number(req.query.limit || 50), 200);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    const myUserId = req.user?.id ?? null;

    // top-level comments
    const top = await pool.query(
      `
      SELECT
        c.id,
        c.video_id,
        c.user_id,
        c.body,
        c.created_at,
        c.updated_at,

        u.username,
        COALESCE(p.display_name, '') AS display_name,

        COALESCE(cls.like_count, 0) AS like_count,

        CASE
          WHEN $3::bigint IS NULL THEN false
          ELSE EXISTS (
            SELECT 1
            FROM comment_likes cl
            WHERE cl.comment_id = c.id
              AND cl.user_id = $3::bigint
          )
        END AS liked_by_me
      FROM video_comments c
      JOIN users u ON u.id = c.user_id
      LEFT JOIN user_profiles p ON p.user_id = u.id
      LEFT JOIN comment_like_stats cls ON cls.comment_id = c.id
      WHERE c.video_id = $1
        AND c.parent_comment_id IS NULL
      ORDER BY c.created_at DESC
      LIMIT $2 OFFSET $4
      `,
      [videoId, limit, myUserId, offset]
    );

    const topItems = top.rows.map((r) => ({
      id: Number(r.id),
      videoId: r.video_id,
      userId: Number(r.user_id),
      username: r.username,
      displayName: r.display_name || r.username,
      body: r.body,
      createdAt: r.created_at,
      updatedAt: r.updated_at,
      likeCount: Number(r.like_count),
      likedByMe: !!r.liked_by_me,
      replies: [],
    }));

    // one-level replies
    const parentIds = topItems.map((c) => c.id);
    if (parentIds.length) {
      const replies = await pool.query(
        `
        SELECT
          c.id,
          c.video_id,
          c.user_id,
          c.parent_comment_id,
          c.body,
          c.created_at,
          c.updated_at,

          u.username,
          COALESCE(p.display_name, '') AS display_name,

          COALESCE(cls.like_count, 0) AS like_count,

          CASE
            WHEN $3::bigint IS NULL THEN false
            ELSE EXISTS (
              SELECT 1
              FROM comment_likes cl
              WHERE cl.comment_id = c.id
                AND cl.user_id = $3::bigint
            )
          END AS liked_by_me
        FROM video_comments c
        JOIN users u ON u.id = c.user_id
        LEFT JOIN user_profiles p ON p.user_id = u.id
        LEFT JOIN comment_like_stats cls ON cls.comment_id = c.id
        WHERE c.video_id = $1
          AND c.parent_comment_id = ANY($2::bigint[])
        ORDER BY c.created_at ASC
        `,
        [videoId, parentIds, myUserId]
      );

      const byParent = new Map();
      for (const r of replies.rows) {
        const pid = Number(r.parent_comment_id);
        if (!byParent.has(pid)) byParent.set(pid, []);
        byParent.get(pid).push({
          id: Number(r.id),
          videoId: r.video_id,
          userId: Number(r.user_id),
          username: r.username,
          displayName: r.display_name || r.username,
          body: r.body,
          createdAt: r.created_at,
          updatedAt: r.updated_at,
          likeCount: Number(r.like_count),
          likedByMe: !!r.liked_by_me,
        });
      }

      for (const c of topItems) {
        c.replies = byParent.get(c.id) || [];
      }
    }

    res.json({ videoId, items: topItems, limit, offset });
  } catch (e) {
    console.error("GET /api/videos/:videoId/comments error:", e);
    res.status(500).json({ error: "Failed to load comments" });
  }
});

app.post("/api/videos/:videoId/comments", requireAuth, async (req, res) => {
  try {
    const videoId = req.params.videoId;
    const userId = req.user.id;

    const body = String(req.body?.body || "").trim();
    const parentCommentId = req.body?.parentCommentId ?? null;

    if (!body) return res.status(400).json({ error: "Comment body required" });
    if (body.length > 2000) return res.status(400).json({ error: "Comment too long" });

    // validate parent if provided (must be top-level comment on same video)
    let parentId = null;
    if (parentCommentId !== null && parentCommentId !== undefined && parentCommentId !== "") {
      const pid = Number(parentCommentId);
      if (!Number.isFinite(pid)) return res.status(400).json({ error: "Bad parentCommentId" });

      const parent = await pool.query(
        `
        SELECT id
        FROM video_comments
        WHERE id = $1
          AND video_id = $2
          AND parent_comment_id IS NULL
        `,
        [pid, videoId]
      );

      if (!parent.rows.length) {
        return res.status(400).json({ error: "Parent comment not found (or not top-level)" });
      }

      parentId = pid;
    }

    const result = await pool.query(
      `
      INSERT INTO video_comments (video_id, user_id, body, parent_comment_id)
      VALUES ($1, $2, $3, $4)
      RETURNING id, video_id, user_id, body, parent_comment_id, created_at, updated_at
      `,
      [videoId, userId, body, parentId]
    );

    const profile = await pool.query(
      `SELECT COALESCE(display_name,'') AS display_name FROM user_profiles WHERE user_id = $1`,
      [userId]
    );
    const displayName = profile.rows[0]?.display_name || req.user.username;

    res.json({
      ok: true,
      comment: {
        id: Number(result.rows[0].id),
        videoId: result.rows[0].video_id,
        userId: Number(result.rows[0].user_id),
        username: req.user.username,
        displayName,
        body: result.rows[0].body,
        parentCommentId: result.rows[0].parent_comment_id
          ? Number(result.rows[0].parent_comment_id)
          : null,
        createdAt: result.rows[0].created_at,
        updatedAt: result.rows[0].updated_at,
        likeCount: 0,
        likedByMe: false,
        replies: [],
      },
    });
  } catch (e) {
    console.error("POST /api/videos/:videoId/comments error:", e);
    res.status(500).json({ error: "Failed to post comment" });
  }
});

// EDIT / DELETE comments
app.patch("/api/comments/:commentId", requireAuth, async (req, res) => {
  const commentId = Number(req.params.commentId);
  const userId = req.user.id;

  if (!Number.isFinite(commentId)) return res.status(400).json({ error: "Bad comment id" });

  const body = String(req.body?.body || "").trim();
  if (!body) return res.status(400).json({ error: "Comment body required" });
  if (body.length > 2000) return res.status(400).json({ error: "Comment too long" });

  try {
    const result = await pool.query(
      `
      UPDATE video_comments
      SET body = $3, updated_at = now()
      WHERE id = $1 AND user_id = $2
      RETURNING id, video_id, user_id, body, parent_comment_id, created_at, updated_at
      `,
      [commentId, userId, body]
    );

    if (!result.rows.length) {
      return res.status(403).json({ error: "Not allowed (or comment not found)" });
    }

    const row = result.rows[0];
    res.json({
      ok: true,
      comment: {
        id: Number(row.id),
        videoId: row.video_id,
        userId: Number(row.user_id),
        body: row.body,
        parentCommentId: row.parent_comment_id ? Number(row.parent_comment_id) : null,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      },
    });
  } catch (e) {
    console.error("PATCH /api/comments/:commentId error:", e);
    res.status(500).json({ error: "Failed to edit comment" });
  }
});

app.delete("/api/comments/:commentId", requireAuth, async (req, res) => {
  const commentId = Number(req.params.commentId);
  const userId = req.user.id;

  if (!Number.isFinite(commentId)) return res.status(400).json({ error: "Bad comment id" });

  try {
    const result = await pool.query(
      `
      DELETE FROM video_comments
      WHERE id = $1 AND user_id = $2
      RETURNING id
      `,
      [commentId, userId]
    );

    if (!result.rows.length) {
      return res.status(403).json({ error: "Not allowed (or comment not found)" });
    }

    res.json({ ok: true, deletedId: commentId });
  } catch (e) {
    console.error("DELETE /api/comments/:commentId error:", e);
    res.status(500).json({ error: "Failed to delete comment" });
  }
});

// likes (toggle style)
app.post("/api/comments/:commentId/toggle-like", requireAuth, async (req, res) => {
  const commentId = Number(req.params.commentId);
  const userId = req.user.id;

  if (!Number.isFinite(commentId)) return res.status(400).json({ error: "Bad comment id" });

  try {
    const existing = await pool.query(
      `SELECT 1 FROM comment_likes WHERE comment_id = $1 AND user_id = $2`,
      [commentId, userId]
    );

    if (existing.rows.length) {
      await pool.query(
        `DELETE FROM comment_likes WHERE comment_id = $1 AND user_id = $2`,
        [commentId, userId]
      );
    } else {
      await pool.query(
        `INSERT INTO comment_likes (comment_id, user_id) VALUES ($1, $2)`,
        [commentId, userId]
      );
    }

    const stats = await pool.query(
      `SELECT COUNT(*)::int AS like_count FROM comment_likes WHERE comment_id = $1`,
      [commentId]
    );

    res.json({
      ok: true,
      commentId,
      liked: !existing.rows.length,
      likeCount: stats.rows[0].like_count,
    });
  } catch (e) {
    console.error("POST /api/comments/:commentId/toggle-like error:", e);
    res.status(500).json({ error: "Failed to toggle like" });
  }
});

// =========================
// VIEWS (once per account per video)
// =========================
app.post("/api/videos/:id/view", requireAuth, async (req, res) => {
  const videoId = String(req.params.id);
  const userId = Number(req.user.id);

  try {
    const result = await pool.query(
      `
      WITH ins AS (
        INSERT INTO video_views (video_id, user_id)
        VALUES ($1::text, $2::bigint)
        ON CONFLICT (video_id, user_id) DO NOTHING
        RETURNING 1
      ),
      upd AS (
        UPDATE videos
        SET views = views + (SELECT COUNT(*) FROM ins)
        WHERE id = $1::text
        RETURNING views
      )
      SELECT
        (SELECT views FROM upd) AS views,
        (SELECT COUNT(*) FROM ins)::int AS added;
      `,
      [videoId, userId]
    );

    if (!result.rows.length || result.rows[0].views == null) {
      return res.status(404).json({ error: "Video not found" });
    }

    res.json({
      ok: true,
      videoId,
      views: Number(result.rows[0].views),
      added: Number(result.rows[0].added || 0),
    });
  } catch (e) {
    console.error("POST /api/videos/:id/view error:", e);
    res.status(500).json({ error: "Failed to record view" });
  }
});

// =========================
// RATINGS: my-rating endpoint (you said we can fix ratings later,
// but this removes the 404 noise in the console)
// =========================
// -------------------------
// RATINGS API
// -------------------------

// Get my rating for a video
app.get("/api/videos/:id/my-rating", requireAuth, async (req, res) => {
  const videoId = String(req.params.id);
  const userId = String(req.user.id);

  try {
    const { rows } = await pool.query(
      `SELECT rating
       FROM video_ratings
       WHERE video_id = $1 AND user_id = $2`,
      [videoId, userId]
    );

    res.json({ rating: rows[0]?.rating ?? null });
  } catch (err) {
    console.error("my-rating error:", err);
    res.status(500).json({ error: "Failed to fetch my rating" });
  }
});



// Rate a video (1..5)
app.post("/api/videos/:id/rate", requireAuth, async (req, res) => {
  const videoId = String(req.params.id);
  const userId = String(req.user.id);
  const rating = Number(req.body.rating);

  if (!Number.isInteger(rating) || rating < 1 || rating > 5) {
    return res.status(400).json({ error: "Rating must be 1-5" });
  }

  try {
    await pool.query(
      `
      INSERT INTO video_ratings (video_id, user_id, rating)
      VALUES ($1, $2, $3)
      ON CONFLICT (video_id, user_id)
      DO UPDATE SET rating = EXCLUDED.rating, updated_at = now()
      `,
      [videoId, userId, rating]
    );

    // recompute avg + count
    const agg = await pool.query(
      `SELECT AVG(rating)::float AS avg, COUNT(*)::int AS count
       FROM video_ratings
       WHERE video_id = $1`,
      [videoId]
    );

    const ratingAvg = agg.rows[0]?.avg ?? 0;
    const ratingCount = agg.rows[0]?.count ?? 0;

    // (optional) store on videos table if you keep denormalized fields
    await pool.query(
      `UPDATE videos SET rating_avg = $2, rating_count = $3 WHERE id = $1`,
      [videoId, ratingAvg, ratingCount]
    ).catch(() => {});

    res.json({ ratingAvg, ratingCount });
  } catch (err) {
    console.error("rate error:", err);
    res.status(500).json({ error: "Failed to rate video" });
  }
});






// =====================
// PROFILE UPLOADS (sorted)
// =====================
app.get("/api/profile/u/:username/videos", async (req, res) => {
  const username = String(req.params.username || "").trim();
  if (!username) return res.status(400).json({ error: "Missing username" });

  const includePrivate =
    req.user && req.user.username?.toLowerCase() === username.toLowerCase();

  const sort = String(req.query.sort || "newest").toLowerCase();

  const ORDER_BY = {
    newest: "v.created_at DESC",
    oldest: "v.created_at ASC",
    views: "v.views DESC, v.created_at DESC",
    rating:
      "COALESCE(vrs.rating_avg, 0) DESC, COALESCE(vrs.rating_count, 0) DESC, v.created_at DESC",
  };

  const orderBy = ORDER_BY[sort] || ORDER_BY.newest;

  try {
    const result = await pool.query(
      `
      SELECT
        v.id,
        v.user_id,
        v.title,
        v.description,
        v.category,
        v.visibility,
        v.filename,
        v.thumb,
        v.duration,
        v.duration_text,
        v.views,
        v.tags,
        v.created_at,

        u.username AS channel_username,
        COALESCE(p.display_name, u.username) AS channel_display_name,

        COALESCE(vrs.rating_avg, 0) AS rating_avg,
        COALESCE(vrs.rating_count, 0) AS rating_count

      FROM videos v
      JOIN users u ON u.id = v.user_id
      LEFT JOIN user_profiles p ON p.user_id = u.id
      LEFT JOIN video_rating_stats vrs ON vrs.video_id = v.id

      WHERE lower(u.username) = lower($1)
        AND (
          v.visibility = 'public'
          OR $2::boolean = true
        )

      ORDER BY ${orderBy}
      LIMIT 200
      `,
      [username, includePrivate]
    );

    // Return same shape as /api/videos items (client expects thumbUrl/playbackUrl)
    const enriched = await Promise.all(result.rows.map((v) => toApiVideo(req, v)));
    return res.json(enriched);
  } catch (e) {
    console.error("Profile videos error:", e);
    return res.status(500).json({ error: "Failed to load uploads" });
  }
});



// -------------------------
// VIDEOS API
// -------------------------
app.get("/api/videos", async (req, res) => {
  try {
    const rows = await fetchVideosFromDb();
    const enriched = await Promise.all(rows.map((v) => toApiVideo(req, v)));
    res.json(enriched);
  } catch (e) {
    console.error("GET /api/videos error:", e);
    res.status(500).json({ error: "Failed to load videos" });
  }
});

app.get("/api/videos/:id", async (req, res) => {
  try {
    const v = await fetchVideoById(req.params.id);
    if (!v) return res.status(404).json({ error: "Not found" });
    res.json(await toApiVideo(req, v));
  } catch (e) {
    console.error("GET /api/videos/:id error:", e);
    res.status(500).json({ error: "Failed to load video" });
  }
});

app.get("/api/categories", async (_req, res) => {
  try {
    const result = await pool.query(
      `
      SELECT DISTINCT category
      FROM videos
      WHERE visibility = 'public'
      ORDER BY category ASC
      `
    );
    res.json(result.rows.map((r) => r.category));
  } catch (e) {
    console.error("GET /api/categories error:", e);
    res.status(500).json({ error: "Failed to load categories" });
  }
});

// -------------------------
// UPLOAD VIDEO
// - local: keep mp4 on disk
// - aws: convert to HLS, upload folder to S3 uploads bucket, store master key in DB
// -------------------------
app.post("/api/videos/upload", requireAuth, upload.single("video"), async (req, res) => {
  try {
    const userId = req.user.id;

    const title = String(req.body?.title || "").trim();
    const description = String(req.body?.description || "").trim();
    const visibility = String(req.body?.visibility || "public").toLowerCase();
    const tagsRaw = String(req.body?.tags || "");

    if (!title) return res.status(400).json({ error: "Title is required" });
    if (!req.file) return res.status(400).json({ error: "MP4 file is required" });

    const allowedVis = new Set(["public", "private", "unlisted"]);
    if (!allowedVis.has(visibility)) {
      return res.status(400).json({ error: "Visibility must be public, private, or unlisted" });
    }

    const tags = Array.from(
      new Set(
        tagsRaw
          .split(",")
          .map((t) => t.trim().toLowerCase())
          .filter(Boolean)
          .slice(0, 30)
      )
    );

    const category = "Other";

    // Thumb generation (local)
    const base = path.parse(req.file.filename).name;
    const thumbName = `${base}.jpg`;
    const thumbPath = path.join(THUMB_DIR, thumbName);

    let storedThumb = "placeholder.jpg";
    try {
      await generateThumbnailHalfwayWithFallback(req.file.path, thumbPath);
      storedThumb = thumbName;
    } catch (e) {
      console.warn("Thumb gen failed, using placeholder:", e.message);
      try {
        if (fs.existsSync(thumbPath)) fs.unlinkSync(thumbPath);
      } catch {}
    }

    let storedFilename = req.file.filename;

    if (VIDEO_SOURCE === "aws") {
      const token = crypto.randomBytes(8).toString("hex");
      const hlsPrefix = `hls/${userId}/${token}`;
      const hlsOutDir = path.join(os.tmpdir(), `mytube-hls-${userId}-${token}`);

      // 1) Generate HLS locally
      await generateHlsVOD(req.file.path, hlsOutDir);

      // 2) Upload entire HLS folder to S3
      await uploadDirToS3({
        bucket: process.env.S3_UPLOADS_BUCKET,
        dirPath: hlsOutDir,
        keyPrefix: hlsPrefix,
      });

      // 3) Store master playlist key in DB
      storedFilename = `${hlsPrefix}/master.m3u8`;

      // 4) Optional: upload thumb to assets bucket (if you want thumbs via CDN_ASSETS)
      if (storedThumb !== "placeholder.jpg" && fs.existsSync(thumbPath)) {
        try {
          await uploadFileToS3({
            bucket: process.env.S3_ASSETS_BUCKET,
            key: storedThumb,
            filePath: thumbPath,
            contentType: "image/jpeg",
          });
        } catch (e) {
          console.warn("Thumb upload to S3 failed (will fall back to local thumb route):", e.message);
        }
      }

      // cleanup local temp
      try {
        if (fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path);
      } catch {}
      try {
        fs.rmSync(hlsOutDir, { recursive: true, force: true });
      } catch {}
    }

    const ins = await pool.query(
      `
      INSERT INTO videos (user_id, title, description, category, visibility, filename, thumb, duration_text, views, tags)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, $9)
      RETURNING id
      `,
      [userId, title, description, category, visibility, storedFilename, storedThumb, null, tags]
    );

    const insertedId = ins.rows[0].id;
    const v = await fetchVideoById(insertedId);
    return res.json({ ok: true, video: await toApiVideo(req, v) });
  } catch (e) {
    console.error("Upload error:", e);
    if (req.file?.path) {
      try {
        fs.unlinkSync(req.file.path);
      } catch {}
    }
    return res.status(500).json({ error: "Failed to upload video" });
  }
});

// -------------------------
// Static thumbs (local placeholder + local mode thumbs)
// -------------------------
app.use("/thumbs", express.static(THUMB_DIR));

// -------------------------
// LOCAL streaming endpoint (only used when VIDEO_SOURCE=local)
// -------------------------
app.get("/videos/:id/stream", async (req, res) => {
  if (VIDEO_SOURCE !== "local") {
    return res.status(404).json({ error: "Streaming endpoint not used in this mode" });
  }

  const videoId = req.params.id;
  const v = await fetchVideoById(videoId);
  if (!v) return res.status(404).end("Not found");

  const filePath = path.join(VIDEO_DIR, v.filename);
  if (!fs.existsSync(filePath)) return res.status(404).end("Missing file");

  const stat = fs.statSync(filePath);
  const fileSize = stat.size;
  const range = req.headers.range;

  const ext = path.extname(filePath).toLowerCase();
  const contentType = ext === ".mp4" ? "video/mp4" : "application/octet-stream";

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

// -------------------------
// DELETE VIDEO (local + aws)
// -------------------------
app.delete("/api/videos/:id", requireAuth, async (req, res) => {
  const videoId = String(req.params.id);
  const userId = req.user.id;

  try {
    const v = await fetchVideoById(videoId);
    if (!v) return res.status(404).json({ error: "Not found" });

    if (Number(v.user_id) !== Number(userId)) {
      return res.status(403).json({ error: "Not allowed" });
    }

    const storedFilename = v.filename; // local filename OR aws key (hls/.../master.m3u8)
    const storedThumb = v.thumb;

    await pool.query(`DELETE FROM videos WHERE id::text = $1::text`, [videoId]);

    if (VIDEO_SOURCE === "local") {
      const videoPath = path.join(VIDEO_DIR, storedFilename);
      try {
        if (storedFilename && fs.existsSync(videoPath)) fs.unlinkSync(videoPath);
      } catch {}

      if (storedThumb && storedThumb !== "placeholder.jpg") {
        const thumbPath = path.join(THUMB_DIR, storedThumb);
        try {
          if (fs.existsSync(thumbPath)) fs.unlinkSync(thumbPath);
        } catch {}
      }
    } else if (VIDEO_SOURCE === "aws") {
      // ✅ HLS is a folder/prefix, not a single file
      if (storedFilename && storedFilename.includes("/master.m3u8")) {
        const prefix = storedFilename.replace(/\/master\.m3u8$/i, "");
        try {
          await deletePrefixFromS3({
            bucket: process.env.S3_UPLOADS_BUCKET,
            prefix,
          });
        } catch (e) {
          console.warn("S3 delete HLS prefix failed (ignored):", e?.message || e);
        }
      } else if (storedFilename) {
        // fallback (single-object delete)
        try {
          await deleteFromS3({ bucket: process.env.S3_UPLOADS_BUCKET, key: storedFilename });
        } catch (e) {
          console.warn("S3 delete video failed (ignored):", e?.message || e);
        }
      }

      if (storedThumb && storedThumb !== "placeholder.jpg") {
        try {
          await deleteFromS3({ bucket: process.env.S3_ASSETS_BUCKET, key: storedThumb });
        } catch (e) {
          console.warn("S3 delete thumb failed (ignored):", e?.message || e);
        }
      }
    }

    return res.json({ ok: true, deletedId: videoId });
  } catch (e) {
    console.error("DELETE /api/videos/:id error:", e);
    return res.status(500).json({ error: "Failed to delete video" });
  }
});

// Debug
app.get("/__whoami", (req, res) => {
  res.json({ ok: true, user: req.user ?? null, time: new Date().toISOString() });
});

app.get("/", (_req, res) => {
  res.send("MYTUBE server ✅ Try /api/videos");
});

const PORT = process.env.PORT || 3001;

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

