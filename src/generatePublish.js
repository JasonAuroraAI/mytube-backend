// server/src/generatePublish.js
import fs from "fs";
import path from "path";
import os from "os";
import crypto from "crypto";
import { spawn } from "child_process";
import { pipeline } from "stream/promises";
import { GetObjectCommand } from "@aws-sdk/client-s3";
import { s3 } from "./aws/s3Client.js";

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

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function normTimeline(raw) {
  const arr = Array.isArray(raw) ? raw : [];
  return arr
    .map((c) => ({
      videoId: String(c.videoId ?? c.id ?? ""),
      start: Number(c.start ?? 0),
      in: Number(c.in ?? 0),
      out: Number(c.out ?? 0),
    }))
    .filter(
      (c) =>
        c.videoId &&
        Number.isFinite(c.start) &&
        Number.isFinite(c.in) &&
        Number.isFinite(c.out)
    )
    .filter((c) => c.out > c.in && c.start >= 0)
    .sort((a, b) => a.start - b.start);
}

// Option A: concat clips back-to-back (ignores gaps)
// Always produces BOTH video + audio for each clip.
// If an input has no audio, we synthesize silence for that clip duration.
function buildFilterForConcat(clips, audioFlags) {
  const parts = [];
  const inputs = []; // must be interleaved: [v0][a0][v1][a1]...

  for (let i = 0; i < clips.length; i++) {
    const c = clips[i];
    const dur = Math.max(0, c.out - c.in);

    // video
    parts.push(
      `[${i}:v]trim=start=${c.in}:duration=${dur},setpts=PTS-STARTPTS[v${i}]`
    );

    // audio (real or silence)
    if (audioFlags?.[i]) {
      parts.push(
        `[${i}:a]atrim=start=${c.in}:duration=${dur},asetpts=PTS-STARTPTS[a${i}]`
      );
    } else {
      parts.push(
        `aevalsrc=0:d=${dur}[a${i}]`
      );
    }

    // IMPORTANT: interleave in pairs
    inputs.push(`[v${i}]`, `[a${i}]`);
  }

  parts.push(`${inputs.join("")}concat=n=${clips.length}:v=1:a=1[vout][aout]`);
  return parts.join(";");
}



async function hasAudioStream(filePath) {
  try {
    const { out } = await runCmd("ffprobe", [
      "-v", "error",
      "-select_streams", "a:0",
      "-show_entries", "stream=codec_type",
      "-of", "default=noprint_wrappers=1:nokey=1",
      filePath,
    ]);
    return String(out || "").trim() === "audio";
  } catch {
    return false;
  }
}



async function downloadS3ToFile({ bucket, key, outPath }) {
  const resp = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  if (!resp?.Body) throw new Error(`S3 download failed for ${key}`);
  await pipeline(resp.Body, fs.createWriteStream(outPath));
  return outPath;
}

export function registerGeneratePublish(app, deps = {}) {
  const { pool, requireAuth, uploadFileToS3 } = deps;

  if (!pool) throw new Error("registerGeneratePublish: missing pool");
  if (!requireAuth) throw new Error("registerGeneratePublish: missing requireAuth");
  if (!uploadFileToS3) throw new Error("registerGeneratePublish: missing uploadFileToS3");

  app.post("/api/generate/publish", requireAuth, async (req, res) => {
    const userId = Number(req.user?.id);

    const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "mytube-export-"));
    const inputDir = path.join(tmpRoot, "inputs");
    const outDir = path.join(tmpRoot, "out");
    fs.mkdirSync(inputDir, { recursive: true });
    fs.mkdirSync(outDir, { recursive: true });

    const cleanup = () => {
      try {
        // rm recursive (Node 14+)
        fs.rmSync(tmpRoot, { recursive: true, force: true });
      } catch {
        // ignore
      }
    };

    try {
      const {
        title,
        description = "",
        tags = "",
        visibility = "public",
        timelineName = "Timeline",
        timeline,
      } = req.body || {};

      const clips = normTimeline(timeline);

      if (!String(title || "").trim()) {
        cleanup();
        return res.status(400).json({ error: "Title is required" });
      }
      if (!clips.length) {
        cleanup();
        return res.status(400).json({ error: "Timeline is empty" });
      }

      // AWS-only
      if (!process.env.S3_UPLOADS_BUCKET) {
        cleanup();
        return res.status(500).json({ error: "Missing env S3_UPLOADS_BUCKET" });
      }

      // Fetch filenames/keys for the clip IDs
      const ids = clips.map((c) => c.videoId);

      const q = await pool.query(
        `
        SELECT id, filename
        FROM videos
        WHERE id::text = ANY($1::text[])
        `,
        [ids]
      );

      const byId = new Map(q.rows.map((r) => [String(r.id), r]));
      for (const c of clips) {
        if (!byId.has(String(c.videoId))) {
          cleanup();
          return res
            .status(400)
            .json({ error: `Unknown clip videoId ${c.videoId}` });
        }
      }

      // Download each source MP4 locally for ffmpeg
      const inputPaths = [];
      for (let i = 0; i < clips.length; i++) {
        const c = clips[i];
        const row = byId.get(String(c.videoId));

        const key = String(row.filename || "");
        if (!key) {
          cleanup();
          return res.status(400).json({ error: `Missing filename for ${c.videoId}` });
        }

        const localName = `src-${i}-${c.videoId}.mp4`;
        const localPath = path.join(inputDir, localName);

        await downloadS3ToFile({
          bucket: process.env.S3_UPLOADS_BUCKET,
          key,
          outPath: localPath,
        });

        inputPaths.push(localPath);
      }

      // Output mp4
      const outName = `export-${Date.now()}-${crypto.randomBytes(6).toString("hex")}.mp4`;
      const outPath = path.join(outDir, outName);

      // ffmpeg concat
      const audioFlags = await Promise.all(inputPaths.map((p) => hasAudioStream(p)));
      const filter = buildFilterForConcat(clips, audioFlags);

      console.log("\n=== FFMPEG FILTER ===\n");
      console.log(filter);
      console.log("\n=====================\n");
      



      const args = [];
      for (const p of inputPaths) args.push("-i", p);

      args.push(
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-filter_complex",
        filter,
        "-map",
        "[vout]",
        "-map",
        "[aout]",
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
        "-movflags",
        "+faststart",
        outPath
      );

      await runCmd("ffmpeg", args);

      // Upload exported mp4
      const uploadKey = `uploads/${userId}/${outName}`;

      await uploadFileToS3({
        bucket: process.env.S3_UPLOADS_BUCKET,
        key: uploadKey,
        filePath: outPath,
        contentType: "video/mp4",
      });

      // DB insert
      const allowedVis = new Set(["public", "private", "unlisted"]);
      const vis = allowedVis.has(String(visibility).toLowerCase())
        ? String(visibility).toLowerCase()
        : "public";

      const tagsArr = Array.from(
        new Set(
          String(tags || "")
            .split(",")
            .map((t) => t.trim().toLowerCase())
            .filter(Boolean)
            .slice(0, 30)
        )
      );

      const ins = await pool.query(
        `
        INSERT INTO videos (user_id, title, description, category, visibility, filename, thumb, duration_text, views, tags)
        VALUES ($1, $2, $3, 'Other', $4, $5, 'placeholder.jpg', NULL, 0, $6)
        RETURNING id
        `,
        [
          userId,
          String(title).trim(),
          String(description || "").trim(),
          vis,
          uploadKey,
          tagsArr,
        ]
      );

      cleanup();

      return res.json({
        ok: true,
        videoId: ins.rows[0].id,
        timelineName,
      });
    } catch (e) {
      console.error("POST /api/generate/publish error:", e);
      cleanup();
      return res
        .status(500)
        .json({ error: e?.message || "Failed to publish generated video" });
    }
  });
}
