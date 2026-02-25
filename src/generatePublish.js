import fs from "fs";
import path from "path";
import os from "os";
import crypto from "crypto";
import { spawn } from "child_process";
import { pipeline } from "stream/promises";
import {
  GetObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { s3 } from "./aws/s3Client.js";

/* ============================================================
   GLOBAL ERROR TRAPS
============================================================ */
process.on("unhandledRejection", (err) => {
  console.error("🔥 UNHANDLED REJECTION:", err?.stack || err);
});

process.on("uncaughtException", (err) => {
  console.error("🔥 UNCAUGHT EXCEPTION:", err?.stack || err);
});

/* ============================================================
   UTIL: run external command with diagnostics
============================================================ */
function runCmd(cmd, args) {
  console.log(`▶️ Running: ${cmd} ${args.join(" ")}`);

  return new Promise((resolve, reject) => {
    const start = Date.now();
    const p = spawn(cmd, args, { windowsHide: true });

    let out = "";
    let err = "";

    p.stdout.on("data", (d) => (out += d.toString()));
    p.stderr.on("data", (d) => (err += d.toString()));

    p.on("error", (e) => {
      console.error(`❌ Spawn error for ${cmd}:`, e);
      reject(e);
    });

    p.on("close", (code, signal) => {
      const ms = Date.now() - start;
      console.log(`⏱ ${cmd} exited code=${code} signal=${signal} in ${ms}ms`);

      if (code === 0) {
        resolve({ out, err });
      } else {
        console.error(`❌ ${cmd} FAILED`);
        console.error("stderr (first 2000 chars):");
        console.error(err.slice(0, 2000));
        reject(new Error(err || `${cmd} exited with code ${code}`));
      }
    });
  });
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function normClips(raw, { kindDefault = null } = {}) {
  const arr = Array.isArray(raw) ? raw : [];
  return arr
    .map((c) => ({
      kind: String(c.kind ?? kindDefault ?? "").trim().toLowerCase() || null,
      track: Number.isFinite(Number(c.track)) ? Number(c.track) : 0,
      videoId: String(c.videoId ?? c.id ?? ""),
      start: Number(c.start ?? 0),
      in: Number(c.in ?? 0),
      out: Number(c.out ?? 0),
      gain: c.gain == null ? 1 : Number(c.gain),
    }))
    .filter(
      (c) =>
        c.videoId &&
        Number.isFinite(c.start) &&
        Number.isFinite(c.in) &&
        Number.isFinite(c.out) &&
        c.out > c.in &&
        c.start >= 0
    )
    .sort((a, b) => a.start - b.start);
}

function timelineDurationSeconds(videoClips) {
  let maxEnd = 0;
  for (const c of videoClips) {
    const end = c.start + (c.out - c.in);
    if (end > maxEnd) maxEnd = end;
  }
  return Math.max(0, maxEnd);
}

async function hasAudioStream(inputPath) {
  try {
    const { out } = await runCmd("ffprobe", [
      "-v",
      "error",
      "-select_streams",
      "a:0",
      "-show_entries",
      "stream=codec_type",
      "-of",
      "default=noprint_wrappers=1:nokey=1",
      inputPath,
    ]);
    return String(out || "").trim() === "audio";
  } catch {
    return false;
  }
}

async function extractThumbnail({ inputPath, outPath, atSeconds }) {
  await runCmd("ffmpeg", [
    "-y",
    "-ss",
    String(atSeconds),
    "-i",
    inputPath,
    "-frames:v",
    "1",
    "-vf",
    "scale=1280:-2",
    "-q:v",
    "2",
    outPath,
  ]);
}

/* ============================================================
   MAIN ROUTE
============================================================ */
export function registerGeneratePublish(app, deps = {}) {
  const { pool, requireAuth, uploadFileToS3 } = deps;

  app.post("/api/generate/publish", requireAuth, async (req, res) => {
    const requestStart = Date.now();

    console.log("====================================");
    console.log("🚀 PUBLISH START");
    console.log("Origin:", req.headers.origin);
    console.log("User:", req.user?.id);
    console.log("Body keys:", Object.keys(req.body || {}));
    console.log("====================================");

    try {
      const userId = Number(req.user?.id);
      const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "mytube-export-"));
      const inputDir = path.join(tmpRoot, "inputs");
      const outDir = path.join(tmpRoot, "out");
      fs.mkdirSync(inputDir, { recursive: true });
      fs.mkdirSync(outDir, { recursive: true });

      const cleanup = () => {
        try {
          fs.rmSync(tmpRoot, { recursive: true, force: true });
        } catch {}
      };

      try {
        const { title, timeline } = req.body || {};

        if (!String(title || "").trim()) {
          cleanup();
          return res.status(400).json({ error: "Title required" });
        }

        console.log("✅ Validation passed");

        if (!Array.isArray(timeline) || !timeline.length) {
          cleanup();
          return res.status(400).json({ error: "Timeline empty" });
        }

        const videoClips = normClips(timeline);
        const totalDur = timelineDurationSeconds(videoClips);

        console.log("📊 Clips normalized:", videoClips.length);

        /* ============================================================
           FFmpeg Test (minimal for diagnostics)
        ============================================================ */

        console.log("🎬 Testing ffmpeg pipeline...");

        await runCmd("ffmpeg", ["-version"]);

        console.log("✅ ffmpeg exists");

        console.log(
          `🎉 Publish handler completed in ${Date.now() - requestStart}ms`
        );

        cleanup();
        return res.json({ ok: true, debug: true });
      } catch (e) {
        console.error("💥 INNER PUBLISH ERROR:");
        console.error(e?.stack || e);
        cleanup();
        return res.status(500).json({
          error: e?.message || "Failed to publish generated video",
        });
      }
    } catch (e) {
      console.error("💥 OUTER PUBLISH ERROR:");
      console.error(e?.stack || e);
      return res.status(500).json({
        error: e?.message || "Publish failed",
      });
    }
  });
}