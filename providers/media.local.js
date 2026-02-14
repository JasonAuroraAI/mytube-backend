import path from "path";
import fs from "fs/promises";

const DATA_PATH = path.resolve("data/videos.json");

function baseUrl(req) {
  return `${req.protocol}://${req.get("host")}`;
}

export default {
  async listVideos(req, { q } = {}) {
    const raw = await fs.readFile(DATA_PATH, "utf-8");
    let videos = JSON.parse(raw);

    if (q && q.trim()) {
      const qq = q.toLowerCase();
      videos = videos.filter(v =>
        (v.title || "").toLowerCase().includes(qq) ||
        (v.category || "").toLowerCase().includes(qq)
      );
    }

    const b = baseUrl(req);
    return videos.map(v => ({
      id: v.id,
      title: v.title,
      category: v.category || "Other",
      visibility: v.visibility || "public",
      thumbUrl: `${b}/thumbs/${v.thumb}`,
      playbackUrl: `${b}/videos/${v.filename}`,
      channel: v.channel || "MyTube",
      views: v.views ?? null,
      durationText: v.durationText || null,
    }));
  }
};
