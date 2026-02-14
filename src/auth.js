import express from "express";
import bcrypt from "bcrypt";
import { v4 as uuid } from "uuid";
import { pool } from "./db.js";

const router = express.Router();

/**
 * Cookie config:
 * - In production (Render/Vercel): cross-site cookie needs SameSite=None + Secure
 * - In dev (localhost): SameSite=Lax and Secure=false is fine
 *
 * Make sure server.js has: app.set("trust proxy", 1)
 */
function cookieOptions() {
  const days = Number(process.env.SESSION_DAYS || 7);
  const isProd = process.env.NODE_ENV === "production";

  return {
    httpOnly: true,
    maxAge: days * 24 * 60 * 60 * 1000,
    path: "/",

    // cross-site cookie support (Vercel -> Render)
    sameSite: isProd ? "none" : "lax",
    secure: isProd, // required for SameSite=None
  };
}

/* helpers */
async function createSession(userId, res) {
  const sessionId = uuid();
  const days = Number(process.env.SESSION_DAYS || 7);

  await pool.query(
    `
    INSERT INTO sessions (id, user_id, expires_at)
    VALUES ($1, $2, now() + ($3 || ' days')::interval)
    `,
    [sessionId, userId, days]
  );

  res.cookie("session_id", sessionId, cookieOptions());
}

/* register */
router.post("/register", async (req, res) => {
  const { email, username, password } = req.body;

  if (!email || !username || !password) {
    return res.status(400).json({ error: "Missing fields" });
  }

  const passwordHash = await bcrypt.hash(password, 12);

  try {
    const result = await pool.query(
      `
      INSERT INTO users (email, username, password_hash)
      VALUES ($1, $2, $3)
      RETURNING id, username, tokens, rating, review_count
      `,
      [email, username, passwordHash]
    );

    const user = result.rows[0];
    await createSession(user.id, res);

    res.json({
      id: user.id,
      username: user.username,
      tokens: user.tokens,
      rating: user.rating,
      reviewCount: user.review_count,
    });
  } catch (err) {
    res.status(400).json({ error: "User already exists" });
  }
});

/* login */
router.post("/login", async (req, res) => {
  const { email, password } = req.body;

  const result = await pool.query(`SELECT * FROM users WHERE email = $1`, [email]);
  const user = result.rows[0];

  if (!user) return res.status(401).json({ error: "Invalid credentials" });

  const ok = await bcrypt.compare(password, user.password_hash);
  if (!ok) return res.status(401).json({ error: "Invalid credentials" });

  await createSession(user.id, res);

  res.json({
    id: user.id,
    username: user.username,
    tokens: user.tokens,
    rating: user.rating,
    reviewCount: user.review_count,
  });
});

/* logout */
router.post("/logout", async (req, res) => {
  const sid = req.cookies?.session_id;

  if (sid) {
    await pool.query(`DELETE FROM sessions WHERE id = $1`, [sid]);
  }

  // Must match cookie options to reliably clear it
  res.clearCookie("session_id", cookieOptions());
  res.json({ ok: true });
});

/* current user */
router.get("/me", async (req, res) => {
  const sid = req.cookies?.session_id;
  if (!sid) return res.status(401).json(null);

  const result = await pool.query(
    `
    SELECT u.id, u.username, u.tokens, u.rating, u.review_count
    FROM sessions s
    JOIN users u ON u.id = s.user_id
    WHERE s.id = $1 AND s.expires_at > now()
    `,
    [sid]
  );

  if (!result.rows[0]) return res.status(401).json(null);

  res.json({
    id: result.rows[0].id,
    username: result.rows[0].username,
    tokens: result.rows[0].tokens,
    rating: result.rows[0].rating,
    reviewCount: result.rows[0].review_count,
  });
});

export default router;
