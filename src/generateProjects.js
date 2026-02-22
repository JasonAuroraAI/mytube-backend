export function registerGenerateProjects(app, deps = {}) {
  const { pool, requireAuth } = deps;

  if (!pool) throw new Error("Missing pool");
  if (!requireAuth) throw new Error("Missing requireAuth");

  // LIST
  app.get("/api/generate/projects", requireAuth, async (req, res) => {
    const userId = req.user.id;

    const r = await pool.query(
      `
      SELECT id, title, created_at, updated_at
      FROM projects
      WHERE user_id = $1
      ORDER BY updated_at DESC
      `,
      [userId]
    );

    res.json(r.rows);
  });

  // CREATE
  app.post("/api/generate/projects", requireAuth, async (req, res) => {
    const userId = req.user.id;
    const { title = "Untitled Project" } = req.body || {};

    const r = await pool.query(
      `
      INSERT INTO projects (user_id, title, timeline)
      VALUES ($1, $2, '[]')
      RETURNING id
      `,
      [userId, title]
    );

    res.json({ id: r.rows[0].id });
  });

  // LOAD
  app.get("/api/generate/projects/:id", requireAuth, async (req, res) => {
    const userId = req.user.id;
    const id = Number(req.params.id);

    const r = await pool.query(
      `
      SELECT id, title, timeline
      FROM projects
      WHERE id = $1 AND user_id = $2
      `,
      [id, userId]
    );

    if (!r.rows.length) return res.status(404).json({ error: "Not found" });

    res.json(r.rows[0]);
  });

  // SAVE
  app.patch("/api/generate/projects/:id", requireAuth, async (req, res) => {
    const userId = req.user.id;
    const id = Number(req.params.id);
    const { title, timeline } = req.body || {};

    await pool.query(
      `
      UPDATE projects
      SET title = $1,
          timeline = $2,
          updated_at = NOW()
      WHERE id = $3 AND user_id = $4
      `,
      [title, JSON.stringify(timeline || []), id, userId]
    );

    res.json({ ok: true });
  });
}