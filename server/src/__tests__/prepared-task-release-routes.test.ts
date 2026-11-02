import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { eq } from "drizzle-orm";
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { agentWakeupRequests, agents, companies, createDb, issues } from "@paperclipai/db";
import { errorHandler } from "../middleware/index.js";
import { issueRoutes } from "../routes/issues.js";
import { getEmbeddedPostgresTestSupport, startEmbeddedPostgresTestDatabase } from "./helpers/embedded-postgres.js";

const wakeup = vi.hoisted(() => vi.fn(async (..._args: unknown[]) => null));
vi.mock("../services/index.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../services/index.js")>()),
  heartbeatService: () => ({ wakeup }),
}));

const support = await getEmbeddedPostgresTestSupport();
(support.supported ? describe : describe.skip)("prepared task release route contract", () => {
  let temporary: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;

  beforeAll(async () => {
    temporary = await startEmbeddedPostgresTestDatabase("paperclip-prepared-task-release-");
    db = createDb(temporary.connectionString, { maxConnections: 4 });
  }, 20_000);
  beforeEach(() => wakeup.mockClear());
  afterAll(async () => { await temporary?.cleanup(); });

  async function fixture() {
    const companyId = randomUUID();
    await db.insert(companies).values({ id: companyId, name: "Prepared task release", issuePrefix: `P${companyId.slice(0, 6)}` });
    const [agent] = await db.insert(agents).values({ companyId, name: "Reviewer", role: "engineer", adapterType: "process", status: "active" }).returning();
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.actor = { type: "board", userId: "board-user", companyIds: [companyId], isInstanceAdmin: true, source: "local_implicit" };
      next();
    });
    app.use("/api", issueRoutes(db, {} as any));
    app.use(errorHandler);
    return { app, companyId, agent: agent! };
  }

  it("parks assigned work through preparation and wakes only after a guarded release commits", async () => {
    const { app, companyId, agent } = await fixture();
    const created = await request(app).post(`/api/companies/${companyId}/issues`).send({
      title: "Review prepared change", status: "backlog", assigneeAgentId: agent.id,
    });
    expect(created.status, JSON.stringify(created.body)).toBe(201);
    expect(created.body.status).toBe("backlog");
    expect(wakeup).not.toHaveBeenCalled();
    const retained = await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.companyId, companyId));
    expect(retained).toHaveLength(1);
    expect(retained[0]).toMatchObject({ reason: "issue_human_assignment", status: "completed", runId: null });

    const contract = "Review the supplied change. Acceptance: cite defects with source locations and record the verdict.";
    const prepared = await request(app).patch(`/api/issues/${created.body.id}`).send({ description: contract });
    expect(prepared.status, JSON.stringify(prepared.body)).toBe(200);
    expect(prepared.body).toMatchObject({ status: "backlog", description: contract });
    expect(wakeup).not.toHaveBeenCalled();

    const released = await request(app).patch(`/api/issues/${created.body.id}`).send({
      status: "todo", expectedStatusVersion: prepared.body.statusVersion,
    });
    expect(released.status, JSON.stringify(released.body)).toBe(200);
    expect(wakeup).toHaveBeenCalledTimes(1);
    expect(wakeup).toHaveBeenCalledWith(agent.id, expect.objectContaining({
      reason: "issue_status_changed", payload: expect.objectContaining({ issueId: created.body.id }),
    }));
    const [saved] = await db.select().from(issues).where(eq(issues.id, created.body.id));
    expect(saved).toMatchObject({ status: "todo", description: contract, assigneeAgentId: agent.id });

    const repeated = await request(app).patch(`/api/issues/${created.body.id}`).send({
      status: "todo", expectedStatusVersion: prepared.body.statusVersion,
    });
    expect(repeated.status, JSON.stringify(repeated.body)).toBe(409);
    expect(wakeup).toHaveBeenCalledTimes(1);
  });
});
