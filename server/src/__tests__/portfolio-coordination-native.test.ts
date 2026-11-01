import { randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import express from "express";
import { eq } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { agents, companies, createDb, issues, projects } from "@paperclipai/db";
import { errorHandler } from "../middleware/index.js";
import { issueRoutes } from "../routes/issues.js";
import { getEmbeddedPostgresTestSupport, startEmbeddedPostgresTestDatabase } from "./helpers/embedded-postgres.js";

const wakeup = vi.hoisted(() => vi.fn(async (..._args: unknown[]) => null));
vi.mock("../services/index.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../services/index.js")>()),
  heartbeatService: () => ({ wakeup }),
}));

const execute = promisify(execFile);
const toolsDirectory = "/qualification/operations/tools";
const support = await getEmbeddedPostgresTestSupport();
const available = support.supported && existsSync(path.join(toolsDirectory, "portfolio_watchdog.py"));

const publisher = String.raw`
import copy, json, sys
from pathlib import Path
import portfolio_capacity as capacity
import portfolio_watchdog as monitor

fixture = json.loads(Path(sys.argv[1]).read_text())
root = Path(sys.argv[2])
root.mkdir(exist_ok=True)
capacity.BASE = monitor.BASE = sys.argv[3] + '/api'
capacity.COMPANY = monitor.COMPANY = fixture['companyId']
monitor.OPS = fixture['operationsProjectId']
monitor.ROOT = root
monitor.STATE = root / 'state.json'
monitor.SNAPSHOT = root / 'snapshot.json'
mode = sys.argv[4]
(root / 'capacity-plan.json').write_text(json.dumps({
    'watchdogPublishEnabled': True, 'goalContinuationPublishEnabled': True}))
calls = 0
def snapshot():
    global calls
    calls += 1
    data = copy.deepcopy(fixture['snapshot'])
    if mode == 'fresh-hold' and calls > 1:
        data['goalPlans'][0]['authorized'] = False
    return data
monitor.snapshot = snapshot
sys.argv = ['portfolio-watchdog', '--publish']
monitor.main()
`;

(available ? describe : describe.skip)("portfolio coordinator native publication", () => {
  let database: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;
  const servers: Server[] = [];
  const directories: string[] = [];

  beforeAll(async () => {
    database = await startEmbeddedPostgresTestDatabase("portfolio-coordinator-native-");
    db = createDb(database.connectionString);
  }, 90_000);
  beforeEach(() => wakeup.mockClear());
  afterEach(async () => {
    await Promise.all(servers.splice(0).map(server => new Promise<void>((resolve, reject) =>
      server.close(error => error ? reject(error) : resolve()))));
    await Promise.all(directories.splice(0).map(directory => rm(directory, { recursive: true, force: true })));
  });
  afterAll(async () => { await db?.$client.end({ timeout: 1 }); await database?.cleanup(); });

  async function fixture() {
    const companyId = randomUUID(), operationsProjectId = randomUUID();
    const projectIds = [randomUUID(), randomUUID()];
    const managerIds = [randomUUID(), randomUUID()];
    await db.insert(companies).values({ id: companyId, name: "Synthetic portfolio",
      issuePrefix: `P${companyId.slice(0, 6)}`, defaultResponsibleUserId: "board-user" });
    await db.insert(projects).values([operationsProjectId, ...projectIds].map((id, index) =>
      ({ id, companyId, name: `Synthetic project ${index}`, status: "in_progress" })));
    await db.insert(agents).values(managerIds.map((id, index) => ({ id, companyId,
      name: `Synthetic manager ${index}`, role: "engineer", adapterType: "process", status: "active" })));
    const stamp = new Date().toISOString();
    const snapshot = {
      checkedAt: stamp, alerts: [], recentRuns: [], coverage: {}, progressObservations: [],
      projects: projectIds.map(id => ({ id, openTasks: [], live: [] })),
      goalPlans: [] as Record<string, unknown>[], taskStates: [] as Record<string, unknown>[],
      acceptanceEvidence: {} as Record<string, unknown>, taskPreflights: {} as Record<string, unknown>,
    };
    for (const [index, projectId] of projectIds.entries()) {
      const controller = randomUUID(), source = randomUUID(), target = randomUUID(), goal = randomUUID();
      snapshot.goalPlans.push({ projectId, goalId: goal, controllerIssueId: controller,
        managerAgentId: managerIds[index], authorizationId: "synthetic-owner-mandate", revision: "v1",
        authorized: true, orderedTaskIds: [source, target], supersededControllerIssueIds: [] });
      snapshot.taskStates.push(
        { id: controller, projectId, goalId: goal, status: "blocked", assigneeAgentId: managerIds[index] },
        { id: source, projectId, goalId: goal, status: "done", assigneeAgentId: randomUUID() },
        { id: target, projectId, goalId: goal, status: "backlog", assigneeAgentId: randomUUID(), blockedByIssueIds: [source] },
      );
      snapshot.acceptanceEvidence[source] = { accepted: true, sourceIssueId: source,
        contractSha256: "a".repeat(64), reviewRunId: randomUUID(), checkedAt: stamp,
        sha256: { "synthetic-result.txt": "b".repeat(64) } };
      snapshot.taskPreflights[target] = { status: "ready", checkedAt: stamp,
        workspaceId: randomUUID(), sourceCommit: "c".repeat(40) };
    }
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.actor = { type: "board", userId: "board-user", companyIds: [companyId],
        isInstanceAdmin: true, source: "local_implicit" };
      next();
    });
    app.use("/api", issueRoutes(db, {} as any));
    app.use(errorHandler);
    const server = createServer(app);
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("Synthetic server did not bind loopback");
    const directory = await mkdtemp(path.join(tmpdir(), "portfolio-coordinator-native-"));
    directories.push(directory);
    const input = path.join(directory, "input.json"), state = path.join(directory, "state.json");
    await writeFile(input, JSON.stringify({ companyId, operationsProjectId, snapshot }));
    async function publish(mode = "ordinary") {
      const result = await execute("python3", ["-c", publisher, input, directory,
        `http://127.0.0.1:${address.port}`, mode], {
        env: { ...process.env, PYTHONPATH: toolsDirectory, PYTHONDONTWRITEBYTECODE: "1" },
        timeout: 30_000,
      });
      return { stdout: result.stdout, state: JSON.parse(await readFile(state, "utf8")) };
    }
    const created = () => db.select().from(issues).where(eq(issues.companyId, companyId));
    return { projectIds, managerIds, operationsProjectId, publish, state, created };
  }

  it("publishes one native coordinator per project across repeated polls and a lost local receipt", async () => {
    const f = await fixture();
    const first = await f.publish();
    const original = await f.created();
    expect(original).toHaveLength(2);
    expect(original.map(issue => issue.assigneeAgentId).sort()).toEqual([...f.managerIds].sort());
    expect(original.every(issue => issue.projectId === f.operationsProjectId && issue.status === "todo")).toBe(true);
    expect(Object.keys(first.state.goalReceipts)).toHaveLength(2);
    expect(wakeup).toHaveBeenCalledTimes(2);
    await f.publish();
    expect((await f.created()).map(issue => issue.id).sort()).toEqual(original.map(issue => issue.id).sort());
    expect(wakeup).toHaveBeenCalledTimes(2);
    await rm(f.state);
    const replay = await f.publish();
    expect((await f.created()).map(issue => issue.id).sort()).toEqual(original.map(issue => issue.id).sort());
    expect(wakeup).toHaveBeenCalledTimes(2);
    expect(Object.values(replay.state.goalReceipts).map((receipt: any) => receipt.receiptId).sort())
      .toEqual(original.map(issue => issue.id).sort());
  }, 90_000);

  it("rechecks scope before native publication and leaves the other project free to advance", async () => {
    const f = await fixture();
    const result = await f.publish("fresh-hold");
    const created = await f.created();
    expect(created).toHaveLength(1);
    expect(created[0].assigneeAgentId).toBe(f.managerIds[1]);
    expect(wakeup).toHaveBeenCalledTimes(1);
    expect(Object.values(result.state.goalReceipts).map((receipt: any) => receipt.projectId)).toEqual([f.projectIds[1]]);
  }, 90_000);
});
