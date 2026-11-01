import { randomUUID } from "node:crypto";
import { and, eq, ne } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  agents, agentWakeupRequests, companies, completionContracts, createDb, environmentLeases,
  heartbeatRuns, issues, nativeRunFinalizations, nativeRunResults, statusDecisionEffects, workAssessments,
} from "@paperclipai/db";
import { startEmbeddedPostgresTestDatabase } from "./helpers/embedded-postgres.js";
import { heartbeatService } from "../services/heartbeat.js";
import { createDeliveryWorkCoordinator } from "../services/delivery-work-coordinator.js";
import { getExecutionBlocker } from "../services/execution-blocker.js";
import { commitNativeStatusDecision } from "../services/native-runtime/status-decision-committer.js";
import { NATIVE_STATUS_ARBITER_POLICY_VERSION } from "../services/native-runtime/status-arbiter.js";

describe("native status continuation behind outgoing ownership", () => {
  let temporary: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;
  const workers: ReturnType<typeof createDeliveryWorkCoordinator>[] = [];
  beforeAll(async () => {
    temporary = await startEmbeddedPostgresTestDatabase("native-status-ownership-");
    db = createDb(temporary.connectionString, { maxConnections: 6, idleTimeoutSeconds: 0, maxLifetimeSeconds: 0 });
    db.$client.options.onnotice = () => {};
  }, 90_000);
  afterEach(async () => { for (const worker of workers.splice(0)) await worker.stop(); });
  afterAll(async () => { await db?.$client.end({ timeout: 1 }); await temporary?.cleanup(); });

  async function fixture() {
    const companyId = randomUUID(), agentId = randomUUID(), issueId = randomUUID(), runId = randomUUID();
    const contractId = randomUUID(), resultId = randomUUID(), assessmentId = randomUUID(), previousAgentId = randomUUID();
    await db.insert(companies).values({ id: companyId, name: "Native handoff", issuePrefix: `N${companyId.replaceAll("-", "")}`, defaultResponsibleUserId: "board" });
    await db.insert(agents).values({ id: agentId, companyId, name: "Worker", status: "idle", adapterType: "process",
      adapterConfig: { command: process.execPath, args: ["-e", "throw Error('provider must not launch')"] },
      runtimeConfig: { heartbeat: { maxConcurrentRuns: 1 } } });
    await db.insert(agents).values({ id: previousAgentId, companyId, name: "Outgoing owner", status: "idle", adapterType: "opencode_local" });
    await db.insert(issues).values({ id: issueId, companyId, title: "Native continuation", status: "in_progress", assigneeAgentId: agentId });
    await db.insert(heartbeatRuns).values({ id: runId, companyId, agentId, runtimeMode: "native", nativeIssueId: issueId,
      status: "succeeded", startedAt: new Date(Date.now() - 120_000), finishedAt: new Date(Date.now() - 60_000),
      contextSnapshot: { issueId }, completionContractId: contractId, completionContractSha256: `contract:${issueId}` });
    await db.insert(completionContracts).values({ id: contractId, companyId, issueId, revision: 1,
      schemaVersion: "paperclip.completion-contract.v1", policyVersion: "phase6-v1", risk: "standard",
      completionAuthority: "server_arbiter", incompleteCriteriaPolicy: "preserve_non_terminal",
      contractJson: { criteria: [{ id: "objective", requirement: "Continue the work" }] },
      canonicalSha256: `contract:${issueId}`, createdByActorType: "system", createdByActorId: "native-status-test" });
    await db.insert(nativeRunResults).values({ id: resultId, companyId, issueId, runId, completionContractId: contractId,
      serverFingerprint: `result:${issueId}`, schemaStatus: "accepted", resultJson: { result: { summary: "Continue the next step" } },
      canonicalSha256: `result:${issueId}` });
    await db.insert(workAssessments).values({ id: assessmentId, companyId, issueId, runId, contractId, resultId,
      triggerKind: "native_result", triggerActorCompanyId: companyId, priorIssueStatus: "in_progress", priorStatusVersion: 0,
      policyVersion: NATIVE_STATUS_ARBITER_POLICY_VERSION, assessmentJson: {}, inputDigest: `assessment:${issueId}` });
    await db.insert(nativeRunFinalizations).values({ runId, companyId, issueId, phase: "assessing", resultId, assessmentId });
    const committed = await commitNativeStatusDecision({ db, companyId, issueId, runId, assessmentId,
      priorStatus: "in_progress", priorStatusVersion: 0, priorDecisionId: null,
      decision: { policyVersion: NATIVE_STATUS_ARBITER_POLICY_VERSION, statusAction: "preserve", toStatus: "in_progress",
        reasonCode: "live_continuation_registered", unblockDescriptor: null,
        effects: [{ kind: "enqueue_continuation", continuationKind: "same_agent", summary: "Continue the next step",
          agentId, idempotencyKey: `continuation:${issueId}` }] } });
    const [effect] = await db.select().from(statusDecisionEffects).where(eq(statusDecisionEffects.decisionId, committed.decision.id));
    expect(effect).toMatchObject({ effectKind: "enqueue_continuation", deliveryState: "delivered" });
    const [outgoing] = await db.insert(heartbeatRuns).values({ companyId, agentId: previousAgentId, status: "cancelled", runtimeMode: "legacy",
      startedAt: new Date(Date.now() - 30_000), finishedAt: new Date(), errorCode: "issue_reassigned",
      contextSnapshot: { issueId }, resultJson: { conversationContinuation: "continue_conversation_v1",
        executionCancellation: { state: "acknowledged", acknowledgedAt: new Date().toISOString() },
        cancellation: { reason: "Handoff", source: "control_plane", expected: true } } }).returning();
    const [lease] = await db.insert(environmentLeases).values({ companyId, issueId, heartbeatRunId: outgoing.id,
      status: "active", provider: "local" }).returning();
    const [busy] = await db.insert(heartbeatRuns).values({ companyId, agentId, status: "running" }).returning();
    return { companyId, agentId, issueId, runId, outgoing, lease, busy, intentId: effect.targetId! };
  }

  async function start() {
    const errors: unknown[] = [];
    const worker = createDeliveryWorkCoordinator({ owner: db, canRun: () => true, onError: error => errors.push(error) });
    workers.push(worker);
    await heartbeatService(db).registerDeferredReceiptWork(worker, () => true).ready;
    expect(errors).toEqual([]);
    return worker;
  }
  const dispatches = (f: Awaited<ReturnType<typeof fixture>>) => db.select().from(agentWakeupRequests).where(and(
    eq(agentWakeupRequests.companyId, f.companyId), eq(agentWakeupRequests.requestedByActorId, `native-status-wake-dispatch:${f.intentId}`),
  ));
  const successors = (f: Awaited<ReturnType<typeof fixture>>) => db.select().from(heartbeatRuns).where(and(
    eq(heartbeatRuns.companyId, f.companyId), ne(heartbeatRuns.id, f.runId), ne(heartbeatRuns.id, f.outgoing.id), ne(heartbeatRuns.id, f.busy.id),
  ));

  it.each(["restart", "held", "concurrent", "paused"])("resumes the committed native decision receipt after out-of-band lease release without another wake (%s)", async mode => {
    const f = await fixture();
    expect(await getExecutionBlocker(db, f.companyId, f.issueId)).toMatchObject({ cause: "execution_owner_active" });
    const worker = await start();
    const [saved] = await dispatches(f);
    expect(saved).toMatchObject({ status: "deferred_issue_execution", runId: null,
      payload: { executionWait: { cause: "execution_owner_active" }, _paperclipWakeContext: { nativeStatusWakeIntentId: f.intentId } } });
    expect(await successors(f)).toHaveLength(0);
    await worker.stop();
    await db.update(agentWakeupRequests).set({ updatedAt: new Date(Date.now() - 60_000) }).where(eq(agentWakeupRequests.id, saved.id));
    if (mode === "held") {
      const held = await start();
      expect(await successors(f)).toHaveLength(0);
      expect(held.nextWakeAt()).not.toBeNull();
      expect((await dispatches(f))[0]).toMatchObject({ id: saved.id, status: "deferred_issue_execution", runId: null });
      await held.stop();
    }
    await db.update(environmentLeases).set({ status: "released", releasedAt: new Date(), cleanupStatus: "success" })
      .where(eq(environmentLeases.id, f.lease.id));
    await db.update(agentWakeupRequests).set({ updatedAt: new Date(Date.now() - 60_000) }).where(eq(agentWakeupRequests.id, saved.id));
    if (mode === "paused") {
      await db.update(agents).set({ status: "paused" }).where(eq(agents.id, f.agentId));
      const paused = await start();
      expect(await successors(f)).toHaveLength(0);
      expect((await dispatches(f))[0]).toMatchObject({ id: saved.id, status: "deferred_issue_execution", runId: null });
      await paused.stop();
      await db.update(agents).set({ status: "idle" }).where(eq(agents.id, f.agentId));
      await db.update(agentWakeupRequests).set({ updatedAt: new Date(Date.now() - 60_000) }).where(eq(agentWakeupRequests.id, saved.id));
    }
    if (mode === "concurrent") await Promise.all([start(), start()]);
    else await start();
    expect(await successors(f)).toHaveLength(1);
    expect(await dispatches(f)).toHaveLength(1);
    const [delivered] = await dispatches(f);
    expect(delivered).toMatchObject({ id: saved.id, status: "queued", runId: expect.any(String) });
    const [run] = await successors(f);
    expect(run).toMatchObject({ status: "queued", contextSnapshot: { nativeStatusWakeIntentId: f.intentId } });
    await heartbeatService(db).resumeQueuedRuns();
    expect(await successors(f)).toHaveLength(1);
  }, 30_000);
});
