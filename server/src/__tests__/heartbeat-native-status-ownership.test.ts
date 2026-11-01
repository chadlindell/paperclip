import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { and, eq, ne, sql } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
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
    await Promise.all(Array.from({ length: 6 }, () => db.execute(sql`select pg_sleep(0.01)`)));
  }, 90_000);
  beforeEach(async () => { await db.execute(sql`truncate companies cascade`); });
  afterEach(async () => { for (const worker of workers.splice(0)) await worker.stop(); vi.useRealTimers(); vi.restoreAllMocks(); });
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
    return { companyId, agentId, issueId, runId, outgoing, lease, busy, assessmentId, decisionId: committed.decision.id, intentId: effect.targetId! };
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

  it("preserves a same-agent Stop even after a newer other-agent cancellation", async () => {
    const f = await fixture();
    const worker = await start();
    const [saved] = await dispatches(f);
    await worker.stop();
    const [stopped] = await db.insert(heartbeatRuns).values({ companyId: f.companyId, agentId: f.agentId,
      status: "cancelled", runtimeMode: "legacy", startedAt: new Date(), finishedAt: new Date(), contextSnapshot: { issueId: f.issueId },
      resultJson: { conversationContinuation: "continue_conversation_v1", executionCancellation: { state: "acknowledged" },
        cancellation: { source: "control_plane", reason: "Stopped by operator", expected: true } },
    }).returning();
    await db.update(environmentLeases).set({ status: "released", releasedAt: new Date(), cleanupStatus: "success" }).where(eq(environmentLeases.id, f.lease.id));
    await db.update(agentWakeupRequests).set({ updatedAt: new Date(Date.now() - 60_000) }).where(eq(agentWakeupRequests.id, saved.id));
    const afterStop = await start();
    expect((await successors(f)).filter(run => run.id !== stopped.id)).toHaveLength(0);
    await afterStop.stop();
    const [later] = await db.insert(heartbeatRuns).values({ companyId: f.companyId, agentId: f.outgoing.agentId,
      status: "cancelled", runtimeMode: "legacy", startedAt: new Date(), finishedAt: new Date(), contextSnapshot: { issueId: f.issueId },
      resultJson: { conversationContinuation: "continue_conversation_v1", executionCancellation: { state: "acknowledged" },
        cancellation: { source: "control_plane", reason: "Other owner stopped", expected: true } },
    }).returning();
    await db.update(agentWakeupRequests).set({ updatedAt: new Date(Date.now() - 60_000) }).where(eq(agentWakeupRequests.id, saved.id));
    const held = await start();
    expect((await successors(f)).filter(run => run.id !== stopped.id && run.id !== later.id)).toHaveLength(0);
    await held.stop();
    await db.update(issues).set({ status: "done" }).where(eq(issues.id, f.issueId));
    await db.update(agentWakeupRequests).set({ updatedAt: new Date(Date.now() - 60_000) }).where(eq(agentWakeupRequests.id, saved.id));
    const closed = await start();
    expect((await successors(f)).filter(run => run.id !== stopped.id && run.id !== later.id)).toHaveLength(0);
    expect(closed.nextWakeAt()).toBeNull();
  });

  it("preserves Stop when the native intent predates it but its dispatch receipt is admitted afterward", async () => {
    const f = await fixture();
    const [stopped] = await db.insert(heartbeatRuns).values({ companyId: f.companyId, agentId: f.agentId,
      status: "cancelled", runtimeMode: "legacy", startedAt: new Date(), finishedAt: new Date(), contextSnapshot: { issueId: f.issueId },
      resultJson: { conversationContinuation: "continue_conversation_v1", executionCancellation: { state: "acknowledged" } },
    }).returning();
    const [later] = await db.insert(heartbeatRuns).values({ companyId: f.companyId, agentId: f.outgoing.agentId,
      status: "cancelled", runtimeMode: "legacy", startedAt: new Date(), finishedAt: new Date(), contextSnapshot: { issueId: f.issueId },
      resultJson: { conversationContinuation: "continue_conversation_v1", executionCancellation: { state: "acknowledged" } },
    }).returning();
    const worker = await start();
    const [saved] = await dispatches(f);
    expect(saved).toMatchObject({ status: "deferred_issue_execution", runId: null });
    expect(saved.requestedAt.getTime()).toBeGreaterThan(stopped.finishedAt!.getTime());
    await worker.stop();
    await db.update(environmentLeases).set({ status: "released", releasedAt: new Date(), cleanupStatus: "success" }).where(eq(environmentLeases.id, f.lease.id));
    await db.update(agentWakeupRequests).set({ updatedAt: new Date(Date.now() - 60_000) }).where(eq(agentWakeupRequests.id, saved.id));
    await start();
    expect((await successors(f)).filter(run => run.id !== stopped.id && run.id !== later.id)).toHaveLength(0);
  });

  it("does not promote a former assignee's older native receipt ahead of the current assignee", async () => {
    const f = await fixture();
    const first = await start();
    await first.stop();
    const [current] = await db.insert(agents).values({ companyId: f.companyId, name: "Current owner", status: "idle", adapterType: "process",
      runtimeConfig: { heartbeat: { maxConcurrentRuns: 1 } } }).returning();
    const [occupied] = await db.insert(heartbeatRuns).values({ companyId: f.companyId, agentId: current.id, status: "running" }).returning();
    const [issue] = await db.update(issues).set({ assigneeAgentId: current.id }).where(eq(issues.id, f.issueId)).returning();
    const [assessment] = await db.select().from(workAssessments).where(eq(workAssessments.id, f.assessmentId));
    const [nextAssessment] = await db.insert(workAssessments).values({ ...assessment, id: randomUUID(), inputDigest: randomUUID(),
      supersedesAssessmentId: assessment.id, priorStatusVersion: issue.statusVersion, priorDecisionId: issue.lastStatusDecisionId }).returning();
    await commitNativeStatusDecision({ db, companyId: f.companyId, issueId: f.issueId, runId: f.runId,
      assessmentId: nextAssessment.id, priorStatus: issue.status, priorStatusVersion: issue.statusVersion,
      priorDecisionId: issue.lastStatusDecisionId, supersedesCommittedDecisionId: f.decisionId,
      decision: { policyVersion: NATIVE_STATUS_ARBITER_POLICY_VERSION, statusAction: "preserve", toStatus: "in_progress",
        reasonCode: "live_continuation_registered", unblockDescriptor: null,
        effects: [{ kind: "enqueue_continuation", continuationKind: "same_agent", agentId: current.id,
          summary: "Continue as current owner", idempotencyKey: `current-owner:${f.issueId}` }] } });
    const parked = await start();
    expect(await db.select().from(agentWakeupRequests).where(and(eq(agentWakeupRequests.companyId, f.companyId),
      eq(agentWakeupRequests.agentId, current.id), eq(agentWakeupRequests.status, "deferred_issue_execution")))).toHaveLength(1);
    await parked.stop();
    await db.update(environmentLeases).set({ status: "released", releasedAt: new Date(), cleanupStatus: "success" }).where(eq(environmentLeases.id, f.lease.id));
    await db.update(agentWakeupRequests).set({ updatedAt: new Date(Date.now() - 60_000) }).where(eq(agentWakeupRequests.companyId, f.companyId));
    await start();
    const admitted = (await successors(f)).filter(run => run.id !== occupied.id);
    expect(admitted.map(run => run.agentId)).toEqual([current.id]);
  });

  it("fires the retained deadline on a running worker and becomes quiet after delivery", async () => {
    const f = await fixture();
    const initial = await start();
    await initial.stop();
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"], shouldAdvanceTime: true });
    const worker = await start();
    expect(await successors(f)).toHaveLength(0);
    const next = worker.nextWakeAt();
    expect(next).not.toBeNull();
    await db.update(environmentLeases).set({ status: "released", releasedAt: new Date(), cleanupStatus: "success" }).where(eq(environmentLeases.id, f.lease.id));
    await vi.advanceTimersByTimeAsync(Math.max(1, next! - Date.now()));
    const until = performance.now() + 5000;
    while ((await successors(f)).length === 0 && performance.now() < until) await delay(20);
    expect(await successors(f)).toHaveLength(1);
    for (let pass = 0; worker.nextWakeAt() !== null && pass < 3; pass++) {
      await vi.advanceTimersByTimeAsync(Math.max(1, worker.nextWakeAt()! - Date.now()));
      await delay(50);
    }
    expect(worker.nextWakeAt()).toBeNull();
    const reads = vi.spyOn(db, "select");
    await vi.advanceTimersByTimeAsync(90_000);
    await delay(20);
    expect(reads).not.toHaveBeenCalled();
  });

  it.each(["done", "cancelled"])("retires the native continuation when the task becomes %s before lease release", async status => {
    const f = await fixture();
    const worker = await start();
    const [saved] = await dispatches(f);
    await worker.stop();
    await db.update(issues).set({ status, statusVersion: sql`${issues.statusVersion} + 1` }).where(eq(issues.id, f.issueId));
    await db.update(environmentLeases).set({ status: "released", releasedAt: new Date(), cleanupStatus: "success" }).where(eq(environmentLeases.id, f.lease.id));
    await db.update(agentWakeupRequests).set({ updatedAt: new Date(Date.now() - 60_000) }).where(eq(agentWakeupRequests.id, saved.id));
    const restarted = await start();
    expect(await successors(f)).toHaveLength(0);
    expect((await dispatches(f))[0]).toMatchObject({ id: saved.id, status: "cancelled", runId: null });
    expect(restarted.nextWakeAt()).toBeNull();
  });
});
