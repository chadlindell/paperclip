import { randomUUID } from "node:crypto";
import { and, asc, eq, ne, sql } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import {
  agentWakeupRequests, agents, companies, createDb, environmentLeases, heartbeatRuns, issueComments, issues,
} from "@paperclipai/db";
import { getEmbeddedPostgresTestSupport, startEmbeddedPostgresTestDatabase } from "./helpers/embedded-postgres.js";
import { getExecutionBlocker } from "../services/execution-blocker.js";
import { heartbeatService } from "../services/heartbeat.js";
import { buildPaperclipWakePayload } from "../services/heartbeat/run-preparation.js";
import { createDeliveryWorkCoordinator } from "../services/delivery-work-coordinator.js";
import { idleWorkSnapshot } from "../services/task-admission.js";

const support = await getEmbeddedPostgresTestSupport();
(support.supported ? describe : describe.skip)("wakes parked behind an execution ownership hold", () => {
  let database: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;
  beforeAll(async () => {
    database = await startEmbeddedPostgresTestDatabase("execution-ownership-wait-");
    db = createDb(database.connectionString);
  }, 90_000);
  afterAll(async () => { await database?.cleanup(); });

  type Fixture = Awaited<ReturnType<typeof seed>>;

  async function seed(input: { stage?: boolean } = {}) {
    const stage = input.stage ?? true;
    const companyId = randomUUID(), previousAgentId = randomUUID(), nextAgentId = randomUUID();
    const issueId = randomUUID(), runId = randomUUID(), stageId = randomUUID();
    await db.insert(companies).values({ id: companyId, name: "Handoff", defaultResponsibleUserId: "board", issuePrefix: "H" + companyId.replaceAll("-", "") });
    await db.insert(agents).values([
      { id: previousAgentId, companyId, name: "Author", role: "engineer", adapterType: "opencode_local", status: "idle", runtimeConfig: { heartbeat: { maxConcurrentRuns: 1 } } },
      { id: nextAgentId, companyId, name: "Reviewer", role: "engineer", adapterType: "opencode_local", status: "idle", runtimeConfig: { heartbeat: { maxConcurrentRuns: 1 } } },
    ]);
    const participant = { type: "agent", agentId: nextAgentId, userId: null };
    const returnAssignee = { type: "agent", agentId: previousAgentId, userId: null };
    const reviewRequest = { instructions: "Review the change; do not implement it. Report a verdict." };
    const executionState = {
      status: "pending", currentStageId: stageId, currentStageIndex: 0, currentStageType: "review",
      currentParticipant: participant, returnAssignee, reviewRequest, completedStageIds: [],
      lastDecisionId: null, lastDecisionOutcome: null, changesRequestedCount: 0,
    };
    await db.insert(issues).values({
      id: issueId, companyId, title: "Review the change", assigneeAgentId: nextAgentId,
      ...(stage ? { status: "in_review", executionState } : { status: "in_progress" }),
    });
    const finishedAt = new Date(Date.now() - 500);
    await db.insert(heartbeatRuns).values({
      id: runId, companyId, agentId: previousAgentId, runtimeMode: "legacy", status: "cancelled",
      errorCode: "issue_reassigned", error: "Cancelled before issue reassignment",
      createdAt: new Date(Date.now() - 60_000), startedAt: new Date(Date.now() - 59_000), finishedAt,
      contextSnapshot: { issueId, taskId: issueId, wakeReason: "issue_assigned" },
      resultJson: {
        conversationContinuation: "continue_conversation_v1", reassignmentStopConfirmed: true,
        executionCancellation: { state: "acknowledged", acknowledgedAt: finishedAt.toISOString() },
        cancellation: { reason: "Cancelled before issue reassignment", source: "control_plane", expected: true },
      },
    });
    const [lease] = await db.insert(environmentLeases).values({
      companyId, issueId, heartbeatRunId: runId, status: "active", provider: "local",
    }).returning();
    const [busy] = await db.insert(heartbeatRuns).values({ companyId, agentId: nextAgentId, status: "running" }).returning();
    const executionStage = {
      stageId, wakeRole: "reviewer", stageType: "review", reviewRequest,
      allowedActions: ["approve", "request_changes"], returnAssignee, currentParticipant: participant, lastDecisionOutcome: null,
    };
    const wake = stage
      ? {
          source: "assignment" as const, triggerDetail: "system" as const, reason: "execution_review_requested",
          payload: { issueId, mutation: "update", executionStage, interruptedRunId: runId },
          requestedByActorType: "agent" as const, requestedByActorId: previousAgentId,
          contextSnapshot: { issueId, taskId: issueId, wakeReason: "execution_review_requested", source: "issue.execution_stage", executionStage, interruptedRunId: runId },
        }
      : {
          source: "assignment" as const, triggerDetail: "system" as const, reason: "issue_assigned",
          payload: { issueId, mutation: "update", interruptedRunId: runId },
          requestedByActorType: "user" as const, requestedByActorId: "board",
          contextSnapshot: { issueId, source: "issue.update", interruptedRunId: runId },
        };
    return { companyId, previousAgentId, nextAgentId, issueId, runId, stageId, lease: lease!, busy: busy!, wake };
  }

  const nextAgentWakes = (f: Fixture) => db.select().from(agentWakeupRequests).where(and(
    eq(agentWakeupRequests.companyId, f.companyId), eq(agentWakeupRequests.agentId, f.nextAgentId),
  )).orderBy(asc(agentWakeupRequests.requestedAt));
  const nextAgentIssueRuns = (f: Fixture) => db.select().from(heartbeatRuns).where(and(
    eq(heartbeatRuns.companyId, f.companyId), eq(heartbeatRuns.agentId, f.nextAgentId), ne(heartbeatRuns.id, f.busy.id),
  )).orderBy(asc(heartbeatRuns.createdAt));
  const releaseLease = (f: Fixture) => db.update(environmentLeases)
    .set({ status: "expired", releasedAt: new Date() }).where(eq(environmentLeases.id, f.lease.id));

  async function parkWake(f: Fixture) {
    const heartbeat = heartbeatService(db);
    expect(await getExecutionBlocker(db, f.companyId, f.issueId)).toMatchObject({ runId: f.runId, cause: "execution_owner_active", recoveryActionId: null });
    expect(await heartbeat.wakeup(f.nextAgentId, f.wake)).toBeNull();
    const [parked] = await nextAgentWakes(f);
    expect(parked).toMatchObject({
      status: "deferred_issue_execution", runId: null, finishedAt: null,
      payload: { issueId: f.issueId, executionWait: { cause: "execution_owner_active", runId: f.runId, recoveryActionId: null } },
    });
    expect(await nextAgentIssueRuns(f)).toHaveLength(0);
    return { heartbeat, parked: parked! };
  }

  async function parkComment(f: Fixture) {
    const [comment] = await db.insert(issueComments).values({
      companyId: f.companyId, issueId: f.issueId, authorType: "user", authorUserId: "board",
      body: "Preserve this input during the handoff.",
    }).returning();
    const heartbeat = heartbeatService(db);
    expect(await heartbeat.wakeup(f.nextAgentId, {
      source: "on_demand", triggerDetail: "manual", reason: "issue_commented",
      payload: { issueId: f.issueId, commentId: comment!.id },
      contextSnapshot: { issueId: f.issueId, wakeCommentId: comment!.id },
      requestedByActorType: "user", requestedByActorId: "board",
    })).toBeNull();
    const [parked] = await nextAgentWakes(f);
    expect(parked).toMatchObject({ status: "deferred_issue_execution", payload: {
      executionWait: { cause: "execution_owner_active" },
      _paperclipWakeContext: { wakeReason: "issue_commented", wakeCommentIds: [comment!.id] },
    } });
    return { heartbeat, comment: comment!, parked: parked! };
  }

  it("parks a stage handoff wake behind the outgoing lease and admits it once after the release", async () => {
    const f = await seed();
    const { heartbeat, parked } = await parkWake(f);

    await heartbeat.resumeExecutionOwnershipWaits({ companyId: f.companyId, issueId: f.issueId });
    expect(await nextAgentIssueRuns(f)).toHaveLength(0);
    expect((await nextAgentWakes(f))[0]!.status).toBe("deferred_issue_execution");

    await releaseLease(f);
    expect(await getExecutionBlocker(db, f.companyId, f.issueId)).toBeNull();
    await Promise.all([
      heartbeat.resumeExecutionOwnershipWaits({ companyId: f.companyId, issueId: f.issueId }),
      heartbeat.resumeExecutionOwnershipWaits({ companyId: f.companyId, issueId: f.issueId }),
    ]);
    const runs = await nextAgentIssueRuns(f);
    expect(runs).toHaveLength(1);
    expect(runs[0]).toMatchObject({ status: "queued", invocationSource: "assignment", triggerDetail: "system" });
    expect(runs[0]!.contextSnapshot).toMatchObject({
      issueId: f.issueId, wakeReason: "execution_review_requested", interruptedRunId: f.runId,
      executionStage: { stageId: f.stageId, wakeRole: "reviewer", currentParticipant: { agentId: f.nextAgentId } },
    });
    const wakes = await nextAgentWakes(f);
    expect(wakes.find(wake => wake.id === parked.id)).toMatchObject({ status: "coalesced", runId: runs[0]!.id });
    expect(wakes.find(wake => wake.runId === runs[0]!.id && wake.status === "queued")).toMatchObject({
      reason: "execution_review_requested", requestedByActorType: "agent", requestedByActorId: f.previousAgentId,
      idempotencyKey: `execution-wait-resume:${parked.id}`,
    });
    expect(wakes.filter(wake => wake.status === "skipped")).toHaveLength(0);

    await heartbeat.resumeExecutionOwnershipWaits({ companyId: f.companyId, issueId: f.issueId });
    await heartbeat.resumeQueuedRuns();
    expect(await nextAgentIssueRuns(f)).toHaveLength(1);
  });

  it("parks a plain assignment handoff the same way", async () => {
    const f = await seed({ stage: false });
    const { heartbeat, parked } = await parkWake(f);
    await releaseLease(f);
    await heartbeat.resumeExecutionOwnershipWaits({ companyId: f.companyId, issueId: f.issueId });
    const runs = await nextAgentIssueRuns(f);
    expect(runs).toHaveLength(1);
    expect(runs[0]!.contextSnapshot).toMatchObject({ issueId: f.issueId, wakeReason: "issue_assigned", interruptedRunId: f.runId });
    expect((await nextAgentWakes(f)).find(wake => wake.id === parked.id)).toMatchObject({ status: "coalesced", runId: runs[0]!.id });
  });

  it("resumes a changes-requested handback to the executor while the reviewer remains the current participant", async () => {
    const f = await seed();
    const [issue] = await db.select().from(issues).where(eq(issues.id, f.issueId));
    const currentParticipant = { type: "agent", agentId: f.previousAgentId, userId: null };
    const returnAssignee = { type: "agent", agentId: f.nextAgentId, userId: null };
    const executionStage = { ...f.wake.payload.executionStage!, wakeRole: "executor",
      currentParticipant, returnAssignee, allowedActions: ["address_changes", "resubmit"],
      lastDecisionOutcome: "changes_requested" };
    await db.update(issues).set({ status: "todo", executionState: {
      ...issue!.executionState as Record<string, unknown>, status: "changes_requested",
      currentParticipant, returnAssignee, lastDecisionOutcome: "changes_requested", changesRequestedCount: 1,
    } }).where(eq(issues.id, f.issueId));
    const heartbeat = heartbeatService(db);
    expect(await heartbeat.wakeup(f.nextAgentId, { ...f.wake, reason: "execution_changes_requested",
      payload: { ...f.wake.payload, executionStage },
      contextSnapshot: { ...f.wake.contextSnapshot, wakeReason: "execution_changes_requested", executionStage },
    })).toBeNull();
    const [parked] = await nextAgentWakes(f);
    expect(parked).toMatchObject({ status: "deferred_issue_execution", runId: null });
    await releaseLease(f);
    await heartbeat.resumeExecutionOwnershipWaits({ companyId: f.companyId, issueId: f.issueId });
    const runs = await nextAgentIssueRuns(f);
    expect(runs).toHaveLength(1);
    expect(runs[0]).toMatchObject({ status: "queued", agentId: f.nextAgentId,
      contextSnapshot: { wakeReason: "execution_changes_requested", executionStage } });
    const delivered = await buildPaperclipWakePayload({
      db, companyId: f.companyId, agentId: f.nextAgentId, contextSnapshot: runs[0]!.contextSnapshot!,
    });
    expect(delivered?.executionStage).toEqual(executionStage);
    expect((await nextAgentWakes(f)).find(wake => wake.id === parked!.id))
      .toMatchObject({ status: "coalesced", runId: runs[0]!.id });
  });

  it("is picked up by fresh recovery services when the lease is released elsewhere", async () => {
    const f = await seed();
    const { parked } = await parkWake(f);
    await releaseLease(f);
    await db.update(agentWakeupRequests).set({ updatedAt: new Date(Date.now() - 60_000) }).where(eq(agentWakeupRequests.id, parked.id));
    await Promise.all([heartbeatService(db).resumeExecutionOwnershipWaits(), heartbeatService(db).resumeExecutionOwnershipWaits()]);
    const runs = await nextAgentIssueRuns(f);
    expect(runs).toHaveLength(1);
    expect((await nextAgentWakes(f)).find(wake => wake.id === parked.id)).toMatchObject({ status: "coalesced", runId: runs[0]!.id });
  });

  it("merges a repeated signal into the parked receipt", async () => {
    const f = await seed();
    const { heartbeat, parked } = await parkWake(f);
    expect(await heartbeat.wakeup(f.nextAgentId, f.wake)).toBeNull();
    const wakes = await nextAgentWakes(f);
    expect(wakes).toHaveLength(1);
    expect(wakes[0]).toMatchObject({ id: parked.id, status: "deferred_issue_execution", coalescedCount: 1 });
    await releaseLease(f);
    await heartbeat.resumeExecutionOwnershipWaits({ companyId: f.companyId, issueId: f.issueId });
    expect(await nextAgentIssueRuns(f)).toHaveLength(1);
  });

  it.each(["eligibility", "admission"] as const)("preserves a newer stage merged before the %s lock", async phase => {
    const f = await seed();
    const { heartbeat, parked } = await parkWake(f);
    await releaseLease(f);
    const [issue] = await db.select().from(issues).where(eq(issues.id, f.issueId));
    const nextStage = { ...f.wake.payload.executionStage!, stageId: randomUUID(),
      reviewRequest: { instructions: "Review the revised change instead." } };
    const transaction = db.transaction.bind(db);
    let transactions = 0;
    let merged = false;
    const spy = vi.spyOn(db, "transaction").mockImplementation(async (callback, config) => {
      if (transactions++ === (phase === "eligibility" ? 0 : 1)) {
        spy.mockRestore();
        await transaction(async tx => {
          await tx.update(issues).set({ executionState: {
            ...issue!.executionState as Record<string, unknown>, currentStageId: nextStage.stageId,
            reviewRequest: nextStage.reviewRequest,
          } }).where(eq(issues.id, f.issueId));
          await tx.update(agentWakeupRequests).set({ payload: {
            ...parked.payload, executionStage: nextStage,
            _paperclipWakeContext: { ...parked.payload!._paperclipWakeContext as Record<string, unknown>, executionStage: nextStage },
          }, coalescedCount: 1, updatedAt: new Date() }).where(eq(agentWakeupRequests.id, parked.id));
        });
        merged = true;
      }
      return transaction(callback, config);
    });
    try {
      await heartbeat.resumeExecutionOwnershipWaits({ companyId: f.companyId, issueId: f.issueId });
    } finally { spy.mockRestore(); }
    expect(merged).toBe(true);
    if (phase === "admission") {
      expect(await nextAgentIssueRuns(f)).toHaveLength(0);
      expect((await nextAgentWakes(f)).find(wake => wake.id === parked.id)?.status).toBe("deferred_issue_execution");
      await heartbeat.resumeExecutionOwnershipWaits({ companyId: f.companyId, issueId: f.issueId });
    }
    const runs = await nextAgentIssueRuns(f);
    expect(runs).toHaveLength(1);
    expect(runs[0]!.contextSnapshot?.executionStage).toEqual(nextStage);
    expect((await nextAgentWakes(f)).find(wake => wake.id === parked.id))
      .toMatchObject({ status: "coalesced", runId: runs[0]!.id });
  });

  it("keeps a handoff separate from the same user's saved comment until ordinary admission adopts both", async () => {
    const f = await seed({ stage: false });
    const { heartbeat, comment, parked } = await parkComment(f);
    await heartbeat.wakeup(f.nextAgentId, f.wake);
    await heartbeat.wakeup(f.nextAgentId, f.wake);
    const wakes = await nextAgentWakes(f);
    expect(wakes).toHaveLength(2);
    expect(wakes.find(wake => wake.id === parked.id)).toEqual(parked);
    const handoff = wakes.find(wake => wake.id !== parked.id)!;
    expect(handoff).toMatchObject({ reason: "issue_assigned", coalescedCount: 1 });

    await releaseLease(f);
    await heartbeat.resumeExecutionOwnershipWaits({ companyId: f.companyId, issueId: f.issueId });
    const runs = await nextAgentIssueRuns(f);
    expect(runs).toHaveLength(1);
    expect(runs[0]!.contextSnapshot).toMatchObject({
      wakeReason: "issue_assigned", wakeCommentIds: [comment.id],
      queuedCommentSourceWakeIds: expect.arrayContaining([parked.id, handoff.id]),
    });
    for (const id of [parked.id, handoff.id]) {
      expect((await nextAgentWakes(f)).find(wake => wake.id === id))
        .toMatchObject({ status: "coalesced", runId: runs[0]!.id });
    }
    await heartbeat.resumeQueuedRuns();
    expect(await nextAgentIssueRuns(f)).toHaveLength(1);
  });

  it("lets the interrupt sweep retry a saved comment after the ownership hold clears", async () => {
    const f = await seed({ stage: false });
    const { heartbeat, comment, parked } = await parkComment(f);
    const requestedAt = new Date(Date.now() - 60_000);
    await db.update(agentWakeupRequests).set({
      payload: { ...parked.payload, queuedCommentInterrupt: { actorId: "board", requestedAt: requestedAt.toISOString() } },
      updatedAt: requestedAt,
    }).where(eq(agentWakeupRequests.id, parked.id));
    await releaseLease(f);

    await heartbeat.resumeQueuedRuns();
    const runs = await nextAgentIssueRuns(f);
    expect(runs).toHaveLength(1);
    expect(runs[0]!.contextSnapshot).toMatchObject({ wakeReason: "issue_commented", wakeCommentIds: [comment.id] });
    const wakes = await nextAgentWakes(f);
    expect(wakes.find(wake => wake.id === parked.id)).toMatchObject({ status: "coalesced", runId: runs[0]!.id });
    expect(wakes.find(wake => wake.status === "queued"))
      .toMatchObject({ idempotencyKey: `queued-comment-interrupt:${parked.id}`, runId: runs[0]!.id });
    await heartbeat.resumeQueuedRuns();
    expect(await nextAgentIssueRuns(f)).toHaveLength(1);
  });

  it("keeps dedicated receipts out of the ownership sweep's batch and retry cadence", async () => {
    const f = await seed({ stage: false });
    const older = new Date(Date.now() - 120_000);
    const dedicatedPayloads = [
      { commentId: randomUUID() },
      { _paperclipWakeContext: { wakeCommentId: randomUUID() } },
      { _paperclipWakeContext: { wakeCommentIds: [randomUUID()] } },
      { queuedCommentInterrupt: { actorId: "board", requestedAt: older.toISOString() } },
      { taskRetry: { actorId: "board", generationId: randomUUID(), sourceRunId: f.runId } },
      { mutation: "interaction", _paperclipWakeContext: { interactionId: randomUUID() } },
      { _paperclipWakeContext: { continuationPolicy: "wake_assignee" } },
      { _paperclipWakeContext: { nativeStatusWakeIntentId: randomUUID() } },
    ];
    const dedicated = await db.insert(agentWakeupRequests).values(Array.from({ length: 50 }, (_, index) => ({
      companyId: f.companyId, agentId: f.nextAgentId, source: "automation", reason: "saved_input",
      status: "deferred_issue_execution", requestedByActorType: "user", requestedByActorId: "board",
      requestedAt: older, updatedAt: older,
      payload: { issueId: f.issueId,
        executionWait: { cause: "execution_owner_active", recoveryActionId: null, runId: f.runId },
        ...dedicatedPayloads[index % dedicatedPayloads.length],
      },
    }))).returning();
    const heartbeat = heartbeatService(db);
    await heartbeat.wakeup(f.nextAgentId, f.wake);
    const wakes = await nextAgentWakes(f);
    expect(wakes).toHaveLength(51);
    const handoff = wakes.find(wake => !dedicated.some(other => other.id === wake.id))!;
    const due = new Date(Date.now() - 60_000);
    await db.update(agentWakeupRequests).set({ updatedAt: due }).where(eq(agentWakeupRequests.id, handoff.id));

    await heartbeat.resumeExecutionOwnershipWaits();
    const after = await nextAgentWakes(f);
    expect(after.find(wake => wake.id === handoff.id)!.updatedAt.getTime()).toBeGreaterThan(due.getTime());
    for (const receipt of dedicated) {
      expect(after.find(wake => wake.id === receipt.id)).toEqual(receipt);
    }
  });

  it("is consumed by a later wake of the same agent instead of resuming as a second run", async () => {
    const f = await seed();
    const { heartbeat, parked } = await parkWake(f);
    await releaseLease(f);
    const later = await heartbeat.wakeup(f.nextAgentId, {
      source: "on_demand", triggerDetail: "manual", reason: "issue_commented",
      payload: { issueId: f.issueId }, contextSnapshot: { issueId: f.issueId },
      requestedByActorType: "user", requestedByActorId: "board",
    });
    expect(later).toMatchObject({ status: "queued" });
    expect(later!.contextSnapshot?.executionStage).toEqual(f.wake.payload.executionStage);
    const delivered = await buildPaperclipWakePayload({
      db, companyId: f.companyId, agentId: f.nextAgentId, contextSnapshot: later!.contextSnapshot!,
    });
    expect(delivered?.executionStage).toEqual(f.wake.payload.executionStage);
    expect((await nextAgentWakes(f)).find(wake => wake.id === later!.wakeupRequestId)?.payload?.executionStage)
      .toEqual(f.wake.payload.executionStage);
    expect((await nextAgentWakes(f)).find(wake => wake.id === parked.id)).toMatchObject({ status: "coalesced", runId: later!.id });
    await heartbeat.resumeExecutionOwnershipWaits({ companyId: f.companyId, issueId: f.issueId });
    await heartbeat.resumeQueuedRuns();
    expect(await nextAgentIssueRuns(f)).toEqual([expect.objectContaining({ id: later!.id })]);
  });

  it.each(["stage", "participant", "completed"] as const)("does not adopt obsolete stage instructions (%s)", async change => {
    const f = await seed();
    const { heartbeat, parked } = await parkWake(f);
    const [issue] = await db.select().from(issues).where(eq(issues.id, f.issueId));
    await db.update(issues).set({ executionState: {
      ...issue!.executionState as Record<string, unknown>,
      ...(change === "stage" ? { currentStageId: randomUUID() }
        : change === "participant" ? { currentParticipant: { type: "agent", agentId: f.previousAgentId, userId: null } }
          : { status: "completed", currentStageId: null, currentParticipant: null }),
    } }).where(eq(issues.id, f.issueId));
    await releaseLease(f);
    const later = await heartbeat.wakeup(f.nextAgentId, {
      source: "on_demand", reason: "issue_commented", payload: { issueId: f.issueId },
      contextSnapshot: { issueId: f.issueId }, requestedByActorType: "user", requestedByActorId: "board",
    });
    expect(later).toMatchObject({ status: "queued" });
    expect(later!.contextSnapshot?.executionStage).toBeUndefined();
    expect((await nextAgentWakes(f)).find(wake => wake.id === parked.id)?.status).toBe("deferred_issue_execution");
    await heartbeat.resumeExecutionOwnershipWaits({ companyId: f.companyId, issueId: f.issueId });
    expect((await nextAgentWakes(f)).find(wake => wake.id === parked.id)?.status).toBe("cancelled");
  });

  it("retires the receipt when a later run of the agent already started on the task", async () => {
    const f = await seed();
    const { heartbeat, parked } = await parkWake(f);
    const [started] = await db.insert(heartbeatRuns).values({
      companyId: f.companyId, agentId: f.nextAgentId, runtimeMode: "legacy", status: "succeeded",
      startedAt: new Date(), finishedAt: new Date(), contextSnapshot: { issueId: f.issueId, wakeReason: "issue_commented" },
    }).returning();
    await releaseLease(f);
    await heartbeat.resumeExecutionOwnershipWaits({ companyId: f.companyId, issueId: f.issueId });
    expect((await nextAgentWakes(f)).find(wake => wake.id === parked.id)).toMatchObject({ status: "cancelled", runId: null });
    expect(await nextAgentIssueRuns(f)).toEqual([expect.objectContaining({ id: started!.id })]);
  });

  it("waits behind a queued run of the agent that has not started yet", async () => {
    const f = await seed();
    const { heartbeat, parked } = await parkWake(f);
    const [queued] = await db.insert(heartbeatRuns).values({
      companyId: f.companyId, agentId: f.nextAgentId, runtimeMode: "legacy", status: "queued",
      contextSnapshot: { issueId: f.issueId, wakeReason: "issue_commented" },
    }).returning();
    await releaseLease(f);
    await heartbeat.resumeExecutionOwnershipWaits({ companyId: f.companyId, issueId: f.issueId });
    expect((await nextAgentWakes(f)).find(wake => wake.id === parked.id)).toMatchObject({ status: "deferred_issue_execution" });
    expect(await nextAgentIssueRuns(f)).toEqual([expect.objectContaining({ id: queued!.id })]);
  });

  it.each(["participant", "stage", "closed"])("retires a stage receipt the task moved past (%s)", async change => {
    const f = await seed();
    const { heartbeat, parked } = await parkWake(f);
    const [issue] = await db.select().from(issues).where(eq(issues.id, f.issueId));
    const state = issue!.executionState as Record<string, unknown>;
    await db.update(issues).set(
      change === "closed" ? { status: "done" }
        : change === "stage" ? { executionState: { ...state, currentStageId: randomUUID() } }
          : { executionState: { ...state, currentParticipant: { type: "agent", agentId: f.previousAgentId, userId: null } } },
    ).where(eq(issues.id, f.issueId));
    await releaseLease(f);
    await heartbeat.resumeExecutionOwnershipWaits({ companyId: f.companyId, issueId: f.issueId });
    expect((await nextAgentWakes(f)).find(wake => wake.id === parked.id)).toMatchObject({ status: "cancelled" });
    expect(await nextAgentIssueRuns(f)).toHaveLength(0);
  });

  it("retires an assignment receipt once the task belongs to someone else", async () => {
    const f = await seed({ stage: false });
    const { heartbeat, parked } = await parkWake(f);
    await db.update(issues).set({ assigneeAgentId: f.previousAgentId }).where(eq(issues.id, f.issueId));
    await releaseLease(f);
    await heartbeat.resumeExecutionOwnershipWaits({ companyId: f.companyId, issueId: f.issueId });
    expect((await nextAgentWakes(f)).find(wake => wake.id === parked.id)).toMatchObject({ status: "cancelled" });
    expect(await nextAgentIssueRuns(f)).toHaveLength(0);
  });
  describe("shared ownership-wait worker", () => {
    const coordinators: ReturnType<typeof createDeliveryWorkCoordinator>[] = [];
    let calls: number;
    let observed: typeof db;
    let draining: boolean;
    const errors: unknown[] = [];
    beforeEach(async () => {
      await db.execute(sql`TRUNCATE TABLE companies CASCADE`);
      calls = 0; draining = false; errors.length = 0;
      observed = new Proxy(db, { get(target, key) {
        if (!["select", "update", "insert", "delete", "execute", "transaction"].includes(String(key))) return Reflect.get(target, key);
        return (...args: unknown[]) => { calls++; return Reflect.apply(Reflect.get(target, key), target, args); };
      } });
      vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"], shouldAdvanceTime: true });
    });
    afterEach(async () => {
      for (const c of coordinators.splice(0)) await c.stop();
      vi.useRealTimers();
    });
    async function start() {
      const heartbeat = heartbeatService(observed);
      const c = createDeliveryWorkCoordinator({ owner: db, canRun: () => !draining,
        canReconcile: () => true, onError: error => { errors.push(error); } });
      coordinators.push(c);
      await heartbeat.registerDeferredReceiptWork(c, () => !draining).ready;
      return c;
    }
    async function settle() {
      await vi.advanceTimersByTimeAsync(30_000);
      await vi.waitFor(() => expect(idleWorkSnapshot().active).toBe(0), { timeout: 5000 });
      expect(errors).toEqual([]);
    }
    async function quiet(c: Awaited<ReturnType<typeof start>>) {
      await settle();
      expect(c.nextWakeAt()).toBeNull();
      const before = calls;
      await vi.advanceTimersByTimeAsync(90_000);
      expect(calls).toBe(before);
    }
    it("does no recurring database work when empty or settled, and the first real producer wakes it", async () => {
      const c = await start();
      await quiet(c);
      const f = await seed();
      await parkWake(f);
      await settle();
      expect(c.nextWakeAt()).not.toBeNull();
      expect(await nextAgentIssueRuns(f)).toHaveLength(0);
      await releaseLease(f);
      await settle();
      expect(await nextAgentIssueRuns(f)).toHaveLength(1);
      await quiet(c);
      const restarted = await start();
      await quiet(restarted);
    });
    it("reconstructs pending receipts at startup and two workers admit only one successor", async () => {
      const f = await seed();
      const { parked } = await parkWake(f);
      await releaseLease(f);
      await db.update(agentWakeupRequests).set({ updatedAt: new Date(Date.now() - 60_000) })
        .where(eq(agentWakeupRequests.id, parked.id));
      const workers = await Promise.all([start(), start()]);
      expect(await nextAgentIssueRuns(f)).toHaveLength(1);
      for (const c of workers) await quiet(c);
    });
    it.each([false, true])("observes nested admission only after outer settlement (rollback=%s)", async rollback => {
      const c = await start();
      await quiet(c);
      const f = await seed();
      const before = calls;
      const write = db.transaction(async tx => {
        expect(await heartbeatService(tx as unknown as typeof db).wakeup(f.nextAgentId, f.wake)).toBeNull();
        await vi.advanceTimersByTimeAsync(1);
        expect(calls).toBe(before);
        if (rollback) throw new Error("rollback admission");
      });
      if (rollback) await expect(write).rejects.toThrow("rollback admission");
      else await write;
      await releaseLease(f);
      await settle();
      expect(await nextAgentIssueRuns(f)).toHaveLength(rollback ? 0 : 1);
      await quiet(c);
    });
    it("retains a committed wake during drain without database polling, then resumes once", async () => {
      const c = await start();
      await quiet(c);
      const f = await seed();
      draining = true;
      await parkWake(f);
      await releaseLease(f);
      const before = calls;
      await vi.advanceTimersByTimeAsync(60_000);
      expect(calls).toBe(before);
      draining = false;
      await settle();
      expect(await nextAgentIssueRuns(f)).toHaveLength(1);
      await quiet(c);
      await c.stop();
      const stoppedCalls = calls;
      await parkWake(await seed());
      await vi.advanceTimersByTimeAsync(60_000);
      expect(calls).toBe(stoppedCalls);
    });
  });
});

