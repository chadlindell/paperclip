import { agentWakeupRequests, heartbeatRuns, issues, statusDecisions, statusDecisionEffects, type Db } from "@paperclipai/db";
import { and, eq, inArray, isNull, ne, or, sql } from "drizzle-orm";
import { alias } from "drizzle-orm/pg-core";
import { boundRunIssueIdSql } from "../run-task-binding.js";
import { readChatControlNativeParent } from "../chat-control-recovery-stop.js";
import { getNativeReviewAssignment } from "./native-review-participant.js";
import { isCompletedOnboardingHandoffWake } from "../chat-completion-delivery.js";
import { isAcknowledgedNativeReassignmentStop, isAcknowledgedNativeStop } from "../acknowledged-native-stop.js";
import { readRunCancellation } from "../run-cancellation.js";

export async function deferredNativeWakeDisposition(db: Db,
  wake: Pick<typeof agentWakeupRequests.$inferSelect, "companyId" | "agentId" | "requestedByActorType" | "requestedByActorId" | "requestedAt" | "payload" | "reason">,
  issueId: string,
): Promise<"allow" | "preserve" | "cancel" | null> {
  const context = wake.payload?._paperclipWakeContext as Record<string, unknown> | undefined;
  const intentId = context?.nativeStatusWakeIntentId;
  if (typeof intentId !== "string" || !intentId.trim()) return null;
  if (wake.requestedByActorType !== "system" || wake.requestedByActorId !== `native-status-wake-dispatch:${intentId}`) return "cancel";
  const parent = await readChatControlNativeParent(db, { companyId: wake.companyId, issueId, agentId: wake.agentId }, wake.requestedByActorId);
  if (parent.kind === "unresolved") return "preserve";
  const [issue] = await db.select({ status: issues.status, assigneeAgentId: issues.assigneeAgentId }).from(issues)
    .where(and(eq(issues.companyId, wake.companyId), eq(issues.id, issueId))).limit(1);
  const reason = typeof context?.wakeReason === "string" ? context.wakeReason : wake.reason;
  const nativeReview = reason === "native_completion_review" ? await getNativeReviewAssignment(db, {
    companyId: wake.companyId, issueId, agentId: wake.agentId, contextSnapshot: context,
  }) : null;
  const onboardingReport = issue?.status === "done" && await isCompletedOnboardingHandoffWake(db, {
    companyId: wake.companyId, issueId, agentId: wake.agentId, reason, contextSnapshot: context,
  });
  if (!issue || (["done", "cancelled"].includes(issue.status) && !onboardingReport) ||
      (issue.assigneeAgentId !== wake.agentId && !nativeReview) || (reason === "native_completion_review" && !nativeReview)) return "cancel";
  const stopped = await db.select().from(heartbeatRuns).where(and(
    eq(heartbeatRuns.companyId, wake.companyId), eq(heartbeatRuns.agentId, wake.agentId), eq(heartbeatRuns.status, "cancelled"),
    eq(boundRunIssueIdSql(), issueId), sql`coalesce(${heartbeatRuns.finishedAt}, ${heartbeatRuns.updatedAt}) >= ${wake.requestedAt}`,
    or(sql`${heartbeatRuns.resultJson}->'executionCancellation'->>'state' = 'acknowledged'`,
      sql`${heartbeatRuns.resultJson}->'cancellation'->>'source' = 'operator'`,
      sql`${heartbeatRuns.resultJson}->'nativeCancellation'->>'dispatchState' = 'acknowledged'`),
  ));
  return stopped.some(run => {
    if (run.resultJson?.queuedCommentInterruptQueueId) return false;
    const cancellation = readRunCancellation(run.resultJson);
    const queuedStop = run.runtimeMode === "legacy" && run.runtimeModeResolvedAt === null && run.startedAt === null &&
      run.processStartedAt === null && run.processPid === null && run.processGroupId === null && cancellation?.source === "operator" && cancellation.expected;
    return queuedStop || (run.resultJson?.executionCancellation as Record<string, unknown> | undefined)?.state === "acknowledged" ||
      isAcknowledgedNativeStop(run) || isAcknowledgedNativeReassignmentStop(run);
  }) ? "preserve" : "allow";
}

/** Recover conversation provenance from a committed continuation, never from
 * provider-supplied source IDs or the mere presence of a chat on the task. */
export async function readNativeContinuationSource(
  db: Pick<Db, "select">,
  binding: { companyId: string; issueId: string; runId: string },
): Promise<string | null> {
  const source = alias(heartbeatRuns, "continuation_source");
  const dispatch = alias(agentWakeupRequests, "continuation_dispatch");
  const sources = await db.select({ runId: source.id }).from(heartbeatRuns)
    .innerJoin(agentWakeupRequests, and(
      eq(agentWakeupRequests.companyId, binding.companyId),
      or(isNull(agentWakeupRequests.runId), eq(agentWakeupRequests.runId, heartbeatRuns.id)),
      eq(agentWakeupRequests.agentId, heartbeatRuns.agentId),
      eq(agentWakeupRequests.reason, "issue_status_changed"),
      eq(agentWakeupRequests.status, "coalesced"),
      eq(agentWakeupRequests.requestedByActorType, "system"),
      eq(agentWakeupRequests.requestedByActorId, "native-status-committer"),
      sql`${agentWakeupRequests.payload}->>'issueId' = ${binding.issueId}`,
      sql`${heartbeatRuns.contextSnapshot}->>'nativeStatusWakeIntentId' = ${agentWakeupRequests.id}::text`,
    ))
    .innerJoin(dispatch, and(
      eq(dispatch.companyId, binding.companyId), eq(dispatch.agentId, heartbeatRuns.agentId),
      eq(dispatch.runId, heartbeatRuns.id), eq(dispatch.reason, "issue_status_changed"),
      eq(dispatch.requestedByActorType, "system"),
      sql`${dispatch.requestedByActorId} = 'native-status-wake-dispatch:' || ${agentWakeupRequests.id}::text`,
      eq(dispatch.idempotencyKey, agentWakeupRequests.idempotencyKey),
      inArray(dispatch.status, ["queued", "claimed", "completed", "coalesced"]),
      sql`${dispatch.payload}->>'issueId' = ${binding.issueId}`,
    ))
    .innerJoin(statusDecisionEffects, and(
      eq(statusDecisionEffects.companyId, binding.companyId), eq(statusDecisionEffects.issueId, binding.issueId),
      eq(statusDecisionEffects.effectKind, "enqueue_continuation"),
      eq(statusDecisionEffects.targetType, "agent_wakeup_request"),
      sql`${statusDecisionEffects.targetId} = ${agentWakeupRequests.id}::text`,
      eq(statusDecisionEffects.deliveryState, "delivered"),
      sql`${statusDecisionEffects.payload}->>'continuationKind' = 'same_agent'`,
      sql`${agentWakeupRequests.payload}->>'continuationKind' = 'same_agent'`,
    ))
    .innerJoin(statusDecisions, and(
      eq(statusDecisions.id, statusDecisionEffects.decisionId), eq(statusDecisions.companyId, binding.companyId),
      eq(statusDecisions.issueId, binding.issueId), eq(statusDecisions.applicationState, "applied"),
      eq(statusDecisions.reasonCode, "live_continuation_registered"),
      sql`${agentWakeupRequests.idempotencyKey} = 'native-status:' || ${statusDecisions.id}::text || ':continuation'`,
      sql`${agentWakeupRequests.payload}->>'nativeDecisionId' = ${statusDecisions.id}::text`,
    ))
    .innerJoin(source, and(
      eq(source.id, statusDecisions.runId), eq(source.companyId, binding.companyId),
      eq(source.nativeIssueId, binding.issueId), eq(source.runtimeMode, "native"),
      eq(source.agentId, heartbeatRuns.agentId), ne(source.id, heartbeatRuns.id),
      sql`${source.responsibleUserId} is not distinct from ${heartbeatRuns.responsibleUserId}`,
    ))
    .where(and(eq(heartbeatRuns.id, binding.runId), eq(heartbeatRuns.companyId, binding.companyId),
      eq(boundRunIssueIdSql(), binding.issueId)))
    .groupBy(source.id).limit(2);
  return sources.length === 1 ? sources[0]!.runId : null;
}
