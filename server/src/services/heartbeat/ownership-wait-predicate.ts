import { and, eq, sql } from "drizzle-orm";
import { agentWakeupRequests } from "@paperclipai/db";

export const DEFERRED_WAKE_CONTEXT_KEY = "_paperclipWakeContext";
export const EXECUTION_OWNER_ACTIVE_CAUSE = "execution_owner_active";
export const INTERACTION_CONTINUATION_CONTEXT_KEYS = [
  "interactionId", "interactionKind", "interactionStatus", "continuationPolicy",
  "checkboxSelection", "itemVerdicts", "newlyResolvedItemIds",
] as const;

export function deferredExecutionOwnershipWaitPredicate() {
  const payload = agentWakeupRequests.payload;
  const context = sql`${payload}->${DEFERRED_WAKE_CONTEXT_KEY}`;
  return sql<boolean>`coalesce(${and(
    eq(agentWakeupRequests.status, "deferred_issue_execution"),
    sql`${payload}->'executionWait'->>'cause' = ${EXECUTION_OWNER_ACTIVE_CAUSE}`,
    sql`${payload}->'executionWait'->>'recoveryActionId' is null`,
    sql`${payload}->>'queuedCommentInterrupt' is null`,
    sql`${payload}->>'taskRetry' is null`,
    sql`nullif(btrim(${payload}->>'commentId'), '') is null`,
    sql`coalesce(nullif(${context}->'wakeCommentIds', 'null'::jsonb), '[]'::jsonb) = '[]'::jsonb`,
    sql`btrim(${payload}->>'mutation') is distinct from 'interaction'`,
    ...["wakeCommentId", "nativeStatusWakeIntentId", ...INTERACTION_CONTINUATION_CONTEXT_KEYS].map(
      (key) => sql`nullif(btrim(${context}->>${key}), '') is null`,
    ),
  )}, false)`;
}
