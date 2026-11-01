import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { expect, it, vi } from "vitest";
import { createRunnerdNativeSessionBackend } from "../backends/codex-native-backend.js";
import type { NativeExecutionInputV1 } from "../contracts/native-execution.js";
import type { PrpEvent } from "../protocol/replay-contract.js";
import { executeNativeSession } from "../native-session-runtime.js";
import { createRunnerdCodexTransport, defaultCapabilityRunnerdBinary } from "./runnerd-codex-transport.js";

it.each([false, true])("requires an accepted report across the real Rust late-goal boundary (reported=%s)", async reported => {
  const root = await mkdtemp(join(tmpdir(), "runner-late-goal-completion-"));
  const identity = { runId: "run-late-goal", sessionId: "session-late-goal", companyId: "company-late-goal", issueId: "issue-late-goal", agentId: "agent-late-goal" };
  const execution: NativeExecutionInputV1 = {
    schema: "paperclip.native-execution-input.v1",
    binding: { runId: identity.runId, companyId: identity.companyId, issueId: identity.issueId, agentId: identity.agentId, executionWorkspaceId: "workspace-late-goal" },
    session: { normalizedSessionId: identity.sessionId, driverKind: "codex_app_server", protocolVersion: 1 },
    task: { identifier: "TEST-1", title: "Finish checks", description: null, prompt: "Watch the already running checks and finish the request.", workMode: "standard" },
    workspace: { cwd: root, repoUrl: null, repoRef: null, branchName: null },
    provider: { kind: "codex", model: null },
    completionContract: { id: "contract-late-goal", sha256: "digest", schemaVersion: "paperclip.completion-contract.v1", contract: {
      revision: "1", objective: "Finish checks", criteria: [{ id: "checks", requirement: "Check the outcome" }],
    } },
    interactionResponses: [], credentialBindings: [],
  };
  const report = { schema: "paperclip.run_result.v1", reportedWorkDisposition: "done", summary: "Verified the synthetic task.",
    completionClaim: { contractRevision: "1", objectiveSatisfied: true,
      criteria: [{ criterionId: "checks", status: "satisfied", evidenceRefs: ["fixture:checks"] }], remainingWork: [] },
    evidence: [{ ref: "fixture:checks" }], verification: [], attentionRequests: [], artifacts: [],
  };
  const reportPath = join(root, "completion.json"), calls = join(root, "calls.log");
  await writeFile(reportPath, JSON.stringify(report));
  const bundle = createRunnerdCodexTransport({ runnerBinary: defaultCapabilityRunnerdBinary(),
    codexCommand: resolve(import.meta.dirname, "../../runner/target/debug/fake-codex-app-server"),
    codexArgs: ["--state-file", join(root, "provider.json"), "--call-log", calls, "--late-complete-goal",
      ...(reported ? ["--terminal-result-file", reportPath] : [])],
    stateDirectory: root,
    prpIdentity: { runnerInstanceId: "runner-late-goal", environmentLeaseId: "lease-late-goal", runId: identity.runId,
      normalizedSessionId: identity.sessionId, turnId: "durable-late-goal-turn", itemId: "late-goal-item" },
  });
  const events: PrpEvent[] = [], completeRun = vi.fn(async () => {});
  try {
    const run = executeNativeSession({ input: execution,
      backend: createRunnerdNativeSessionBackend(execution, { runnerInstanceId: "runner-late-goal", transportFactory: () => bundle.transport }),
      runnerInstanceId: "runner-late-goal", controlPlaneInstanceId: "controller-late-goal", timeoutMs: 10_000,
      controlPlane: { async openRun() {}, async checkpointSession() {},
        async appendEvent(event) { events.push(event); if (event.eventType === "turn.completed") await writeFile(join(root, "late-goal-turn-observed"), "observed"); return { cursor: events.length, highestContiguousSourceSeq: event.sourceSeq, disposition: "committed" }; },
        async replayEvents() { return { events: [], highestContiguousSourceSeq: 0 }; }, completeRun },
    });
    if (reported) {
      await expect(run).resolves.toMatchObject({ result: report });
      expect(completeRun).toHaveBeenCalledOnce();
      expect((await readFile(calls, "utf8")).match(/^terminal-result:accepted$/gm)).toHaveLength(1);
    } else {
      await expect(run).rejects.toMatchObject({ code: "native_provider_terminal_failed", providerCode: "semantic_completion_missing", recoverable: false });
      expect(completeRun).not.toHaveBeenCalled();
      expect(events.some(event => event.eventType === "run.result.accepted")).toBe(false);
    }
    const active = events.findIndex(event => event.eventType === "session.goal.updated" && (event.payload.goal as {status?: string})?.status === "active");
    const turn = events.findIndex(event => event.eventType === "turn.completed");
    const complete = events.findIndex(event => event.eventType === "session.goal.updated" && (event.payload.goal as {status?: string})?.status === "complete");
    expect(active).toBeGreaterThanOrEqual(0); expect(turn).toBeGreaterThan(active); expect(complete).toBeGreaterThan(turn);
  } finally {
    await bundle.transport.close().catch(() => undefined);
    const evidence = bundle.evidence();
    for (const pid of [evidence.runnerPid, evidence.providerPid]) if (pid) { try { process.kill(pid, "SIGKILL"); } catch { /* owned child already stopped */ } }
    await rm(root, { recursive: true, force: true });
  }
}, 25_000);
