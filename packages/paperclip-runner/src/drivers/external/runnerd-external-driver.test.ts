import { randomUUID } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { expect, it, vi } from "vitest";
import { RunnerdExternalDriver, DOT_EXTERNAL_CODEC } from "./runnerd-external-driver.js";
import { DurablePrpControlPlane } from "../../control-plane/durable-prp-control-plane.js";
import { parseNativeExecutionInput, type NativeExecutionInputV6 } from "../../contracts/native-execution.js";
import { NATIVE_RUNTIME_ASSET_SCHEMA, PAPERCLIP_EXECUTION_PROMPT, PAPERCLIP_EXECUTION_PROMPT_REVISION,
  nativeRuntimePromptDigest, canonicalNativeRuntimeContextDigest } from "../../contracts/runtime-context.js";

function execution(root: string, provider: "openai_dot" | "grok_bot" = "openai_dot"): NativeExecutionInputV6 {
  const companyId = randomUUID(), agentId = randomUUID();
  const digest = "0".repeat(64);
  const context = {
    prompt: { revision: PAPERCLIP_EXECUTION_PROMPT_REVISION, text: PAPERCLIP_EXECUTION_PROMPT, digest: nativeRuntimePromptDigest() },
    instructions: { entryPath: "AGENTS.md", bundle: { schema: NATIVE_RUNTIME_ASSET_SCHEMA, digest, manifestDigest: digest, rootPath: root, fileCount: 1, totalBytes: 4 } },
    skills: [], mcp: { assignmentSetId: "none", digest, bindingId: null },
  };
  return parseNativeExecutionInput({
    schema: "paperclip.native-execution-input.v6",
    binding: { companyId, agentId, runId: randomUUID(), issueId: randomUUID(), executionWorkspaceId: randomUUID() },
    task: { identifier: "DOT-1", title: "Synthetic bridge recovery", description: null, prompt: "Read the synthetic counter.", workMode: "standard" },
    provider: { kind: provider, model: null, binding: { companyId, agentId, bindingId: randomUUID(), bindingGeneration: 1,
      acceptByUnixMs: Date.now() + 600_000, expiresAtUnixMs: Date.now() + 7_200_000 } },
    workspace: { access: "none", cwd: null, repoUrl: null, repoRef: null, branchName: null },
    session: { normalizedSessionId: randomUUID(), driverKind: provider === "grok_bot" ? "grok_bot_mcp" : "openai_dot_mcp", protocolVersion: 1, lifecyclePolicy: { mode: "per_turn", idleTimeoutMs: null } },
    executionMode: "default", planningContext: null,
    completionContract: { id: randomUUID(), sha256: "sha256:" + digest, schemaVersion: "paperclip.completion-contract.v1",
      contract: { revision: "1", objective: "Read the counter", criteria: [{ id: "objective", requirement: "Read the counter" }] } },
    runtimeContext: { ...context, aggregateDigest: canonicalNativeRuntimeContextDigest(context) }, interactionResponses: [], credentialBindings: [],
  }) as NativeExecutionInputV6;
}

function deferred() {
  let release!: () => void;
  const promise = new Promise<void>(resolvePromise => { release = resolvePromise; });
  return { promise, release };
}

// Exercise the real session and real durable journal. Only the unrelated Runner
// startup command acknowledgments and the selected cleanup fault are simulated.
async function fixture(stage: "port" | "release" | "stop" | "drain" | "held_port" | "success") {
  const root = mkdtempSync(resolve(tmpdir(), "external-detach-fence-"));
  writeFileSync(resolve(root, "AGENTS.md"), "Synthetic instructions");
  const input = execution(root);
  const identity = { runnerInstanceId: randomUUID(), environmentLeaseId: randomUUID(), runId: input.binding.runId,
    normalizedSessionId: input.session.normalizedSessionId!, turnId: "turn-test", itemId: "item-test" };
  let core!: DurablePrpControlPlane;
  const handlerGate = deferred(), detachGate = deferred();
  const recover = vi.fn(async () => { await handlerGate.promise; return { result: { receipt: "existing-effect" } }; });
  const fail = async (point: string) => { if (stage === point) throw new Error(`synthetic ${point} failure`); };
  const driver = new RunnerdExternalDriver({
    execution: input, stateDirectory: root, identity,
    runnerArtifact: { version: "0.3.0", digest: `sha256:${"a".repeat(64)}` },
    adoptExistingRunner: { pid: 123, processGroupId: null, startedAt: new Date().toISOString(), isAlive: () => true },
    readProviderState: async () => null,
    recoverSemanticToolInput: recover,
    port: { dispatch: async () => {}, settle: async () => {}, attach: async () => async () => {
      if (stage === "held_port") await detachGate.promise;
      await fail("port");
    } },
    controlPlaneRegistration: async authority => {
      core = authority;
      const original = core.getCommand.bind(core);
      vi.spyOn(core, "getCommand").mockImplementation(id => {
        const command = original(id);
        return command && ["dot_prepare", "dot_open"].includes(id) ? { ...command, status: "completed" } : command;
      });
      return { connectUrl: "ws://127.0.0.1:1/synthetic-unused", release: () => fail("release") };
    },
  }, DOT_EXTERNAL_CODEC);
  const session = await driver.openSession({ runId: identity.runId, normalizedSessionId: identity.normalizedSessionId,
    workingDirectory: root });
  // Seed an already-admitted durable input: wire admission itself is covered by
  // durable-prp-control-plane.test.ts. This isolates external detach ownership.
  const correlation = { runId: identity.runId, normalizedSessionId: identity.normalizedSessionId,
    turnId: identity.turnId, itemId: identity.itemId };
  core.store.state.committedEvents.push({ sourceSeq: 1, sourceEventId: "pending-input", eventType: "semantic_tool.input",
    priority: 0, deliveryCount: 1, logicalEffectCount: 1,
    envelope: { payload: { ...correlation, sourceEventId: "pending-input", eventType: "semantic_tool.input",
      payload: { semantic_tool: { callId: "pending-call", operationId: "write_document", input: {}, correlation } } } } });
  core.store.state.ackedSourceSeq = 1;
  core.store.save();
  if (stage === "stop") vi.spyOn(core, "stop").mockRejectedValue(new Error("synthetic stop failure"));
  if (stage === "drain") vi.spyOn(core, "drainPendingConnectionProcessing").mockRejectedValue(new Error("synthetic drain failure"));
  return { root, session, core, recover, handlerGate, detachGate,
    journal: () => readFileSync(core.store.path, "utf8"),
    cleanup: async () => { handlerGate.release(); detachGate.release(); vi.restoreAllMocks();
      await core.stop(); await core.drainPendingConnectionProcessing(); rmSync(root, { recursive: true, force: true }); } };
}

it.each(["port", "release", "stop", "drain"] as const)(
  "failed external detach at %s fences late recovery receipts and keeps input pending", async stage => {
    const f = await fixture(stage);
    const recovering = f.core.recoverPendingSemanticInputs();
    try {
      expect(f.recover).toHaveBeenCalledOnce();
      const before = f.journal();
      await expect(f.session.detachControllerForRestart()).rejects.toThrow(`synthetic ${stage} failure`);
      f.handlerGate.release();
      await recovering;
      await f.core.recoverPendingSemanticInputs();
      expect(f.recover).toHaveBeenCalledOnce();
      expect(f.journal()).toBe(before);
      expect(f.core.store.state.commands.some(command => command.type === "semantic_tool.result")).toBe(false);
      expect(f.core.semanticToolResultsSettled()).toBe(false);
    } finally { f.handlerGate.release(); await recovering; await f.cleanup(); }
  });

it("fences external recovery synchronously while port detach is still pending", async () => {
  const f = await fixture("held_port");
  let detaching: Promise<void> | undefined;
  try {
    const before = f.journal();
    detaching = f.session.detachControllerForRestart();
    f.handlerGate.release();
    await f.core.recoverPendingSemanticInputs();
    expect(f.recover).not.toHaveBeenCalled();
    expect(f.journal()).toBe(before);
    expect(f.core.semanticToolResultsSettled()).toBe(false);
    f.detachGate.release(); await detaching;
  } finally { f.detachGate.release(); await detaching; await f.cleanup(); }
});

it("preserves normal external recovery before detach and successful detach cleanup", async () => {
  const f = await fixture("success");
  try {
    f.handlerGate.release();
    await f.core.recoverPendingSemanticInputs();
    expect(f.recover).toHaveBeenCalledOnce();
    expect(f.core.store.state.commands.filter(command => command.type === "semantic_tool.result")).toHaveLength(1);
    const before = f.journal();
    await f.session.detachControllerForRestart();
    expect(f.journal()).toBe(before);
  } finally { await f.cleanup(); }
});
