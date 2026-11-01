import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { expect, it, vi } from "vitest";
import type { DurablePrpControlPlane } from "../control-plane/durable-prp-control-plane.js";
import type { CodexRpcServerRequest } from "../drivers/codex/app-server-transport.js";
import { createCapabilityRunnerdCodexTransport, defaultCapabilityRunnerdBinary } from "./runnerd-codex-transport.js";

it.each(["install", "close", "detach", "detach-after-dispatch", "detach-stop-failure", "detach-drain-failure"] as const)("holds an authenticated semantic call until handler readiness or %s", async mode => {
  const root = await mkdtemp(join(tmpdir(), "runner-handler-readiness-"));
  let core!: DurablePrpControlPlane;
  const bundle = createCapabilityRunnerdCodexTransport({
    runnerBinary: defaultCapabilityRunnerdBinary(),
    codexCommand: resolve(import.meta.dirname, "../../runner/target/debug/fake-codex-app-server"),
    codexArgs: ["--state-file", join(root, "provider.json"), "--emit-tool-call"],
    stateDirectory: root,
    controlPlaneRegistration: async authority => {
      core = authority;
      await authority.start();
      return { release: () => undefined };
    },
  });
  const outcome = { success: true, contentItems: [{ type: "inputText", text: "one retained result" }] };
  let releaseOutcome!: () => void;
  const pendingOutcome = new Promise<void>(resolve => { releaseOutcome = resolve; });
  const handler = vi.fn(async (_request: CodexRpcServerRequest) => {
    if (mode === "detach-after-dispatch") await pendingOutcome;
    return outcome;
  });
  let pid: number | null = null;
  try {
    await bundle.transport.request("thread/start", { cwd: root, dynamicTools: [{ name: "get_task_context", description: "Read the task.", inputSchema: { type: "object", properties: {}, additionalProperties: false } }] });
    if (mode === "detach-after-dispatch") bundle.transport.setServerRequestHandler(handler);
    await bundle.transport.request("turn/start", { input: [{ type: "text", text: "Read the task." }] });
    pid = bundle.evidence().runnerPid;
    expect(bundle.evidence().runnerProcessGroupId).toBe(pid);
    await vi.waitFor(() => expect(core.store.state.committedEvents.some(event => event.eventType === "semantic_tool.input")).toBe(true));
    // The authenticated provider is already asking for a tool, but the driver
    // has not published its recovered session/handler yet. No error result may
    // be manufactured in this gap, and no external effect has been dispatched.
    await new Promise(resolveWait => setTimeout(resolveWait, 50));
    expect(core.store.state.commands.filter(command => command.type === "semantic_tool.result")).toHaveLength(0);
    expect(core.semanticToolResultsSettled()).toBe(false);
    if (mode === "close") await bundle.transport.close();
    if (mode === "detach-after-dispatch") await vi.waitFor(() => expect(handler).toHaveBeenCalledOnce());
    if (mode.startsWith("detach")) {
      const drain = core.drainPendingConnectionProcessing.bind(core);
      let releaseDrain!: () => void;
      const drainGate = new Promise<void>(resolve => { releaseDrain = resolve; });
      vi.spyOn(core, "drainPendingConnectionProcessing").mockImplementation(async () => { await drainGate; await drain(); });
      if (mode === "detach-stop-failure") vi.spyOn(core, "stop").mockRejectedValueOnce(new Error("injected stop failure"));
      if (mode === "detach-drain-failure") vi.spyOn(core, "drainPendingConnectionProcessing").mockRejectedValueOnce(new Error("injected drain failure"));
      const commandCount = core.store.state.commands.length;
      const detached = bundle.detachControllerForRestart();
      void detached.catch(() => undefined);
      try {
        await expect(bundle.transport.request("turn/start", { input: [{ type: "text", text: "Do not admit replacement work." }] })).rejects.toThrow("transport is closed");
        await expect(bundle.transport.request("thread/start", { cwd: root })).rejects.toThrow("transport is closed");
        if (mode === "detach") {
          bundle.transport.setServerRequestHandler(handler);
          await new Promise(resolveWait => setTimeout(resolveWait, 50));
          expect(handler).not.toHaveBeenCalled();
        }
        if (mode === "detach-after-dispatch") {
          releaseOutcome();
          await new Promise(resolveWait => setTimeout(resolveWait, 50));
        }
        expect(core.store.state.commands).toHaveLength(commandCount);
      } finally { releaseDrain(); }
      if (mode.endsWith("failure")) await expect(detached).rejects.toThrow("injected");
      else await detached;
    }
    const retiredJournal = mode.startsWith("detach") ? await readFile(join(root, "control-plane/control-plane-state.json"), "utf8") : null;
    releaseOutcome();
    bundle.transport.setServerRequestHandler(handler);
    if (mode === "install") {
      await vi.waitFor(() => expect(handler).toHaveBeenCalledOnce());
      await vi.waitFor(() => expect(core.store.state.commands.filter(command => command.type === "semantic_tool.result")).toHaveLength(1));
      expect(core.store.state.commands.find(command => command.type === "semantic_tool.result")?.payload).toMatchObject({ isError: false });
      expect(handler.mock.calls[0]?.[0]).toMatchObject({ method: "item/tool/call", params: { tool: "get_task_context" } });
    } else {
      await new Promise(resolveWait => setTimeout(resolveWait, 50));
      if (mode === "detach-after-dispatch") expect(handler).toHaveBeenCalledOnce();
      else expect(handler).not.toHaveBeenCalled();
      if (mode.startsWith("detach")) {
        expect(await readFile(join(root, "control-plane/control-plane-state.json"), "utf8")).toBe(retiredJournal);
        expect(core.store.state.commands.filter(command => command.type === "semantic_tool.result")).toHaveLength(0);
      }
    }
  } finally {
    await bundle.transport.close().catch(() => undefined);
    await core?.stop().catch(() => undefined);
    if (pid) { try { process.kill(-pid, "SIGKILL"); } catch { /* owned child already stopped */ } }
    await rm(root, { recursive: true, force: true });
  }
}, 20_000);

it("waits for authenticated startup when a recovered provider calls an already installed handler", async () => {
  const root = await mkdtemp(join(tmpdir(), "runner-handler-early-recovery-"));
  const tool = { name: "get_task_context", description: "Read the task.", inputSchema: { type: "object", properties: {}, additionalProperties: false } };
  const options = { runnerBinary: defaultCapabilityRunnerdBinary(),
    codexCommand: resolve(import.meta.dirname, "../../runner/target/debug/fake-codex-app-server"),
    codexArgs: ["--state-file", join(root, "provider.json"), "--hold-turn", "--emit-tool-call-on-resume"], stateDirectory: root,
    prpIdentity: { runnerInstanceId: "early-handler-runner", environmentLeaseId: "early-handler-lease", runId: "early-handler-run",
      normalizedSessionId: "early-handler-session", turnId: "early-handler-turn", itemId: "early-handler-item" } };
  const first = createCapabilityRunnerdCodexTransport(options);
  let restored: ReturnType<typeof createCapabilityRunnerdCodexTransport> | undefined;
  const kill = (pid: number | null) => { if (pid) { try { process.kill(pid, "SIGKILL"); } catch { /* owned child already exited */ } } };
  try {
    await first.transport.request("thread/start", { cwd: root, dynamicTools: [tool] });
    await first.transport.request("turn/start", { input: [{ type: "text", text: "Hold this synthetic turn." }] });
    const previous = first.evidence();
    await first.detachControllerForRestart();
    kill(previous.runnerPid); kill(previous.providerPid);
    await vi.waitFor(() => {
      for (const pid of [previous.runnerPid, previous.providerPid]) if (pid) expect(() => process.kill(pid, 0)).toThrow();
    });
    restored = createCapabilityRunnerdCodexTransport({ ...options, resumeDynamicTools: [tool], resumeActiveTurnId: "provider-turn-1" });
    const handler = vi.fn(async (_request: CodexRpcServerRequest) => ({ success: true, contentItems: [{ type: "inputText", text: JSON.stringify({ task: { id: "synthetic-task" } }) }] }));
    restored.transport.setServerRequestHandler(handler);
    await restored.transport.request("thread/read", { threadId: "codex-thread-1" });
    await vi.waitFor(() => expect(handler).toHaveBeenCalledOnce());
    expect(handler.mock.calls[0]).toEqual([expect.objectContaining({ params: expect.objectContaining({ threadId: "codex-thread-1", turnId: "provider-turn-1" }) })]);
  } finally {
    await restored?.transport.close().catch(() => undefined);
    await first.transport.close().catch(() => undefined);
    for (const bundle of [first, restored]) if (bundle) { const e = bundle.evidence(); kill(e.runnerPid); kill(e.providerPid); }
    await rm(root, { recursive: true, force: true });
  }
}, 20_000);
