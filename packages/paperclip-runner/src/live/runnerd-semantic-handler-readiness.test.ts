import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { expect, it, vi } from "vitest";
import type { DurablePrpControlPlane } from "../control-plane/durable-prp-control-plane.js";
import type { CodexRpcServerRequest } from "../drivers/codex/app-server-transport.js";
import { createCapabilityRunnerdCodexTransport, defaultCapabilityRunnerdBinary } from "./runnerd-codex-transport.js";

it.each(["install", "close", "detach", "detach-after-dispatch"] as const)("holds an authenticated semantic call until handler readiness or %s", async mode => {
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
      const commandCount = core.store.state.commands.length;
      const detached = bundle.detachControllerForRestart();
      try {
        await expect(bundle.transport.request("turn/start", { input: [{ type: "text", text: "Do not admit replacement work." }] })).rejects.toThrow("transport is closed");
        await expect(bundle.transport.request("thread/start", { cwd: root })).rejects.toThrow("transport is closed");
        expect(core.store.state.commands).toHaveLength(commandCount);
      } finally { releaseDrain(); }
      await detached;
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
    if (pid) { try { process.kill(-pid, "SIGKILL"); } catch { /* owned child already stopped */ } }
    await rm(root, { recursive: true, force: true });
  }
}, 20_000);
