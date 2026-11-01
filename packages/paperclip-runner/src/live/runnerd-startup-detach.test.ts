import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it, vi } from "vitest";
import { DurablePrpControlPlane } from "../control-plane/durable-prp-control-plane.js";
import { createCapabilityRunnerdCodexTransport } from "./runnerd-codex-transport.js";

it.each(["settled", "rejected", "timeout"] as const)("joins admitted resume preparation before detaching its journal writer (%s)", async outcome => {
  const root = await mkdtemp(join(tmpdir(), "runner-startup-detach-"));
  const identity = { runnerInstanceId: "runner-startup", environmentLeaseId: "lease-startup", runId: "run-prior",
    normalizedSessionId: "session-startup", turnId: "turn-prior", itemId: "item-prior" };
  const successorIdentity = { ...identity, runId: "run-next", turnId: "turn-next", itemId: "item-next" };
  const artifact = { version: "0.3.0", digest: `sha256:${"a".repeat(64)}` };
  const seed = new DurablePrpControlPlane({ stateDirectory: join(root, "control-plane"), identity,
    expectedRunnerVersion: artifact.version, expectedRunnerDigest: artifact.digest });
  seed.persistRunAttachTemplate({ provider: { kind: "codex", command: "/fixture/codex", model: "fixture" } });
  const original = await readFile(seed.store.path, "utf8");
  let enter!: () => void, release!: () => void, reject!: (error: Error) => void;
  const entered = new Promise<void>(resolve => { enter = resolve; });
  const preparation = new Promise<void>((resolve, fail) => { release = resolve; reject = fail; });
  const steps: string[] = [];
  const suspended = { schema: "paperclip.runner.durable.state.v1", ...identity, lifecycle: "suspended" };
  const archive = vi.fn(async () => { steps.push("archive"); return suspended; });
  const launch = vi.fn(() => { throw new Error("Unexpected provider launch"); });
  const registration = vi.fn(async () => { throw new Error("Unexpected route registration"); });
  const bundle = createCapabilityRunnerdCodexTransport({
    stateDirectory: root, runnerArtifact: artifact, runnerBinary: "/fixture/runnerd", codexCommand: "/fixture/codex",
    prpIdentity: successorIdentity, closeGraceMs: outcome === "timeout" ? 100 : 1_500,
    readRunnerState: async () => suspended,
    prepareExternalRunnerState: async () => { enter(); await preparation; },
    archiveExternalRunnerState: archive, runnerProcessLauncher: launch, controlPlaneRegistration: registration,
  });
  const startup = bundle.transport.request("thread/read", {}).then(
    () => ({ error: null }), error => ({ error: error as Error }),
  );
  let detached: Promise<void> | undefined;
  try {
    await entered;
    let completed = false;
    detached = bundle.detachControllerForRestart().then(() => { completed = true; steps.push("detached"); });
    void detached.catch(() => undefined);
    await new Promise<void>(resolve => setImmediate(resolve));
    const completedWhilePreparationHeld = completed;
    if (outcome === "timeout") {
      await expect(detached).rejects.toThrow("startup drain timed out");
      expect(await readFile(seed.store.path, "utf8")).toBe(original);
      expect(archive).not.toHaveBeenCalled();
      await expect(bundle.detachControllerForRestart()).rejects.toThrow("startup drain timed out");
      await expect(bundle.transport.close()).rejects.toThrow("startup drain timed out");
      expect(completed).toBe(false);
      reject(new Error("fixture preparation retired after failed detach"));
      await startup;
      return;
    }
    if (outcome === "rejected") reject(new Error("fixture preparation rejected"));
    else release();
    const result = await startup;
    await detached;
    expect(result.error?.message).toMatch(outcome === "rejected" ? /fixture preparation rejected/ : /closed during provider startup/);
    expect(completedWhilePreparationHeld).toBe(false);
    expect(steps).toEqual(outcome === "rejected" ? ["detached"] : ["archive", "detached"]);
    if (outcome === "settled") {
      const written = JSON.parse(await readFile(seed.store.path, "utf8"));
      expect(written.commands.some((command: { type: string }) => command.type === "run.attach")).toBe(true);
      const successor = new DurablePrpControlPlane({ stateDirectory: join(root, "control-plane"), identity: successorIdentity,
        expectedRunnerVersion: artifact.version, expectedRunnerDigest: artifact.digest });
      successor.queueCommand("session.snapshot", {}, "successor-command");
      const accepted = await readFile(successor.store.path, "utf8");
      await bundle.transport.close();
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(await readFile(successor.store.path, "utf8")).toBe(accepted);
    }
  } finally {
    release();
    await startup;
    await detached?.catch(() => undefined);
    expect(launch).not.toHaveBeenCalled();
    expect(registration).not.toHaveBeenCalled();
    await rm(root, { recursive: true, force: true });
  }
}, 5_000);
