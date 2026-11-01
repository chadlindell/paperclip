import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it, vi } from "vitest";
import * as runtimeMaterializer from "../drivers/runtime-context-materializer.js";
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

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}

async function registeredFixture(releaseRoute: () => Promise<void>, registrationGate?: Promise<void>) {
  const root = await mkdtemp(join(tmpdir(), "runner-detach-owned-core-"));
  const entered = deferred();
  let core!: DurablePrpControlPlane;
  const materialize = vi.spyOn(runtimeMaterializer, "prepareIsolatedCodexHome").mockResolvedValue(undefined);
  const bundle = createCapabilityRunnerdCodexTransport({
    stateDirectory: root, runnerBinary: "/fixture/runnerd", codexCommand: "/fixture/codex", closeGraceMs: 100,
    runnerArtifact: { version: "0.3.0", digest: `sha256:${"a".repeat(64)}` },
    controlPlaneRegistration: async authority => {
      core = authority;
      await core.start();
      const connectUrl = core.connectUrl;
      entered.resolve();
      await registrationGate;
      return { connectUrl, release: releaseRoute };
    },
    runnerProcessLauncher: () => { throw new Error("fixture launch stopped"); },
  });
  const opening = bundle.transport.request("thread/start", { cwd: root, dynamicTools: [] });
  void opening.catch(() => undefined);
  await entered.promise;
  if (!registrationGate) await expect(opening).rejects.toThrow("fixture launch stopped");
  return { bundle, core, root, opening, async cleanup() {
    await opening.catch(() => undefined);
    await bundle.transport.close().catch(() => undefined);
    await core.stop().catch(() => undefined);
    materialize.mockRestore();
    await rm(root, { recursive: true, force: true });
  } };
}

async function boundedOutcome(operation: Promise<void>) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      operation.then(() => ({ error: null }), error => ({ error: error as Error })),
      new Promise<never>((_resolve, reject) => { timer = setTimeout(() => reject(new Error("fixture retirement never settled")), 700); }),
    ]);
  } finally { clearTimeout(timer); }
}

it.each(["stopped", "stop-failed"] as const)("still retires an existing core after startup join timeout (%s)", async mode => {
  const gate = deferred();
  const release = vi.fn(async () => {});
  const fixture = await registeredFixture(release, gate.promise);
  const stop = vi.spyOn(fixture.core, "stop");
  if (mode === "stop-failed") stop.mockRejectedValueOnce(new Error("fixture stop failed"));
  try {
    const next = fixture.bundle.transport.notifications()[Symbol.asyncIterator]().next();
    const result = await boundedOutcome(fixture.bundle.detachControllerForRestart());
    expect(result.error?.message).toContain("startup drain timed out");
    expect(stop).toHaveBeenCalled();
    if (mode === "stop-failed") {
      expect(result.error).toBeInstanceOf(AggregateError);
      expect(JSON.stringify((result.error as AggregateError).errors, (_key, value) => value instanceof Error ? { message: value.message, errors: (value as AggregateError).errors } : value)).toContain("fixture stop failed");
    }
    await expect(next).resolves.toMatchObject({ done: true });
    gate.resolve();
    await expect(fixture.opening).rejects.toThrow("closed during provider startup");
    expect(release).toHaveBeenCalledOnce();
    await expect(fixture.bundle.detachControllerForRestart()).rejects.toThrow("startup drain timed out");
  } finally {
    gate.resolve();
    stop.mockRestore();
    await fixture.cleanup();
  }
});

it.each(["held", "failed", "held-stop"] as const)("rejects unconfirmed retirement while still attempting all cleanup (%s)", async mode => {
  const gate = deferred();
  const release = vi.fn(async () => {
    if (mode === "held") await gate.promise;
    if (mode === "failed") throw new Error("fixture route release failed");
  });
  const fixture = await registeredFixture(release);
  const originalStop = fixture.core.stop.bind(fixture.core);
  const stop = vi.spyOn(fixture.core, "stop");
  if (mode === "held-stop") stop.mockImplementationOnce(async () => { await gate.promise; await originalStop(); });
  try {
    const next = fixture.bundle.transport.notifications()[Symbol.asyncIterator]().next();
    const result = await boundedOutcome(fixture.bundle.detachControllerForRestart());
    expect(result.error?.message).toMatch(mode === "failed" ? /fixture route release failed/ : /timed out/);
    expect(release).toHaveBeenCalledOnce();
    expect(stop).toHaveBeenCalled();
    await expect(next).resolves.toMatchObject({ done: true });
    await expect(fixture.bundle.detachControllerForRestart()).rejects.toThrow();
  } finally {
    gate.resolve();
    stop.mockRestore();
    await fixture.cleanup();
  }
});
