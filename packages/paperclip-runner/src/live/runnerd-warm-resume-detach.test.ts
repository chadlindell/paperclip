import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it, vi } from "vitest";
import { DurablePrpControlPlane } from "../control-plane/durable-prp-control-plane.js";
import { createCapabilityRunnerdCodexTransport } from "./runnerd-codex-transport.js";

function gate() {
  let release!: () => void;
  const promise = new Promise<void>(done => { release = done; });
  return { promise, release };
}

// Controlled durable checkpoint and public transport API. No Runner or provider
// process is launched: this proves controller cleanup, not native execution.
it.each([false, true])("preserves source-route cleanup during admitted warm resume (release failure: %s)", async failRelease => {
  const root = await mkdtemp(join(tmpdir(), "runner-warm-resume-detach-"));
  const source = { runnerInstanceId: "runner-warm-resume", environmentLeaseId: "lease-warm-resume",
    runId: "source-run", normalizedSessionId: "session-warm-resume", turnId: "source-turn", itemId: "source-item" };
  const target = { ...source, runId: "target-run", turnId: "target-turn", itemId: "target-item" };
  const artifact = { version: "0.3.0", digest: `sha256:${"a".repeat(64)}` };
  const seed = new DurablePrpControlPlane({ stateDirectory: join(root, "control-plane"), identity: source,
    expectedRunnerVersion: artifact.version, expectedRunnerDigest: artifact.digest });
  const template = { provider: { kind: "codex", command: "/fixture/codex", model: "fixture" } };
  const connection = { mode: "connect" as const, connectUrl: "ws://127.0.0.1:31999/target" };
  seed.persistRunAttachTemplate(template);
  seed.stageRunAttachment(target, template);
  // Restore the persisted queued boundary directly. Creating a new warm command
  // requires a real authenticated Runner, which this controller-only test does
  // not claim to exercise; the normal recovery reader validates this checkpoint.
  const queued = structuredClone(seed.store.state);
  queued.commands.push({ schema: "paperclip.prp.command.v1", commandId: "queued-attachment", controllerSeq: 1,
    type: "run.attach", issuedAt: new Date().toISOString(), status: "pending", result: null,
    payload: { ...template, paperclipNextAuthority: { identity: target, connection } } });
  const { runAttachTemplate: _template, ...staged } = queued.stagedRunAttachment!;
  queued.stagedRunAttachment = { ...staged, commandId: "queued-attachment" };
  seed.store.commit(queued);
  const entered = gate(), held = gate(), releaseEntered = gate(), releaseHeld = gate();
  const releaseRoute = vi.fn(async () => {
    releaseEntered.release();
    await releaseHeld.promise;
    if (failRelease) throw new Error("synthetic source registration release failed");
  });
  const launch = vi.fn(() => { throw new Error("Unexpected native launch"); });
  const registration = vi.fn(async (_core: DurablePrpControlPlane, identity = source) => {
    expect(identity).toEqual(source);
    entered.release();
    await held.promise;
    return { connection: { mode: "connect" as const, connectUrl: "ws://127.0.0.1:31999/source" }, release: releaseRoute };
  });
  const bundle = createCapabilityRunnerdCodexTransport({
    runnerBinary: "/fixture/runnerd", codexCommand: "/fixture/codex", runnerArtifact: artifact,
    stateDirectory: root, prpIdentity: target, closeGraceMs: 1_500,
    readRunnerState: async () => ({ schema: "paperclip.runner.durable.state.v1", ...source, lifecycle: "session_open" }),
    adoptExistingRunner: { pid: 999999, processGroupId: 999999, startedAt: "2026-01-01T00:00:00.000Z",
      isAlive: () => true, signal: () => { throw new Error("Synthetic owner must not be signalled"); } },
    runnerProcessLauncher: launch, controlPlaneRegistration: registration,
    warmTransitionRegistrationMode: "routed_connect",
    resumeProviderSession: { driverSessionId: "fixture-thread", providerSessionId: "fixture-session" },
  });
  const opening = bundle.transport.request("thread/read", {});
  void opening.catch(() => undefined);
  let detaching: Promise<void> | undefined;
  try {
    await Promise.race([entered.promise, opening.then(() => { throw new Error("Unexpected completed resume"); })]);
    let settled = false;
    detaching = bundle.detachControllerForRestart().finally(() => { settled = true; });
    void detaching.catch(() => undefined);
    await new Promise(done => setImmediate(done));
    expect(settled).toBe(false);
    held.release();
    await releaseEntered.promise;
    await new Promise(done => setImmediate(done));
    expect(settled).toBe(false);
    releaseHeld.release();
    await expect(opening).rejects.toThrow(/closed during provider startup/);
    if (failRelease) await expect(detaching).rejects.toThrow("synthetic source registration release failed");
    else await detaching;
    expect(releaseRoute).toHaveBeenCalledOnce();
    expect(registration).toHaveBeenCalledOnce();
    expect(launch).not.toHaveBeenCalled();
    const retired = await readFile(seed.store.path, "utf8");
    await new Promise(done => setImmediate(done));
    expect(await readFile(seed.store.path, "utf8")).toBe(retired);
    if (failRelease) await expect(bundle.detachControllerForRestart()).rejects.toThrow("synthetic source registration release failed");
  } finally {
    held.release(); releaseHeld.release();
    await opening.catch(() => undefined);
    await detaching?.catch(() => undefined);
    await bundle.transport.close().catch(() => undefined);
    await seed.stop();
    await rm(root, { recursive: true, force: true });
  }
}, 5_000);
