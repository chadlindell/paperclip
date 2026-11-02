import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it, vi } from "vitest";
import * as runtimeMaterializer from "../drivers/runtime-context-materializer.js";
import { DurablePrpControlPlane } from "../control-plane/durable-prp-control-plane.js";
import { createCapabilityRunnerdCodexTransport } from "./runnerd-codex-transport.js";

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
      entered.resolve();
      await registrationGate;
      return { connectUrl: core.connectUrl, release: releaseRoute };
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

it.each(["settled", "failed"] as const)("joins an existing close without converting its outcome to detach success (%s)", async mode => {
  const gate = deferred();
  const releasing = deferred();
  const fixture = await registeredFixture(async () => {
    releasing.resolve();
    await gate.promise;
    if (mode === "failed") throw new Error("fixture close release failed");
  });
  const closing = fixture.bundle.transport.close();
  void closing.catch(() => undefined);
  try {
    await releasing.promise;
    let detached = false;
    const detaching = fixture.bundle.detachControllerForRestart().then(() => { detached = true; });
    void detaching.catch(() => undefined);
    await new Promise<void>(resolve => setImmediate(resolve));
    const completedBeforeClose = detached;
    gate.resolve();
    if (mode === "failed") {
      await expect(closing).rejects.toThrow("fixture close release failed");
      await expect(detaching).rejects.toThrow("fixture close release failed");
      await expect(fixture.bundle.detachControllerForRestart()).rejects.toThrow("fixture close release failed");
    } else {
      await closing;
      await detaching;
    }
    expect(completedBeforeClose).toBe(false);
  } finally {
    gate.resolve();
    await closing.catch(() => undefined);
    await fixture.cleanup();
  }
});

it("does not convert an already failed close into the first successful detach", async () => {
  const fixture = await registeredFixture(async () => {});
  const failure = new Error("fixture persistent core stop failure");
  const stop = vi.spyOn(fixture.core, "stop").mockRejectedValue(failure);
  try {
    const closeResult = await fixture.bundle.transport.close().then(() => null, error => error);
    expect(closeResult).toBeInstanceOf(Error);
    expect(stop).toHaveBeenCalled();
    // There has been no previous detach: the closed flag is not retirement proof.
    await expect(fixture.bundle.detachControllerForRestart()).rejects.toBe(closeResult);
    await expect(fixture.bundle.detachControllerForRestart()).rejects.toBe(closeResult);
  } finally { stop.mockRestore(); await fixture.cleanup(); }
});
