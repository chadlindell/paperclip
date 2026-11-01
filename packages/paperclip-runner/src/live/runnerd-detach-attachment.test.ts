import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { expect, it } from "vitest";
import type { DurablePrpControlPlane, RunnerProcessHandle, RunnerProcessResult } from "../control-plane/durable-prp-control-plane.js";
import { createCapabilityRunnerdCodexTransport, defaultCapabilityRunnerdBinary } from "./runnerd-codex-transport.js";

function gate() {
  let release!: () => void;
  const promise = new Promise<void>(done => { release = done; });
  return { promise, release };
}

it.each(["registration", "activation"] as const)(
  "joins an admitted warm attachment and its route release before detach succeeds (%s)",
  async mode => {
    const root = await mkdtemp(join(tmpdir(), "runnerd-detach-attachment-"));
    const entered = gate();
    const held = gate();
    const releaseEntered = gate();
    const releaseHeld = gate();
    const handles: RunnerProcessHandle[] = [];
    const routes: string[] = [];
    let core!: DurablePrpControlPlane;
    let initialized = false;
    let attaching: Promise<void> | undefined;
    let detaching: Promise<void> | undefined;
    let detached = false;
    const providerLog = join(root, "provider-calls.log");
    const bundle = createCapabilityRunnerdCodexTransport({
      runnerBinary: defaultCapabilityRunnerdBinary(),
      codexCommand: resolve(import.meta.dirname, "../../runner/target/debug/fake-codex-app-server"),
      codexArgs: ["--state-file", join(root, "provider.json"), "--call-log", providerLog],
      sourceCodexHome: "",
      stateDirectory: root,
      closeGraceMs: 5_000,
      lifecyclePolicy: { mode: "warm", idleTimeoutMs: 60_000 },
      controlPlaneRegistration: async (authority, identity) => {
        core = authority;
        if (!initialized) {
          initialized = true;
          await authority.start();
          return { release: () => { routes.push("prior:released"); } };
        }
        expect(identity?.runId).toBe("attachment-next-run");
        routes.push("next:registration-started");
        if (mode === "registration") {
          entered.release();
          await held.promise;
        }
        routes.push("next:registered");
        return {
          activate: async () => {
            routes.push("next:activation-started");
            if (mode === "activation") {
              entered.release();
              await held.promise;
            }
            routes.push("next:activated");
          },
          release: async () => {
            routes.push("next:release-started");
            releaseEntered.release();
            await releaseHeld.promise;
            routes.push("next:released");
          },
        };
      },
      runnerProcessLauncher: spec => {
        const child = spawn(spec.command, [...spec.args], {
          cwd: spec.cwd, env: spec.environment, stdio: "ignore",
        });
        const completion = new Promise<RunnerProcessResult>((done, reject) => {
          child.once("error", reject);
          child.once("exit", (code, signal) => done({ code, signal, stdout: "", stderr: "" }));
        });
        const handle = { child, completion };
        handles.push(handle);
        return handle;
      },
    });
    const journal = () => readFile(join(root, "control-plane", "control-plane-state.json"), "utf8");
    try {
      await bundle.transport.request("thread/start", { cwd: root });
      const initialIdentity = structuredClone(core.store.state.identity);
      const providerPid = bundle.evidence().codexPid;
      expect(providerPid).toBeGreaterThan(0);
      attaching = bundle.transport.attachRun!({
        runId: "attachment-next-run", turnId: "attachment-next-turn", itemId: "attachment-next-item",
      });
      void attaching.catch(() => undefined);
      await entered.promise;
      if (mode === "activation") {
        expect(core.store.state.commands.some(command => command.type === "run.attach" && command.status === "completed")).toBe(true);
      }
      detaching = bundle.detachControllerForRestart().then(() => { detached = true; });
      void detaching.catch(() => undefined);
      await new Promise(done => setTimeout(done, 100));
      expect(detached, "detach must join the originally admitted attachment callback").toBe(false);
      held.release();
      await releaseEntered.promise;
      await new Promise(done => setTimeout(done, 100));
      expect(detached, "detach must also join that attachment's asynchronous route release").toBe(false);
      releaseHeld.release();
      await expect(attaching).rejects.toThrow(/closed/);
      await detaching;
      expect(detached).toBe(true);
      expect(routes.filter(value => value === "next:released")).toHaveLength(1);
      if (mode === "registration") {
        expect(core.store.state.identity).toEqual(initialIdentity);
        expect(routes).not.toContain("next:activation-started");
        expect(core.store.state.commands.some(command => command.type === "run.attach")).toBe(false);
      }
      const retiredJournal = await journal();
      const retiredRoutes = [...routes];
      const retiredIdentity = structuredClone(core.store.state.identity);
      const providerCalls = await readFile(providerLog, "utf8");
      expect(providerCalls.split("\n").filter(line => line === "initialize")).toHaveLength(1);
      expect(handles).toHaveLength(1);
      expect(bundle.evidence().codexPid).toBe(providerPid);
      await new Promise(done => setTimeout(done, 150));
      expect(await journal()).toBe(retiredJournal);
      expect(routes).toEqual(retiredRoutes);
      expect(core.store.state.identity).toEqual(retiredIdentity);
      expect(await readFile(providerLog, "utf8")).toBe(providerCalls);
      expect(handles).toHaveLength(1);
    } finally {
      held.release();
      releaseHeld.release();
      await attaching?.catch(() => undefined);
      await detaching?.catch(() => undefined);
      await bundle.transport.close().catch(() => undefined);
      for (const handle of handles) {
        if (handle.child.exitCode === null && handle.child.signalCode === null) handle.child.kill("SIGKILL");
        await handle.completion.catch(() => undefined);
      }
      await rm(root, { recursive: true, force: true });
    }
  }, 20_000,
);
