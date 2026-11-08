import { mkdtemp, readFile, rm } from "node:fs/promises";
import { createServer, request } from "node:http";
import { createHash, randomBytes } from "node:crypto";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { expect, it } from "vitest";
import { inspectWarmRunTransition, inspectStagedRunAttachmentHeader, type DurablePrpCheckpoint, type DurablePrpControlPlane, type RunnerProcessHandle, type RunnerProcessResult,
  type PrpWireConnection } from "../control-plane/durable-prp-control-plane.js";
import { createCapabilityRunnerdCodexTransport, defaultCapabilityRunnerdBinary, readRunnerdArtifactBinding } from "./runnerd-codex-transport.js";

// The guest rejects WebSocket extensions. Use the same uncompressed wire
// contract as production, without importing the server connector into Runner.
async function openOwnedWire(url: string): Promise<{ wire: PrpWireConnection; closed: Promise<void>; close(): void }> {
  return new Promise((resolvePeer, reject) => {
    const key = randomBytes(16).toString("base64");
    const req = request(url.replace("ws:", "http:"), { headers: {
      Connection: "Upgrade", Upgrade: "websocket", "Sec-WebSocket-Version": "13", "Sec-WebSocket-Key": key,
    } });
    req.on("error", reject); req.on("response", response => { response.resume(); reject(new Error(`Guest upgrade refused: ${response.statusCode}`)); });
    req.setTimeout(1_000, () => req.destroy(new Error("Guest upgrade timeout")));
    req.on("upgrade", (response, socket, head) => {
      socket.setTimeout(0);
      const expected = createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
      if (response.headers["sec-websocket-accept"] !== expected) { socket.destroy(); reject(new Error("Invalid guest upgrade")); return; }
      const listeners: Array<(value: unknown) => void> = [], closes: Array<() => void> = [];
      let buffer: Buffer = head, ended = false;
      let closeResolve!: () => void;
      const closed = new Promise<void>(done => { closeResolve = done; });
      const finish = () => { if (ended) return; ended = true; for (const close of closes) close(); closeResolve(); };
      const send = (payload: Buffer, opcode: number) => {
        const mask = randomBytes(4), prefix = payload.length < 126 ? Buffer.from([0x80 | opcode, 0x80 | payload.length]) : Buffer.alloc(payload.length <= 65535 ? 4 : 10);
        if (payload.length >= 126) { prefix[0] = 0x80 | opcode; prefix[1] = 0x80 | (payload.length <= 65535 ? 126 : 127);
          if (payload.length <= 65535) prefix.writeUInt16BE(payload.length, 2); else prefix.writeBigUInt64BE(BigInt(payload.length), 2); }
        const masked = Buffer.from(payload); for (let index = 0; index < masked.length; index++) masked[index] ^= mask[index % 4]!;
        socket.write(Buffer.concat([prefix, mask, masked]));
      };
      const parse = () => {
        while (buffer.length >= 2) {
          const opcode = buffer[0]! & 15, initial = buffer[1]! & 127;
          if (buffer.length < (initial === 126 ? 4 : initial === 127 ? 10 : 2)) return;
          const size = initial === 126 ? buffer.readUInt16BE(2) : initial === 127 ? Number(buffer.readBigUInt64BE(2)) : initial;
          const offset = initial === 126 ? 4 : initial === 127 ? 10 : 2;
          if (buffer.length < offset + size) return;
          // Upgrade head bytes can precede the caller's message subscription.
          // Retain complete data frames until the control-plane listener exists.
          if ((opcode === 1 || opcode === 2) && listeners.length === 0) return;
          const payload = buffer.subarray(offset, offset + size); buffer = buffer.subarray(offset + size);
          if (opcode === 8) { socket.destroy(); finish(); return; }
          if (opcode === 9) send(payload, 10);
          if (opcode === 1 || opcode === 2) for (const listener of listeners) listener(JSON.parse(payload.toString("utf8")));
        }
      };
      socket.on("data", bytes => { buffer = Buffer.concat([buffer, bytes]); parse(); }); socket.on("close", finish); socket.on("error", finish);
      const close = () => { socket.destroy(); finish(); };
      resolvePeer({ wire: { sendJson: value => send(Buffer.from(JSON.stringify(value)), 1), close,
        onJson: listener => { listeners.push(listener); parse(); }, onClose: listener => { closes.push(() => listener({})); } }, closed, close });
      queueMicrotask(parse);
    }); req.end();
  });
}

function gate() {
  let release!: () => void;
  const promise = new Promise<void>(done => { release = done; });
  return { promise, release };
}

// Real Runner, scripted provider and listening PRP connection. The gates hold
// owned I/O callbacks; no private transport state or transition proof is mocked.
it.each(["prior-release", "physical-read"] as const)("does not activate a staged listening route after detach times out during %s", async mode => {
  const root = await mkdtemp(join(tmpdir(), "runner-detach-staged-listen-"));
  const guest = createServer();
  await new Promise<void>(done => guest.listen(0, "127.0.0.1", done));
  const address = guest.address();
  if (!address || typeof address === "string") throw new Error("Missing owned listener");
  const port = address.port;
  await new Promise<void>(done => guest.close(() => done()));
  const source = { runnerInstanceId: "runner-listen", environmentLeaseId: "lease-listen", runId: "source-run",
    normalizedSessionId: "session-listen", turnId: "source-turn", itemId: "source-item" };
  const target = { ...source, runId: "target-run", turnId: "target-turn", itemId: "target-item" };
  const entered = gate(), held = gate();
  const handles: RunnerProcessHandle[] = [];
  const routeEvents: string[] = [];
  const routeClosers: Array<() => void> = [];
  const connections: Promise<void>[] = [];
  let core!: DurablePrpControlPlane;
  let attaching: Promise<void> | undefined, detaching: Promise<void> | undefined;
  let physicalHeld = false;
  let runnerStderr = "";
  const binary = defaultCapabilityRunnerdBinary(), artifact = readRunnerdArtifactBinding(binary);
  const bundle = createCapabilityRunnerdCodexTransport({
    runnerBinary: binary, codexCommand: resolve(import.meta.dirname, "../../runner/target/debug/fake-codex-app-server"),
    codexArgs: ["--state-file", join(root, "provider.json"), "--call-log", join(root, "provider-calls.log")],
    sourceCodexHome: "", stateDirectory: root, prpIdentity: source,
    closeGraceMs: 200, runnerReconnectGraceMs: 5_000,
    lifecyclePolicy: { mode: "warm", idleTimeoutMs: 60_000 },
    onStagedRunAttachment: async () => {},
    readRunnerState: async () => {
      const state = JSON.parse(await readFile(join(root, "runner/runner-state.json"), "utf8"));
      if (mode === "physical-read" && !physicalHeld && core.store.state.stagedRunAttachment) {
        const proof = inspectWarmRunTransition({ controlPlaneState: core.store.state, runnerState: state,
          expectedNewIdentity: target, expectedRunnerVersion: artifact.version, expectedRunnerDigest: artifact.digest });
        if (proof?.runnerIdentity.runId === target.runId) {
          physicalHeld = true; entered.release(); await held.promise;
        }
      }
      return state;
    },
    controlPlaneRegistration: async (authority, identity = authority.store.state.identity) => {
      core = authority;
      const isTarget = identity.runId === target.runId;
      const name = isTarget ? "target" : "source";
      const path = `/api/runner/v1/connect/${identity.runId}`;
      let stopped = false, peer: Awaited<ReturnType<typeof openOwnedWire>> | undefined;
      const close = () => { stopped = true; peer?.close(); };
      routeClosers.push(close);
      let resolveReady!: () => void, rejectReady!: (error: Error) => void;
      const ready = new Promise<void>((done, fail) => { resolveReady = done; rejectReady = fail; });
      void ready.catch(() => undefined);
      const connect = async () => {
        const deadline = Date.now() + 6_000;
        while (!stopped && Date.now() < deadline) {
          try {
            peer = await openOwnedWire(`ws://127.0.0.1:${port}${path}`);
            if (stopped) { peer.close(); return; }
            core.attachWireConnection(peer.wire); resolveReady(); await peer.closed;
          } catch { /* Runner retargets its listener only after the old ACK. */ }
          if (!stopped) await new Promise<void>(done => setTimeout(done, 50));
        }
        if (!stopped) rejectReady(new Error("Owned listener never became ready"));
      };
      return { connection: { mode: "listen" as const, listenAddress: "0.0.0.0", listenPort: port, listenPath: path },
        activate: () => { routeEvents.push(`${name}:activate`); connections.push(connect()); }, ready: () => ready,
        release: async (reason?: "detach") => {
          routeEvents.push(`${name}:release:${reason ?? "normal"}`);
          if (!isTarget && mode === "prior-release") { entered.release(); await held.promise; }
          close();
        },
      };
    },
    runnerProcessLauncher: spec => {
      const child = spawn(spec.command, [...spec.args], { cwd: spec.cwd, env: spec.environment, stdio: ["ignore", "ignore", "pipe"] });
      child.stderr.on("data", bytes => { runnerStderr += bytes.toString(); });
      const completion = new Promise<RunnerProcessResult>((done, fail) => {
        child.once("error", fail); child.once("exit", (code, signal) => done({ code, signal, stdout: "", stderr: "" }));
      });
      const handle = { child, completion }; handles.push(handle); return handle;
    },
  });
  try {
    await bundle.transport.request("thread/start", { cwd: root });
    attaching = bundle.transport.attachRun!({ runId: target.runId, turnId: target.turnId, itemId: target.itemId });
    void attaching.catch(() => undefined);
    await entered.promise;
    expect(core.store.state.commands.some(command => command.type === "run.attach" && command.status === "completed")).toBe(true);
    expect(routeEvents).not.toContain("target:activate");
    detaching = bundle.detachControllerForRestart();
    await expect(detaching).rejects.toThrow(/startup drain timed out/);
    held.release();
    await expect(attaching).rejects.toThrow(/closed/);
    expect(routeEvents).not.toContain("target:activate");
    expect(routeEvents).toContain("target:release:detach");
    expect(handles).toHaveLength(1);
    const journal = await readFile(join(root, "control-plane/control-plane-state.json"), "utf8");
    const routes = [...routeEvents];
    await bundle.transport.close().catch(() => undefined);
    await new Promise<void>(done => setImmediate(done));
    expect(await readFile(join(root, "control-plane/control-plane-state.json"), "utf8")).toBe(journal);
    expect(routeEvents).toEqual(routes);
  } catch (error) {
    throw new Error(`${String(error)}; Runner stderr: ${runnerStderr}`, { cause: error });
  } finally {
    held.release();
    await attaching?.catch(() => undefined);
    await detaching?.catch(() => undefined);
    await bundle.detachControllerForRestart().catch(() => undefined);
    for (const close of routeClosers) close();
    for (const handle of handles) {
      if (handle.child.exitCode === null && handle.child.signalCode === null) handle.child.kill("SIGKILL");
      await handle.completion.catch(() => undefined);
    }
    await Promise.allSettled(connections);
    await core?.stop();
    await rm(root, { recursive: true, force: true });
  }
}, 15_000);


it.each(["prior-release", "physical-read"] as const)("does not activate a recovered staged listening route after detach during %s", async mode => {
  const root = await mkdtemp(join(tmpdir(), "runner-recovery-staged-listen-"));
  const originalRoot = join(root, "original"), recoveredRoot = join(root, "recovered");
  const guest = createServer();
  await new Promise<void>(done => guest.listen(0, "127.0.0.1", done));
  const address = guest.address();
  if (!address || typeof address === "string") throw new Error("Missing owned listener");
  const port = address.port;
  await new Promise<void>(done => guest.close(() => done()));
  const source = { runnerInstanceId: "runner-recovery-listen", environmentLeaseId: "lease-recovery-listen", runId: "source-run",
    normalizedSessionId: "session-recovery-listen", turnId: "source-turn", itemId: "source-item" };
  const target = { ...source, runId: "target-run", turnId: "target-turn", itemId: "target-item" };
  const queued = gate(), queuedHold = gate(), entered = gate(), held = gate();
  let checkpoint!: DurablePrpCheckpoint, generation = 0, checkpointHeld = false, recovering = false, physicalHeld = false;
  let core!: DurablePrpControlPlane, runnerStderr = "";
  const registered = new WeakSet<DurablePrpControlPlane>(), cores = new Set<DurablePrpControlPlane>();
  const routeEvents: string[] = [], routeClosers: Array<() => void> = [], connections: Promise<void>[] = [];
  const handles: RunnerProcessHandle[] = [];
  const binary = defaultCapabilityRunnerdBinary(), artifact = readRunnerdArtifactBinding(binary);
  const readRunnerState = async () => {
    const state = JSON.parse(await readFile(join(originalRoot, "runner/runner-state.json"), "utf8"));
    if (recovering && mode === "physical-read" && !physicalHeld && core.store.state.stagedRunAttachment) {
      const proof = inspectWarmRunTransition({ controlPlaneState: core.store.state, runnerState: state,
        expectedNewIdentity: target, expectedRunnerVersion: artifact.version, expectedRunnerDigest: artifact.digest });
      if (proof?.runnerIdentity.runId === target.runId) { physicalHeld = true; entered.release(); await held.promise; }
    }
    return state;
  };
  const options = {
    runnerBinary: binary, codexCommand: resolve(import.meta.dirname, "../../runner/target/debug/fake-codex-app-server"),
    codexArgs: ["--state-file", join(root, "provider.json"), "--call-log", join(root, "provider-calls.log")],
    sourceCodexHome: "", stateDirectory: originalRoot, prpIdentity: source,
    closeGraceMs: 300, runnerReconnectGraceMs: 5_000,
    lifecyclePolicy: { mode: "warm" as const, idleTimeoutMs: 60_000 },
    onStagedRunAttachment: async () => {}, onRecoveryAccountingEvent: async () => {}, readRunnerState,
    controlPlaneRegistration: async (authority: DurablePrpControlPlane, identity = authority.store.state.identity) => {
      core = authority; cores.add(core);
      if (!registered.has(core)) {
        registered.add(core);
        const epoch = generation;
        core.setCheckpointPersistence(async saved => {
          if (epoch !== generation) throw new Error("retired fixture writer");
          checkpoint = saved;
          if (!checkpointHeld && inspectStagedRunAttachmentHeader(saved.records.get("header")!)?.phase === "attachment_queued") {
            checkpointHeld = true; queued.release(); await queuedHold.promise;
            if (epoch !== generation) throw new Error("retired fixture writer");
          }
        });
      }
      const isTarget = identity.runId === target.runId, phase = recovering ? "recovery" : "original";
      const name = `${phase}:${isTarget ? "target" : "source"}`;
      const path = `/api/runner/v1/connect/${identity.runId}`;
      let stopped = false, peer: Awaited<ReturnType<typeof openOwnedWire>> | undefined;
      const close = () => { stopped = true; peer?.close(); }; routeClosers.push(close);
      let resolveReady!: () => void, rejectReady!: (error: Error) => void;
      const ready = new Promise<void>((done, fail) => { resolveReady = done; rejectReady = fail; });
      void ready.catch(() => undefined);
      const connect = async () => {
        const deadline = Date.now() + 6_000;
        while (!stopped && Date.now() < deadline) {
          try {
            peer = await openOwnedWire(`ws://127.0.0.1:${port}${path}`);
            if (stopped) { peer.close(); return; }
            authority.attachWireConnection(peer.wire); resolveReady(); await peer.closed;
          } catch { /* The real Runner owns the listener transition. */ }
          if (!stopped) await new Promise<void>(done => setTimeout(done, 50));
        }
        if (!stopped) rejectReady(new Error("Owned recovery listener never became ready"));
      };
      return { connection: { mode: "listen" as const, listenAddress: "0.0.0.0", listenPort: port, listenPath: path },
        activate: () => { routeEvents.push(`${name}:activate`); connections.push(connect()); }, ready: () => ready,
        release: async (reason?: "detach") => {
          routeEvents.push(`${name}:release:${reason ?? "normal"}`);
          if (phase === "recovery" && !isTarget && mode === "prior-release") { entered.release(); await held.promise; }
          close();
        },
      };
    },
    runnerProcessLauncher: (spec: Parameters<NonNullable<Parameters<typeof createCapabilityRunnerdCodexTransport>[0]["runnerProcessLauncher"]>>[0]) => {
      const child = spawn(spec.command, [...spec.args], { cwd: spec.cwd, env: spec.environment, stdio: ["ignore", "ignore", "pipe"] });
      child.stderr.on("data", bytes => { runnerStderr += bytes.toString(); });
      const completion = new Promise<RunnerProcessResult>((done, fail) => {
        child.once("error", fail); child.once("exit", (code, signal) => done({ code, signal, stdout: "", stderr: "" }));
      });
      const handle = { child, completion }; handles.push(handle); return handle;
    },
  };
  const first = createCapabilityRunnerdCodexTransport(options);
  let replacement: ReturnType<typeof createCapabilityRunnerdCodexTransport> | undefined;
  let attach: Promise<void> | undefined, recovery: Promise<unknown> | undefined, detaching: Promise<void> | undefined;
  let providerPid: number | null = null;
  try {
    const opened = await first.transport.request("thread/start", { cwd: root });
    providerPid = first.evidence().codexPid;
    const runnerPid = handles[0]!.child.pid!;
    attach = first.transport.attachRun!({ runId: target.runId, turnId: target.turnId, itemId: target.itemId });
    void attach.catch(() => undefined);
    await queued.promise;
    expect(inspectStagedRunAttachmentHeader(checkpoint.records.get("header")!)?.phase).toBe("attachment_queued");
    // This fixture externally fences the old checkpoint writer to model lost
    // controller authority. Ignoring its detach rejection below is fixture-only;
    // production takeover still requires successful retirement proof.
    generation++;
    const firstDetach = first.detachControllerForRestart(); void firstDetach.catch(() => undefined);
    queuedHold.release(); await firstDetach.catch(() => undefined); await attach.catch(() => undefined);
    checkpoint.restoreSessionDirectory(recoveredRoot);
    recovering = true;
    replacement = createCapabilityRunnerdCodexTransport({ ...options, stateDirectory: recoveredRoot, prpIdentity: target,
      runnerStateDirectory: join(originalRoot, "runner"), runnerArtifact: artifact,
      warmTransitionRegistrationMode: "routed_same_endpoint",
      prepareExternalRunnerState: async () => { throw new Error("Live recovery must not replace Runner state"); },
      resumeProviderSession: { driverSessionId: String((opened.thread as { id: string }).id),
        providerSessionId: String((opened.thread as { sessionId: string }).sessionId) },
      adoptExistingRunner: { pid: runnerPid, processGroupId: null, startedAt: first.transport.processInfo().startedAt,
        isAlive: () => handles[0]!.child.exitCode === null },
      runnerProcessLauncher: () => { throw new Error("Unexpected duplicate Runner launch"); },
    });
    recovery = replacement.transport.request("thread/read", {}); void recovery.catch(() => undefined);
    await entered.promise;
    expect(routeEvents).not.toContain("recovery:target:activate");
    detaching = replacement.detachControllerForRestart();
    await expect(detaching).rejects.toThrow(/startup drain timed out/);
    held.release();
    await expect(recovery).rejects.toThrow(/closed/);
    expect(routeEvents).not.toContain("recovery:target:activate");
    expect(routeEvents).toContain("recovery:target:release:detach");
    expect(handles).toHaveLength(1);
  } catch (error) {
    throw new Error(`${String(error)}; Runner stderr: ${runnerStderr}; routes: ${JSON.stringify(routeEvents)}`, { cause: error });
  } finally {
    queuedHold.release(); held.release();
    await attach?.catch(() => undefined); await recovery?.catch(() => undefined); await detaching?.catch(() => undefined);
    await replacement?.detachControllerForRestart().catch(() => undefined);
    await first.detachControllerForRestart().catch(() => undefined);
    for (const close of routeClosers) close();
    for (const handle of handles) {
      if (handle.child.exitCode === null && handle.child.signalCode === null) handle.child.kill("SIGKILL");
      await handle.completion.catch(() => undefined);
    }
    if (providerPid) { try { process.kill(providerPid, "SIGKILL"); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; } }
    await Promise.allSettled(connections);
    for (const authority of cores) await authority.stop().catch(() => undefined);
    await rm(root, { recursive: true, force: true });
  }
}, 20_000);
