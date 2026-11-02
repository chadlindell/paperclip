import { accessSync, constants, readFileSync } from "node:fs";
import { dirname, isAbsolute, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { CommandManagedRuntimeRunner } from "@paperclipai/adapter-utils/command-managed-runtime";

import { defaultCapabilityRunnerdBinary, runnerBinaryTarget } from "../../vendor/paperclip-runner/index.js";

// Retain this module path for installed probes and current execution adapters.
const moduleDirectory = dirname(fileURLToPath(import.meta.url));

function executableName(): string {
  return process.platform === "win32" ? "paperclip-runnerd.exe" : "paperclip-runnerd";
}

export function resolvePaperclipRunnerBinary(
  configuredPath = process.env.PAPERCLIP_RUNNER_BINARY,
): string {
  if (configuredPath && !isAbsolute(configuredPath)) {
    throw new Error("PAPERCLIP_RUNNER_BINARY must be an absolute path");
  }
  if (configuredPath) {
    try {
      accessSync(configuredPath, constants.R_OK | (process.platform === "win32" ? 0 : constants.X_OK));
      return configuredPath;
    } catch {
      // Preserve the existing fallback for an unavailable configured path.
    }
  }
  const candidates = [
    // The runner owns its compiled/vendored root and verifies the target header.
    // Never prefer a flat build-host executable over a staged release target.
    defaultCapabilityRunnerdBinary(),
    resolve(
      moduleDirectory,
      "../../../../packages/paperclip-runner/runner/target/release",
      executableName(),
    ),
  ].filter((candidate): candidate is string => Boolean(candidate));
  for (const candidate of candidates) {
    try {
      accessSync(candidate, constants.R_OK | (process.platform === "win32" ? 0 : constants.X_OK));
      return candidate;
    } catch {
      // Continue through the fixed production and workspace locations.
    }
  }
  throw new Error(
    "paperclip_runner_binary_missing: build @paperclipai/paperclip-runner or set PAPERCLIP_RUNNER_BINARY",
  );
}

export async function resolveRemotePaperclipRunnerBinary(input: {
  runner: CommandManagedRuntimeRunner;
  remoteCwd: string;
  configuredPath?: string | null;
}): Promise<string> {
  const probe = await input.runner.execute({ command: "sh", args: ["-c", "uname -s; uname -m"],
    cwd: input.remoteCwd, bypassSession: true, timeoutMs: 10_000 });
  const [os, arch] = probe.stdout.trim().split(/\r?\n/);
  const platform = os === "Linux" ? "linux" : os === "Darwin" ? "darwin" : null;
  const architecture = arch === "x86_64" ? "x64" : arch === "aarch64" || arch === "arm64" ? "arm64" : null;
  if (probe.exitCode !== 0 || probe.timedOut || !platform || !architecture) {
    throw new Error("runner_remote_artifact_platform_mismatch: could not identify a supported remote OS and architecture; check the selected execution environment and retry");
  }
  const configured = input.configuredPath?.trim();
  if (configured && !isAbsolute(configured)) throw new Error("PAPERCLIP_RUNNER_REMOTE_BINARY_PATH must be an absolute path");
  const controllerOverride = platform === process.platform && architecture === process.arch
    ? process.env.PAPERCLIP_RUNNER_BINARY : undefined;
  const binary = configured || (controllerOverride
    ? resolvePaperclipRunnerBinary(controllerOverride)
    : defaultCapabilityRunnerdBinary(platform, architecture));
  let bytes: Buffer;
  try { bytes = readFileSync(binary); }
  catch (cause) { throw new Error(`runner_remote_artifact_unavailable: install the Paperclip release containing the ${platform}-${architecture} daemon or configure PAPERCLIP_RUNNER_REMOTE_BINARY_PATH`, { cause }); }
  if (runnerBinaryTarget(bytes) !== `${platform}-${architecture}`) {
    throw new Error(`runner_remote_artifact_platform_mismatch: the selected daemon does not match ${platform}-${architecture}; install the matching Paperclip release or configure PAPERCLIP_RUNNER_REMOTE_BINARY_PATH`);
  }
  return binary;
}
