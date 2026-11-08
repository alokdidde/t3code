import { describe, expect, it } from "@effect/vitest";

import {
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestratorMcpThreadReadResult,
  type OrchestratorMcpThreadTimelineItem,
  type ServerProvider,
} from "@t3tools/contracts";

import { agentUpdated, defaultHarness, harnessFromProvider, type TrackedAgent } from "./wire.ts";

const at = "2026-11-07T00:00:00.000Z";
const agent: TrackedAgent = {
  threadId: "t1",
  name: "Make the wheels bigger",
  parts: ["part:rover/wheel-l"],
  commentId: "cmt:1",
  chat: false,
  replies: 0,
  stopRequested: false,
};

const read = (
  status: OrchestratorMcpThreadReadResult["thread"]["status"],
  runs = 1,
): OrchestratorMcpThreadReadResult => ({
  thread: {
    threadId: ThreadId.make("t1"),
    projectId: ProjectId.make("p1"),
    title: "Make the wheels bigger",
    createdBy: "agent",
    creationSource: "mcp",
    status,
    latestRunId: null,
    activeRunId: null,
    providerInstanceId: ProviderInstanceId.make("raven"),
    model: "default",
    runtimeMode: "auto-accept-edits",
    interactionMode: "default",
    linkedPullRequest: null,
    titleRegeneration: null,
    branch: "fulmetry/cmt-1",
    worktreePath: "/w/1",
    parentThreadId: null,
    relationshipToParent: null,
    runCount: runs,
    itemCount: 0,
    pendingRequestCount: 0,
    archived: false,
    settled: false,
    settledAt: null,
    snoozed: false,
    snoozedUntil: null,
    createdAt: at,
    updatedAt: at,
  },
  recentRuns: Array.from({ length: runs }, (_, i) => ({
    runId: RunId.make(`r${i}`),
    ordinal: i + 1,
    status: "running" as const,
    providerInstanceId: ProviderInstanceId.make("raven"),
    model: "default",
    requestedAt: at,
    startedAt: at,
    completedAt: null,
  })),
  items: [],
  nextPosition: null,
  hasMore: false,
});

const item = (
  type: string,
  text: string | null,
  title: string | null = null,
): OrchestratorMcpThreadTimelineItem => ({
  position: 0,
  visibility: "local",
  sourceThreadId: ThreadId.make("t1"),
  itemId: TurnItemId.make(`i-${type}`),
  runId: null,
  messageId: null,
  createdBy: null,
  creationSource: null,
  type,
  status: "completed",
  title,
  text,
  textTruncated: false,
  updatedAt: at,
});

const provider = (id: string, overrides: Partial<ServerProvider> = {}): ServerProvider => ({
  instanceId: ProviderInstanceId.make(id),
  driver: ProviderDriverKind.make(id),
  status: "ready",
  enabled: true,
  installed: true,
  auth: { status: "authenticated" },
  checkedAt: at,
  version: "1.0.0",
  models: [],
  slashCommands: [],
  skills: [],
  ...overrides,
});

describe("agentUpdated", () => {
  it.each([
    ["running", false, false, "working"],
    ["running", true, false, "waiting"],
    ["waiting", false, false, "waiting"],
    ["completed", false, false, "done"],
    ["interrupted", false, true, "stopped"],
    ["interrupted", false, false, "failed"],
    ["failed", false, false, "failed"],
  ] as const)(
    "thread %s (approval %s, stop asked %s) reads as %s",
    (status, approval, stop, state) => {
      const update = agentUpdated(
        { ...agent, stopRequested: stop },
        read(status),
        [],
        approval ? { requestId: "req-1", title: "Run pytest?" } : undefined,
      );
      expect(update.state).toBe(state);
      expect(update.pendingApproval !== undefined).toBe(state === "waiting" && approval);
    },
  );

  it("an idle thread that never ran is starting, not done", () => {
    expect(agentUpdated(agent, read("idle", 0), [], undefined).state).toBe("starting");
    expect(agentUpdated(agent, read("idle", 1), [], undefined).state).toBe("done");
  });

  it("shows the latest step and two lines of the latest reply", () => {
    const update = agentUpdated(
      agent,
      read("running"),
      [
        item("user_message", "make the wheels bigger"),
        item("assistant_message", "Widened the wheels.\n\nThey are 70 mm now.\nChecks pass."),
        item("file_change", null, "Edit wheel.py"),
      ],
      undefined,
    );
    expect(update.step).toBe("Edit wheel.py");
    expect(update.preview).toBe("Widened the wheels. They are 70 mm now.");
    expect(update.worktreePath).toBe("/w/1");
  });
});

describe("harnessFromProvider", () => {
  it("orders not installed, then signed out, before ready", () => {
    expect(
      harnessFromProvider(provider("codex", { installed: false }), undefined, undefined).status,
    ).toBe("needsInstall");
    expect(
      harnessFromProvider(
        provider("codex", { auth: { status: "unauthenticated" } }),
        undefined,
        undefined,
      ).status,
    ).toBe("needsSignIn");
    expect(
      harnessFromProvider(provider("codex", { enabled: false }), undefined, undefined).status,
    ).toBe("disabled");
    expect(
      harnessFromProvider(
        provider("pi", { status: "error", unavailableReason: "Pi 0.31 is too old" }),
        undefined,
        undefined,
      ),
    ).toMatchObject({ status: "unavailable", message: "Pi 0.31 is too old" });
  });

  it("a running sign-in shows its prompt and reads as checking", () => {
    const harness = harnessFromProvider(
      provider("codex", { auth: { status: "unauthenticated" } }),
      undefined,
      {
        instanceId: ProviderInstanceId.make("codex"),
        phase: "waiting",
        flowId: "flow-1",
        authorizationUrl: null,
        expiresAt: null,
        message: null,
        interaction: {
          type: "deviceCode",
          id: "i1",
          url: "https://example.test/device",
          userCode: "AB-12",
        },
      },
    );
    expect(harness.status).toBe("checking");
    expect(harness.setup).toMatchObject({
      step: "signingIn",
      prompt: "deviceCode",
      userCode: "AB-12",
    });
  });
});

describe("defaultHarness", () => {
  const ready = (id: string) => harnessFromProvider(provider(id), undefined, undefined);
  const signedOut = (id: string) =>
    harnessFromProvider(
      provider(id, { auth: { status: "unauthenticated" } }),
      undefined,
      undefined,
    );

  it("prefers Raven, then Claude Code, then any ready harness", () => {
    expect(defaultHarness([ready("codex"), ready("claudeAgent"), ready("raven")])?.id).toBe(
      "raven",
    );
    expect(defaultHarness([ready("codex"), ready("claudeAgent"), signedOut("raven")])?.id).toBe(
      "claudeAgent",
    );
    expect(defaultHarness([signedOut("claudeAgent"), ready("codex")])?.id).toBe("codex");
    expect(defaultHarness([signedOut("raven")])).toBeUndefined();
  });
});
