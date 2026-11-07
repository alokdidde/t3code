/**
 * The Fulmetry desktop app's view of T3 Code: what `t3 fulmetry` sends it over stdio, built from T3's own
 * thread and provider state. Pure, so the bridge's I/O stays in `cli/fulmetry.ts`.
 * The wire is Fulmetry's `contracts/agent/fulmetry-ext.schema.json` (events) plus its `harness.*` methods.
 */
import type {
  OrchestratorMcpThreadReadResult,
  OrchestratorMcpThreadTimelineItem,
  ProviderAuthState,
  ProviderInstallState,
  ServerProvider,
} from "@t3tools/contracts";

export const FULMETRY_EXT_VERSION = 2;
/** Fulmetry's default harness, then its fallback. */
const DEFAULT_HARNESS_ORDER = ["raven", "claudeAgent"] as const;
export const PROJECT_CHAT_TITLE = "Fulmetry project chat";

export type AgentWireState = "starting" | "working" | "waiting" | "stopped" | "done" | "failed";

export interface AgentUpdatedEvent {
  readonly type: "fulmetry.agentUpdated";
  readonly agentId: string;
  readonly name: string;
  readonly state: AgentWireState;
  readonly step: string;
  readonly preview: string;
  readonly parts: ReadonlyArray<string>;
  readonly replies: number;
  readonly commentId?: string;
  readonly pendingApproval?: { readonly requestId: string; readonly title: string };
  readonly provider: string;
  readonly model: string;
  readonly branch?: string;
  readonly worktreePath?: string;
}

/** What the bridge remembers about an agent between reads of its thread. */
export interface TrackedAgent {
  readonly threadId: string;
  readonly name: string;
  readonly parts: ReadonlyArray<string>;
  readonly commentId?: string;
  readonly chat: boolean;
  /** Messages the person sent after the first one. */
  readonly replies: number;
  /** Set by stopAgent, so the interrupted run reads as stopped and not failed. */
  readonly stopRequested: boolean;
}

const STEP_BY_ITEM_TYPE: Record<string, string> = {
  reasoning: "Thinking",
  command_execution: "Running a command",
  file_change: "Editing files",
  mcp_tool_call: "Using a tool",
  dynamic_tool_call: "Using a tool",
  web_search: "Searching the web",
  plan: "Planning",
  assistant_message: "Writing a reply",
  approval_request: "Waiting for approval",
};

const lastAssistantText = (items: ReadonlyArray<OrchestratorMcpThreadTimelineItem>) =>
  items.findLast((item) => item.type === "assistant_message" && (item.text ?? "") !== "")?.text ??
  "";

/** Two lines of the latest reply, as the agent card shows it. */
const previewOf = (text: string) => {
  const lines = text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line !== "");
  const preview = lines.slice(0, 2).join(" ");
  return preview.length > 240 ? `${preview.slice(0, 239)}…` : preview;
};

const agentState = (
  read: OrchestratorMcpThreadReadResult,
  agent: TrackedAgent,
  pendingApproval: boolean,
): AgentWireState => {
  switch (read.thread.status) {
    case "idle":
      return read.recentRuns.length === 0 ? "starting" : "done";
    case "completed":
      return "done";
    case "interrupted":
    case "cancelled":
      return agent.stopRequested ? "stopped" : "failed";
    case "failed":
    case "rolled_back":
      return "failed";
    default:
      return pendingApproval || read.thread.status === "waiting" ? "waiting" : "working";
  }
};

export const agentUpdated = (
  agent: TrackedAgent,
  read: OrchestratorMcpThreadReadResult,
  items: ReadonlyArray<OrchestratorMcpThreadTimelineItem>,
  pendingApproval: { readonly requestId: string; readonly title: string } | undefined,
): AgentUpdatedEvent => {
  const state = agentState(read, agent, pendingApproval !== undefined);
  const latest = items.findLast((item) => item.type !== "user_message");
  const step =
    state === "done"
      ? "Done"
      : state === "stopped"
        ? "Stopped"
        : state === "failed"
          ? "Failed"
          : state === "waiting"
            ? (pendingApproval?.title ?? "Waiting for an answer")
            : state === "starting"
              ? "Starting"
              : (latest?.title ?? STEP_BY_ITEM_TYPE[latest?.type ?? ""] ?? "Working");
  return {
    type: "fulmetry.agentUpdated",
    agentId: agent.threadId,
    name: agent.name,
    state,
    step,
    preview: previewOf(lastAssistantText(items)),
    parts: agent.parts,
    replies: agent.replies,
    ...(agent.commentId === undefined ? {} : { commentId: agent.commentId }),
    ...(pendingApproval === undefined || state !== "waiting" ? {} : { pendingApproval }),
    provider: read.thread.providerInstanceId,
    model: read.thread.model,
    ...(read.thread.branch === null ? {} : { branch: read.thread.branch }),
    ...(read.thread.worktreePath === null ? {} : { worktreePath: read.thread.worktreePath }),
  };
};

/** A comment is worked on while its agent runs, resolved when it is done, and open again otherwise. */
export const commentStatus = (state: AgentWireState) =>
  state === "done" ? "resolved" : state === "failed" || state === "stopped" ? "open" : "working";

/** Names an agent after its task in a few words, as the app's fake does: "make the wheels bigger" -> "Make the wheels bigger". */
export const nameFor = (text: string) => {
  const words = text.trim().split(/\s+/).slice(0, 4).join(" ");
  const trimmed = words.replace(/[\p{P}]+$/u, "");
  return trimmed === "" ? "Agent" : trimmed[0]!.toUpperCase() + trimmed.slice(1);
};

export interface ProjectContext {
  readonly projectRoot: string;
  readonly projectTitle: string;
  readonly chatThreadId: string | null;
}

const toolsNote =
  "Fulmetry agents share T3 Code's thread tools: t3_thread_list finds the project's other agents, t3_thread_read reads one, t3_thread_send messages one.";

export const chatPrompt = (
  context: ProjectContext,
  message: {
    readonly text: string;
    readonly scope: string;
    readonly objectId?: string;
    readonly partId?: string;
  },
  first: boolean,
) => {
  const about = [
    message.scope !== "project" ? `scope: ${message.scope}` : null,
    message.objectId === undefined ? null : `object: ${message.objectId}`,
    message.partId === undefined ? null : `part: ${message.partId}`,
  ].filter((line) => line !== null);
  const preamble = first
    ? [
        `You are the project chat of the Fulmetry project "${context.projectTitle}" at ${context.projectRoot}.`,
        "The person talks to you from Fulmetry's chat panel. Answer briefly. Every comment they pin on a part starts its own agent in its own thread and worktree.",
        `${toolsNote} Read those agents' threads when the person asks about their work.`,
        "",
      ]
    : [];
  return [...preamble, ...(about.length === 0 ? [] : [`(${about.join(", ")})`]), message.text].join(
    "\n",
  );
};

export const commentPrompt = (
  context: ProjectContext,
  comment: {
    readonly text: string;
    readonly commentId: string;
    readonly objectId?: string;
    readonly partId?: string;
  },
) =>
  [
    `A comment in the Fulmetry project "${context.projectTitle}" started you. You work in your own worktree of ${context.projectRoot}.`,
    [
      `comment: ${comment.commentId}`,
      comment.objectId === undefined ? null : `object: ${comment.objectId}`,
      comment.partId === undefined ? null : `part: ${comment.partId}`,
    ]
      .filter((line) => line !== null)
      .join(", "),
    context.chatThreadId === null
      ? toolsNote
      : `${toolsNote} The project chat is thread ${context.chatThreadId}; read it for context, and message it only with something the chat should know.`,
    "Make the change the comment asks for, then reply in one or two sentences with what you changed.",
    "",
    comment.text,
  ].join("\n");

export type HarnessStatus =
  | "ready"
  | "needsInstall"
  | "needsSignIn"
  | "checking"
  | "disabled"
  | "unavailable";

export interface HarnessSetupWire {
  readonly step:
    | "idle"
    | "installing"
    | "signingIn"
    | "verifying"
    | "succeeded"
    | "failed"
    | "cancelled";
  readonly fraction: number;
  readonly prompt: "none" | "browser" | "deviceCode" | "credentials" | "terminal";
  readonly url: string;
  readonly userCode: string;
  readonly fields: ReadonlyArray<{
    readonly name: string;
    readonly label: string;
    readonly secret: boolean;
  }>;
  readonly output: string;
  readonly message: string;
}

export interface HarnessWire {
  readonly id: string;
  readonly driver: string;
  readonly name: string;
  readonly maker: string;
  readonly accent: string;
  readonly status: HarnessStatus;
  readonly version: string;
  readonly account: string;
  readonly canInstall: boolean;
  readonly canSignIn: boolean;
  readonly fromRegistry: boolean;
  readonly docsUrl: string;
  readonly message: string;
  readonly models: ReadonlyArray<{ readonly id: string; readonly name: string }>;
  readonly setup: HarnessSetupWire;
}

const MAKERS: Record<string, string> = {
  claudeAgent: "Anthropic",
  codex: "OpenAI",
  cursor: "Anysphere",
  opencode: "SST",
  antigravity: "Google",
  grok: "xAI",
  kiro: "AWS",
  pi: "Mario Zechner",
};

const IDLE_SETUP: HarnessSetupWire = {
  step: "idle",
  fraction: -1,
  prompt: "none",
  url: "",
  userCode: "",
  fields: [],
  output: "",
  message: "",
};

const installSetup = (install: ProviderInstallState): HarnessSetupWire | null => {
  switch (install.phase) {
    case "downloading":
    case "extracting":
    case "verifying":
      return {
        ...IDLE_SETUP,
        step: "installing",
        fraction:
          install.totalBytes === null || install.totalBytes === 0
            ? -1
            : Math.min(1, install.downloadedBytes / install.totalBytes),
        message:
          install.message ??
          (install.phase === "downloading" ? "Downloading" : "Verifying the download"),
      };
    case "failed":
      return { ...IDLE_SETUP, step: "failed", message: install.message ?? "The install failed" };
    case "cancelled":
      return { ...IDLE_SETUP, step: "cancelled" };
    case "succeeded":
      return { ...IDLE_SETUP, step: "succeeded" };
    default:
      return null;
  }
};

const authSetup = (auth: ProviderAuthState): HarnessSetupWire | null => {
  const message = auth.message ?? "";
  switch (auth.phase) {
    case "starting":
      return { ...IDLE_SETUP, step: "signingIn", message: message || "Starting the sign-in" };
    case "waiting": {
      const interaction = auth.interaction ?? null;
      if (interaction === null) {
        return auth.authorizationUrl === null
          ? { ...IDLE_SETUP, step: "signingIn", message }
          : {
              ...IDLE_SETUP,
              step: "signingIn",
              prompt: "browser",
              url: auth.authorizationUrl,
              message,
            };
      }
      switch (interaction.type) {
        case "browser":
          return {
            ...IDLE_SETUP,
            step: "signingIn",
            prompt: "browser",
            url: interaction.url,
            message,
          };
        case "deviceCode":
          return {
            ...IDLE_SETUP,
            step: "signingIn",
            prompt: "deviceCode",
            url: interaction.url,
            userCode: interaction.userCode,
            message,
          };
        case "credentials":
          return {
            ...IDLE_SETUP,
            step: "signingIn",
            prompt: "credentials",
            fields: interaction.fields,
            message,
          };
        case "terminal":
          return {
            ...IDLE_SETUP,
            step: "signingIn",
            prompt: "terminal",
            output: interaction.output,
            message,
          };
      }
      return { ...IDLE_SETUP, step: "signingIn", message };
    }
    case "verifying":
      return { ...IDLE_SETUP, step: "verifying", message: message || "Checking the sign-in" };
    case "failed":
      return { ...IDLE_SETUP, step: "failed", message: message || "The sign-in failed" };
    case "cancelled":
      return { ...IDLE_SETUP, step: "cancelled" };
    case "succeeded":
      return { ...IDLE_SETUP, step: "succeeded" };
    default:
      return null;
  }
};

export const harnessFromProvider = (
  provider: ServerProvider,
  install: ProviderInstallState | undefined,
  auth: ProviderAuthState | undefined,
): HarnessWire => {
  const installing = install === undefined ? null : installSetup(install);
  const signingIn = auth === undefined ? null : authSetup(auth);
  // A running install or sign-in wins over a finished one.
  const active = [installing, signingIn].find(
    (s) =>
      s !== null && (s.step === "installing" || s.step === "signingIn" || s.step === "verifying"),
  );
  const setup = active ?? signingIn ?? installing ?? IDLE_SETUP;
  const canSignIn = provider.setup?.canAuthenticate ?? false;
  const status: HarnessStatus =
    active !== undefined
      ? "checking"
      : !provider.enabled
        ? "disabled"
        : !provider.installed
          ? "needsInstall"
          : provider.availability === "unavailable" || provider.status === "error"
            ? provider.auth.status === "unauthenticated"
              ? "needsSignIn"
              : "unavailable"
            : provider.auth.status === "unauthenticated"
              ? "needsSignIn"
              : "ready";
  return {
    id: provider.instanceId,
    driver: provider.driver,
    name: provider.displayName ?? provider.instanceId,
    maker:
      provider.instanceId === "raven"
        ? "EverMind"
        : (MAKERS[provider.driver] ?? (provider.driver === "acpRegistry" ? "ACP registry" : "")),
    accent: provider.accentColor ?? "",
    status,
    version: provider.installedVersion ?? provider.version ?? "",
    account: provider.auth.email ?? provider.auth.label ?? "",
    canInstall: provider.setup?.canInstall ?? false,
    canSignIn,
    fromRegistry: provider.driver === "acpRegistry" && provider.instanceId !== "raven",
    docsUrl: provider.setup?.documentationUrl ?? "",
    message: status === "ready" ? "" : (provider.unavailableReason ?? provider.message ?? ""),
    models: provider.models
      .filter((model) => model.isLegacy !== true)
      .toSorted((left, right) => Number(right.isDefault === true) - Number(left.isDefault === true))
      .map((model) => ({ id: model.slug, name: model.name })),
    setup,
  };
};

/** The harness an agent runs on when the app names none: Raven, then Claude Code, then any ready one. */
export const defaultHarness = (harnesses: ReadonlyArray<HarnessWire>) => {
  const ready = harnesses.filter((harness) => harness.status === "ready");
  for (const id of DEFAULT_HARNESS_ORDER) {
    const found = ready.find((harness) => harness.id === id);
    if (found !== undefined) return found;
  }
  return ready[0];
};

/** Raven, as an ACP agent T3 Code starts with `raven acp`: added once, when the person has none. */
export const RAVEN_INSTANCE = {
  driver: "acpRegistry",
  displayName: "Raven",
  accentColor: "#7A5AF8",
  enabled: true,
  config: { source: "local", commandPath: "raven", commandArgs: ["acp"] },
} as const;
