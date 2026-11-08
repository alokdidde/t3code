/**
 * `t3 fulmetry`: the Fulmetry app's way into T3 Code, newline-delimited JSON on stdio: `{id, method, params}`
 * answered by `{id, result}` or `{id, error}`, plus `{event}` pushes. Threads use T3's MCP thread tools;
 * harness setup and approvals use the WebSocket RPC. Starts a server when none runs, stopped at end of stdin.
 */
import {
  CommandId,
  OrchestratorMcpThreadReadResult,
  OrchestratorMcpThreadSendResult,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeRequestId,
  ThreadId,
  WsRpcGroup,
  type OrchestratorMcpThreadTimelineItem,
  type ProviderAuthState,
  type ProviderInstallState,
  type ServerProvider,
  type ServerSettings,
} from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as References from "effect/References";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import * as SecureRandom from "effect/crypto/SecureRandom";
import { Command, GlobalFlag } from "effect/cli";
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { RpcClient, RpcSerialization } from "effect/rpc";
import * as Socket from "effect/socket/Socket";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerConfig from "../config.ts";
import {
  agentUpdated,
  chatPrompt,
  commentPrompt,
  commentStatus,
  defaultHarness,
  FULMETRY_EXT_VERSION,
  harnessFromProvider,
  nameFor,
  PROJECT_CHAT_TITLE,
  RAVEN_INSTANCE,
  type AgentUpdatedEvent,
  type HarnessWire,
  type TrackedAgent,
} from "../fulmetry/wire.ts";
import { mcpToolSession } from "../mcp/AcpMcpStdioBridge.ts";
import { readPersistedServerRuntimeState } from "../serverRuntimeState.ts";
import { projectLocationFlags, resolveCliAuthConfig } from "./config.ts";
import { makeThreadTools, type ThreadTools } from "./thread.ts";

const POLL_INTERVAL = Duration.millis(750);
const SERVER_START_TIMEOUT = Duration.seconds(90);
const WINDOW = 24;

class BridgeError extends Schema.TaggedError<BridgeError>()("BridgeError", {
  code: Schema.String,
  detail: Schema.String,
}) {
  override get message(): string {
    return this.detail;
  }
}
const isBridgeError = Schema.is(BridgeError);
const fail = (code: string, detail: string) => Effect.fail(new BridgeError({ code, detail }));
/** Every failure reaches the app as `{code, message}`; the bridge's own codes pass through. */
const asBridgeError = (error: unknown) =>
  isBridgeError(error)
    ? error
    : new BridgeError({
        code: "failed",
        detail: error instanceof Error ? error.message : String(error),
      });

const Params = Schema.Record(Schema.String, Schema.Unknown);
type Params = typeof Params.Type;
const Request = Schema.Struct({
  id: Schema.Number,
  method: Schema.String,
  params: Schema.optional(Params),
});
const decodeRequest = Schema.decodeUnknownOption(Schema.fromJsonString(Request));
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const sameJson = (left: unknown, right: unknown) => encodeJson(left) === encodeJson(right);

const LaunchResult = Schema.Struct({ threadId: ThreadId, projectId: ProjectId });
const ProjectList = Schema.Struct({
  projects: Schema.Array(
    Schema.Struct({ id: ProjectId, title: Schema.String, workspaceRoot: Schema.String }),
  ),
});
const CreatedProject = Schema.Struct({ id: ProjectId });
const ThreadList = Schema.Struct({
  threads: Schema.Array(Schema.Struct({ threadId: ThreadId, title: Schema.String })),
});
const WebSocketTicket = Schema.Struct({ ticket: Schema.String });
const decodeStringRecord = Schema.decodeUnknownOption(Schema.Record(Schema.String, Schema.String));

const str = (params: Params, key: string): string | undefined => {
  const value = params[key];
  return typeof value === "string" && value.trim() !== "" ? value : undefined;
};
const required = (params: Params, key: string) => {
  const value = str(params, key);
  return value === undefined
    ? fail("invalid_argument", `${key} is required`)
    : Effect.succeed(value);
};
const optional = (params: Params, ...keys: ReadonlyArray<string>) =>
  Object.fromEntries(
    keys.flatMap((key) => {
      const value = str(params, key);
      return value === undefined ? [] : [[key, value]];
    }),
  );

const makeRpcClient = RpcClient.make(WsRpcGroup);
type Rpc = Effect.Success<typeof makeRpcClient>;

/** The running server for this base dir, or one this command starts and stops with its scope. */
const ensureServer = Effect.fn("cli.fulmetry.ensureServer")(function* (
  runtimeStatePath: string,
  baseDir: string,
) {
  const running = yield* readPersistedServerRuntimeState(runtimeStatePath);
  if (Option.isSome(running)) return running.value.origin;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const [entry] = process.argv.slice(1);
  if (entry === undefined)
    return yield* fail("server_unavailable", "cannot find the t3 entry point");
  yield* spawner.spawn(
    ChildProcess.make(
      process.execPath,
      [...process.execArgv, entry, "serve", "--no-browser", "--base-dir", baseDir],
      { stdin: "ignore", stdout: "ignore", stderr: "inherit", forceKillAfter: Duration.seconds(5) },
    ),
  );
  // The server writes its runtime state once it listens; nothing else says it is up.
  const state = yield* readPersistedServerRuntimeState(runtimeStatePath).pipe(
    Effect.flatMap((state) =>
      Option.isSome(state) ? Effect.succeed(state.value) : fail("server_unavailable", "starting"),
    ),
    Effect.retry(Schedule.spaced(Duration.millis(250))),
    Effect.timeoutOrElse({
      duration: SERVER_START_TIMEOUT,
      orElse: () => fail("server_unavailable", "T3 Code did not start"),
    }),
  );
  return state.origin;
});

const connectRpc = Effect.fn("cli.fulmetry.connectRpc")(function* (origin: string, token: string) {
  const httpClient = yield* HttpClient.HttpClient;
  const { ticket } = yield* HttpClientRequest.post(
    new URL("/api/auth/websocket-ticket/rpc", origin).href,
  ).pipe(
    HttpClientRequest.bearerToken(token),
    httpClient.execute,
    Effect.flatMap(HttpClientResponse.filterStatusOk),
    Effect.flatMap(HttpClientResponse.schemaBodyJson(WebSocketTicket)),
  );
  const socketUrl = new URL("/ws", origin.replace(/^http/, "ws"));
  socketUrl.searchParams.set("wsTicket", ticket);
  socketUrl.searchParams.set("orchestrationProtocol", "2");
  const protocol = yield* Layer.build(
    Layer.effect(RpcClient.Protocol, RpcClient.makeProtocolSocket()).pipe(
      Layer.provide(
        Layer.mergeAll(
          Socket.layerWebSocket(socketUrl.href).pipe(
            Layer.provide(Socket.layerWebSocketConstructorGlobal),
          ),
          RpcSerialization.layerJson,
        ),
      ),
    ),
  );
  return yield* makeRpcClient.pipe(Effect.provide(protocol));
});

const fulmetryCommand = Command.make("fulmetry", { ...projectLocationFlags }).pipe(
  Command.withDescription("Serve the Fulmetry desktop app over stdio (newline-delimited JSON)."),
  Command.withHandler((flags) =>
    Effect.gen(function* () {
      const logLevel = yield* GlobalFlag.LogLevel;
      const config = yield* resolveCliAuthConfig(flags, logLevel, { quietLogs: true });
      yield* Effect.gen(function* () {
        const origin = yield* ensureServer(config.serverRuntimeStatePath, config.baseDir);
        const environmentAuth = yield* EnvironmentAuth.EnvironmentAuth;
        const mcp = yield* Effect.acquireRelease(
          environmentAuth.issueMcpClientSession({
            label: "Fulmetry",
            access: "full-access",
            peerLink: false,
            client: { deviceType: "desktop" },
          }),
          (session) =>
            environmentAuth.revokeSession(session.sessionId).pipe(Effect.ignore({ log: true })),
        );
        const session = yield* mcpToolSession({
          endpoint: new URL("/mcp", origin).href,
          authorization: `Bearer ${mcp.token}`,
          clientName: "fulmetry",
        });
        const bearer = yield* Effect.acquireRelease(
          environmentAuth.issueSession({ label: "Fulmetry", subject: "fulmetry" }),
          (issued) =>
            environmentAuth.revokeSession(issued.sessionId).pipe(Effect.ignore({ log: true })),
        );
        const rpc = yield* connectRpc(origin, bearer.token);
        yield* serve(makeThreadTools(session, origin), rpc);
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(
            EnvironmentAuth.layerRuntime.pipe(
              Layer.provide(ServerConfig.layer(config)),
              Layer.provide(Layer.succeed(References.MinimumLogLevel, config.logLevel)),
            ),
            FetchHttpClient.layer,
          ),
        ),
      );
    }).pipe(
      // stdout carries only frames.
      Effect.provide(Logger.layer([Logger.withConsoleError(Logger.defaultLogger)])),
    ),
  ),
);

const serve = Effect.fn("cli.fulmetry.serve")(function* (tools: ThreadTools, rpc: Rpc) {
  const scope = yield* Effect.scope;
  const stdio = yield* Stdio.Stdio;
  const newId = (yield* SecureRandom.SecureRandom).uuidV4();
  const write = (frame: unknown) =>
    Stream.make(`${encodeJson(frame)}\n`).pipe(Stream.encodeText, Stream.run(stdio.stdout()));
  const emit = (event: unknown) => write({ event });

  let project: { readonly id: string; readonly root: string; readonly title: string } | null = null;
  let chatThreadId: string | null = null;
  const agents = new Map<string, TrackedAgent>();
  const lastSent = new Map<string, AgentUpdatedEvent>();
  const postedReplies = new Set<string>();
  const startedByCommand = new Map<string, unknown>();

  let providers: ReadonlyArray<ServerProvider> = [];
  let settings: ServerSettings | null = null;
  const installs = new Map<string, ProviderInstallState>();
  const auths = new Map<string, ProviderAuthState>();
  const harnessList = () =>
    providers.map((p) =>
      harnessFromProvider(p, installs.get(p.instanceId), auths.get(p.instanceId)),
    );
  let lastHarnesses: ReadonlyArray<HarnessWire> = [];
  const publishHarnesses = Effect.suspend(() => {
    const harnesses = harnessList();
    if (sameJson(harnesses, lastHarnesses)) return Effect.void;
    lastHarnesses = harnesses;
    return emit({ type: "fulmetry.harnessesUpdated", harnesses });
  });
  const harness = (id: string) => {
    const found = harnessList().find((h) => h.id === id);
    return found === undefined ? fail("not_found", `no harness ${id}`) : Effect.succeed(found);
  };

  yield* rpc.subscribeServerConfig({}).pipe(
    Stream.runForEach((event) =>
      Effect.suspend(() => {
        if (event.type === "snapshot") {
          providers = event.config.providers;
          settings = event.config.settings;
        } else if (event.type === "providerStatuses") {
          providers = event.payload.providers;
        } else if (event.type === "settingsUpdated") {
          settings = event.payload.settings;
        }
        return publishHarnesses;
      }),
    ),
    Effect.catchCause(() =>
      emit({ type: "fulmetry.banner", text: "Lost T3 Code's harness list. Restart Fulmetry." }),
    ),
    Effect.forkIn(scope),
  );

  // Raven is Fulmetry's default harness, run as `raven acp`; added once, when the person has none.
  const ensureRaven = Effect.suspend(() =>
    settings === null || Object.hasOwn(settings.providerInstances, "raven")
      ? Effect.void
      : rpc["server.updateSettings"]({
          patch: {},
          providerInstanceMutation: {
            operation: "create",
            instanceId: ProviderInstanceId.make("raven"),
            instance: {
              ...RAVEN_INSTANCE,
              driver: ProviderDriverKind.make(RAVEN_INSTANCE.driver),
              config: {
                ...RAVEN_INSTANCE.config,
                commandArgs: [...RAVEN_INSTANCE.config.commandArgs],
              },
            },
          },
        }),
  );

  const pickHarness = Effect.fn("cli.fulmetry.pickHarness")(function* (
    provider: string | undefined,
    model: string | undefined,
  ) {
    const chosen =
      provider === undefined ? defaultHarness(harnessList()) : yield* harness(provider);
    if (chosen === undefined) {
      return yield* fail("no_harness", "no harness is ready; connect one first");
    }
    if (chosen.status !== "ready") {
      return yield* fail("harness_not_ready", `${chosen.name} is not ready`);
    }
    const slug = model ?? chosen.models[0]?.id;
    if (slug === undefined) return yield* fail("not_found", `${chosen.name} offers no model`);
    return { instanceId: chosen.id, model: slug };
  });

  const requireProject = Effect.suspend(() =>
    project === null
      ? fail("not_subscribed", "call fulmetry.subscribe first")
      : Effect.succeed(project),
  );

  const track = (agent: TrackedAgent) =>
    Effect.sync(() => {
      agents.set(agent.threadId, agent);
      lastSent.delete(agent.threadId);
    });

  const chatReplies = (
    agent: TrackedAgent,
    provider: string,
    items: ReadonlyArray<OrchestratorMcpThreadTimelineItem>,
  ) =>
    Effect.forEach(
      items.filter(
        (item) =>
          item.type === "assistant_message" &&
          item.status === "completed" &&
          !postedReplies.has(item.itemId),
      ),
      (item) => {
        postedReplies.add(item.itemId);
        return emit({
          type: "fulmetry.chatPosted",
          author: harnessList().find((h) => h.id === provider)?.name ?? "Agent",
          text: item.text ?? "",
          agentId: agent.threadId,
        });
      },
      { discard: true },
    );

  /** One read of an agent's thread: its latest items, its status, and the approval it waits on. */
  const refresh = Effect.fn("cli.fulmetry.refresh")(function* (agent: TrackedAgent) {
    const head = yield* tools.call(
      "t3_thread_read",
      { threadId: agent.threadId, limit: 1, runLimit: 1 },
      OrchestratorMcpThreadReadResult,
    );
    const read = yield* tools.call(
      "t3_thread_read",
      {
        threadId: agent.threadId,
        view: "activity",
        limit: WINDOW,
        runLimit: 1,
        maxCharsPerItem: 4_000,
        ...(head.thread.itemCount > WINDOW
          ? { afterPosition: head.thread.itemCount - WINDOW - 1 }
          : {}),
      },
      OrchestratorMcpThreadReadResult,
    );
    let approval: { readonly requestId: string; readonly title: string } | undefined;
    if (read.thread.pendingRequestCount > 0) {
      const projection = yield* rpc["orchestration.getThreadProjection"]({
        threadId: ThreadId.make(agent.threadId),
      });
      const pending = projection.runtimeRequests.find(
        (request) => request.status === "pending" && request.kind !== "user_input",
      );
      const item = read.items.findLast((candidate) => candidate.type === "approval_request");
      if (pending !== undefined) {
        approval = {
          requestId: pending.id,
          title: item?.title ?? `Allow ${pending.kind.replaceAll("_", " ")}?`,
        };
      }
    }
    const update = agentUpdated(agent, read, read.items, approval);
    if (!sameJson(lastSent.get(agent.threadId), update)) {
      lastSent.set(agent.threadId, update);
      yield* emit(update);
      if (agent.commentId !== undefined) {
        const reply = read.items.findLast((item) => item.type === "assistant_message")?.text ?? "";
        const status = commentStatus(update.state);
        yield* emit({
          type: "fulmetry.commentUpdated",
          commentId: agent.commentId,
          status,
          agentId: agent.threadId,
          ...(status === "resolved" && reply !== "" ? { reply } : {}),
        });
      }
    }
    if (agent.chat) yield* chatReplies(agent, update.provider, read.items);
    // A finished comment agent needs no more reads until someone replies to it.
    if (!agent.chat && ["done", "failed", "stopped"].includes(update.state)) {
      agents.delete(agent.threadId);
    }
  });

  yield* Effect.suspend(() =>
    Effect.forEach(
      [...agents.values()],
      (agent) => refresh(agent).pipe(Effect.ignore({ log: true })),
      {
        discard: true,
      },
    ),
  ).pipe(Effect.andThen(Effect.sleep(POLL_INTERVAL)), Effect.forever, Effect.forkIn(scope));

  // Follows an install or sign-in until it ends, republishing the harness list on every step.
  const follow = <A, E>(stream: Stream.Stream<A, E>, onState: (state: A) => void) =>
    stream.pipe(
      Stream.runForEach((state) =>
        Effect.sync(() => onState(state)).pipe(Effect.andThen(publishHarnesses)),
      ),
      Effect.ignore({ log: true }),
      Effect.forkIn(scope),
      Effect.asVoid,
    );

  const launchComment = Effect.fn("cli.fulmetry.launchComment")(function* (
    input: Record<string, unknown>,
    workspaceStrategy: Record<string, unknown> | undefined,
  ) {
    return yield* tools.call(
      "t3_thread_launch",
      workspaceStrategy === undefined ? input : { ...input, workspaceStrategy },
      LaunchResult,
    );
  });

  const subscribe = Effect.fn("fulmetry.subscribe")(function* (params: Params) {
    const ext = params.fulmetryExt;
    if (typeof ext !== "number" || ext > FULMETRY_EXT_VERSION) {
      return yield* fail("unsupported", `this bridge speaks fulmetryExt ${FULMETRY_EXT_VERSION}`);
    }
    const root = yield* required(params, "projectRoot");
    const title =
      str(params, "projectTitle") ??
      root.split(/[\\/]/).findLast((segment) => segment !== "") ??
      "Fulmetry project";
    const listed = yield* tools.call("t3_project_list", { limit: 100 }, ProjectList);
    const existing = listed.projects.find((p) => p.workspaceRoot === root);
    const id =
      existing?.id ??
      (yield* tools.call("t3_project_create", { title, workspaceRoot: root }, CreatedProject)).id;
    project = { id, root, title: existing?.title ?? title };
    const chats = yield* tools.call(
      "t3_thread_list",
      { projectId: id, titleContains: PROJECT_CHAT_TITLE, limit: 1 },
      ThreadList,
    );
    chatThreadId = chats.threads[0]?.threadId ?? null;
    if (chatThreadId !== null) {
      // Replies from earlier sessions are history, not news.
      const history = yield* tools.call(
        "t3_thread_read",
        { threadId: chatThreadId, view: "messages", limit: 100, runLimit: 1 },
        OrchestratorMcpThreadReadResult,
      );
      for (const item of history.items) postedReplies.add(item.itemId);
      yield* track({
        threadId: chatThreadId,
        name: "Project chat",
        parts: [],
        chat: true,
        replies: 0,
        stopRequested: false,
      });
    }
    yield* ensureRaven.pipe(Effect.ignore({ log: true }));
    return {
      projectId: id,
      chatThreadId,
      defaultHarness: defaultHarness(harnessList())?.id ?? null,
    };
  });

  const sendChat = Effect.fn("fulmetry.sendChat")(function* (params: Params) {
    const commandId = str(params, "commandId") ?? (yield* newId);
    if (startedByCommand.has(commandId)) return startedByCommand.get(commandId);
    const p = yield* requireProject;
    const text = yield* required(params, "text");
    const selection = yield* pickHarness(str(params, "provider"), str(params, "model"));
    const message = {
      text,
      scope: str(params, "scope") ?? "project",
      ...optional(params, "objectId", "partId"),
    };
    const context = { projectRoot: p.root, projectTitle: p.title, chatThreadId };
    let agentId: string;
    if (chatThreadId === null) {
      const launched = yield* tools.call(
        "t3_thread_launch",
        {
          projectId: p.id,
          title: PROJECT_CHAT_TITLE,
          message: chatPrompt(context, message, true),
          modelSelection: selection,
          clientRequestId: commandId,
        },
        LaunchResult,
      );
      chatThreadId = launched.threadId;
      agentId = launched.threadId;
    } else {
      agentId = chatThreadId;
      yield* tools.call(
        "t3_thread_send",
        {
          threadId: chatThreadId,
          message: chatPrompt(context, message, false),
          mode: "queue",
          clientRequestId: commandId,
        },
        OrchestratorMcpThreadSendResult,
      );
    }
    yield* track({
      threadId: agentId,
      name: "Project chat",
      parts: [],
      chat: true,
      replies: (agents.get(agentId)?.replies ?? -1) + 1,
      stopRequested: false,
    });
    const result = { agentId };
    startedByCommand.set(commandId, result);
    return result;
  });

  const startAgentForComment = Effect.fn("fulmetry.startAgentForComment")(function* (
    params: Params,
  ) {
    const commandId = str(params, "commandId") ?? (yield* newId);
    if (startedByCommand.has(commandId)) return startedByCommand.get(commandId);
    const p = yield* requireProject;
    const text = yield* required(params, "text");
    const commentId = yield* required(params, "commentId");
    const selection = yield* pickHarness(str(params, "provider"), str(params, "model"));
    const anchor = params.anchor;
    const part =
      typeof anchor === "object" && anchor !== null && "part" in anchor ? anchor.part : undefined;
    const partId = typeof part === "string" && part !== "" ? part : undefined;
    const input = {
      projectId: p.id,
      title: nameFor(text),
      message: commentPrompt(
        { projectRoot: p.root, projectTitle: p.title, chatThreadId },
        {
          text,
          commentId,
          ...optional(params, "objectId"),
          ...(partId === undefined ? {} : { partId }),
        },
      ),
      modelSelection: selection,
      runtimeMode: "auto-accept-edits",
      clientRequestId: commandId,
    };
    // Each comment works in its own worktree; a folder that is not a git checkout gets the folder itself.
    const launched = yield* launchComment(input, {
      type: "worktree",
      baseRef: str(params, "baseRevision") ?? "HEAD",
      branch: `fulmetry/${commentId.replace(/[^A-Za-z0-9._-]+/g, "-")}-${commandId.slice(0, 6)}`,
      startFromOrigin: false,
    }).pipe(Effect.catch(() => launchComment(input, undefined)));
    yield* track({
      threadId: launched.threadId,
      name: nameFor(text),
      parts: partId === undefined ? [] : [partId],
      commentId,
      chat: false,
      replies: 0,
      stopRequested: false,
    });
    const result = { agentId: launched.threadId, threadId: launched.threadId };
    startedByCommand.set(commandId, result);
    return result;
  });

  const reply = Effect.fn("fulmetry.reply")(function* (params: Params) {
    const agentId = yield* required(params, "agentId");
    const text = yield* required(params, "text");
    yield* tools.call(
      "t3_thread_send",
      { threadId: agentId, message: text, mode: "queue" },
      OrchestratorMcpThreadSendResult,
    );
    const prior = agents.get(agentId);
    yield* track({
      threadId: agentId,
      name: prior?.name ?? nameFor(text),
      parts: prior?.parts ?? [],
      ...(prior?.commentId === undefined ? {} : { commentId: prior.commentId }),
      chat: agentId === chatThreadId,
      replies: (prior?.replies ?? 0) + 1,
      stopRequested: false,
    });
    return {};
  });

  const DECISIONS = ["accept", "acceptForSession", "decline", "cancel"] as const;
  const respondToApproval = Effect.fn("fulmetry.respondToApproval")(function* (params: Params) {
    const agentId = yield* required(params, "agentId");
    const requestId = yield* required(params, "requestId");
    const decision = DECISIONS.find((d) => d === (str(params, "decision") ?? "accept"));
    if (decision === undefined) return yield* fail("invalid_argument", "unknown decision");
    yield* rpc["orchestration.dispatchCommand"]({
      type: "runtime-request.respond",
      commandId: CommandId.make(yield* newId),
      threadId: ThreadId.make(agentId),
      requestId: RuntimeRequestId.make(requestId),
      decision,
    });
    return {};
  });

  const stopAgent = Effect.fn("fulmetry.stopAgent")(function* (params: Params) {
    const agentId = yield* required(params, "agentId");
    const agent = agents.get(agentId);
    if (agent !== undefined) agents.set(agentId, { ...agent, stopRequested: true });
    yield* tools.call("t3_thread_interrupt", { threadId: agentId, stop: true }, Schema.Unknown);
    return {};
  });

  const installHarness = Effect.fn("harness.install")(function* (params: Params) {
    const h = yield* harness(yield* required(params, "harness"));
    if (!h.canInstall) return yield* fail("unsupported", `${h.name} cannot be installed from here`);
    const instanceId = ProviderInstanceId.make(h.id);
    installs.set(h.id, yield* rpc["provider.install.start"]({ instanceId }));
    yield* publishHarnesses;
    yield* follow(rpc["provider.install.subscribe"]({ instanceId }), (state) =>
      installs.set(h.id, state),
    );
    return {};
  });

  const signInHarness = Effect.fn("harness.signIn")(function* (params: Params) {
    const h = yield* harness(yield* required(params, "harness"));
    if (!h.canSignIn) return yield* fail("unsupported", `${h.name} signs in on its own`);
    if (h.status === "needsInstall")
      return yield* fail("not_installed", `${h.name} is not installed`);
    const instanceId = ProviderInstanceId.make(h.id);
    auths.set(h.id, yield* rpc["provider.auth.start"]({ instanceId }));
    yield* publishHarnesses;
    yield* follow(rpc["provider.auth.subscribe"]({ instanceId }), (state) =>
      auths.set(h.id, state),
    );
    return {};
  });

  const answerSignIn = Effect.fn("harness.answerSignIn")(function* (params: Params) {
    const id = yield* required(params, "harness");
    const auth = auths.get(id);
    const interaction = auth?.interaction ?? null;
    if (auth === undefined || auth.flowId === null || interaction === null) {
      return yield* fail("not_found", "no sign-in in progress");
    }
    const values = decodeStringRecord(params.values);
    const response =
      interaction.type === "browser"
        ? {
            type: "browser" as const,
            action: params.accept === false ? ("decline" as const) : ("accept" as const),
          }
        : interaction.type === "credentials"
          ? { type: "credentials" as const, values: Option.getOrElse(values, () => ({})) }
          : interaction.type === "terminal"
            ? { type: "terminal" as const, data: str(params, "terminalInput") ?? "" }
            : null;
    if (response === null) return yield* fail("unsupported", "this sign-in finishes by itself");
    auths.set(
      id,
      yield* rpc["provider.auth.respond"]({
        instanceId: ProviderInstanceId.make(id),
        flowId: auth.flowId,
        interactionId: interaction.id,
        response,
      }),
    );
    yield* publishHarnesses;
    return {};
  });

  const cancelHarnessSetup = Effect.fn("harness.cancel")(function* (params: Params) {
    const id = yield* required(params, "harness");
    const instanceId = ProviderInstanceId.make(id);
    const flowId = auths.get(id)?.flowId ?? null;
    const operationId = installs.get(id)?.operationId ?? null;
    if (flowId !== null) auths.set(id, yield* rpc["provider.auth.cancel"]({ instanceId, flowId }));
    if (operationId !== null) {
      installs.set(id, yield* rpc["provider.install.cancel"]({ instanceId, operationId }));
    }
    yield* publishHarnesses;
    return {};
  });

  const signOutHarness = Effect.fn("harness.signOut")(function* (params: Params) {
    const id = yield* required(params, "harness");
    auths.set(id, yield* rpc["provider.auth.logout"]({ instanceId: ProviderInstanceId.make(id) }));
    yield* publishHarnesses;
    return {};
  });

  const setHarnessEnabled = Effect.fn("harness.setEnabled")(function* (params: Params) {
    const h = yield* harness(yield* required(params, "harness"));
    const instanceId = ProviderInstanceId.make(h.id);
    const current = settings?.providerInstances[instanceId];
    yield* rpc["server.updateSettings"]({
      patch: {},
      providerInstanceMutation: {
        operation: "upsert",
        instanceId,
        instance: {
          ...(current ?? { driver: ProviderDriverKind.make(h.driver) }),
          enabled: params.enabled !== false,
        },
      },
    });
    return {};
  });

  const searchHarnessRegistry = Effect.fn("harness.searchRegistry")(function* (params: Params) {
    const { agents: found } = yield* rpc["server.searchAcpRegistry"]({
      query: str(params, "query") ?? "",
    });
    return {
      results: found.map((a) => ({
        id: a.id,
        name: a.name,
        version: a.version,
        description: a.description,
      })),
    };
  });

  const addRegistryHarness = Effect.fn("harness.addRegistry")(function* (params: Params) {
    const registryId = yield* required(params, "registryId");
    const { agents: found } = yield* rpc["server.searchAcpRegistry"]({ query: registryId });
    const agent = found.find((a) => a.id === registryId);
    if (agent === undefined) return yield* fail("not_found", `the registry has no ${registryId}`);
    yield* rpc["server.prepareAcpRegistryAgent"]({ agentId: agent.id });
    const instanceId = ProviderInstanceId.make(
      registryId.toLowerCase().replace(/[^a-z0-9-]+/g, "-"),
    );
    yield* rpc["server.updateSettings"]({
      patch: {},
      providerInstanceMutation: {
        operation: "upsert",
        instanceId,
        instance: {
          driver: ProviderDriverKind.make("acpRegistry"),
          displayName: agent.name,
          enabled: true,
          config: { source: "registry", agentId: agent.id },
        },
      },
    });
    return { harness: instanceId };
  });

  const removeHarness = Effect.fn("harness.remove")(function* (params: Params) {
    const h = yield* harness(yield* required(params, "harness"));
    if (!h.fromRegistry)
      return yield* fail("unsupported", `${h.name} is built in; turn it off instead`);
    yield* rpc["server.updateSettings"]({
      patch: {},
      providerInstanceMutation: { operation: "remove", instanceId: ProviderInstanceId.make(h.id) },
    });
    return {};
  });

  const bridged =
    <E>(method: (params: Params) => Effect.Effect<unknown, E>) =>
    (params: Params) =>
      method(params).pipe(Effect.mapError(asBridgeError));
  const methods: Record<string, (params: Params) => Effect.Effect<unknown, BridgeError>> = {
    "fulmetry.subscribe": bridged(subscribe),
    "fulmetry.sendChat": bridged(sendChat),
    "fulmetry.startAgentForComment": bridged(startAgentForComment),
    "fulmetry.reply": bridged(reply),
    "fulmetry.respondToApproval": bridged(respondToApproval),
    "fulmetry.stopAgent": bridged(stopAgent),
    "harness.list": () => Effect.succeed({ harnesses: harnessList() }),
    "harness.install": bridged(installHarness),
    "harness.signIn": bridged(signInHarness),
    "harness.answerSignIn": bridged(answerSignIn),
    "harness.cancel": bridged(cancelHarnessSetup),
    "harness.signOut": bridged(signOutHarness),
    "harness.setEnabled": bridged(setHarnessEnabled),
    "harness.searchRegistry": bridged(searchHarnessRegistry),
    "harness.addRegistry": bridged(addRegistryHarness),
    "harness.remove": bridged(removeHarness),
  };

  const handle = Effect.fn("cli.fulmetry.handle")(function* (line: string) {
    if (line.trim() === "") return;
    const request = decodeRequest(line);
    if (Option.isNone(request)) {
      return yield* write({
        id: null,
        error: { code: "invalid_request", message: "not a request" },
      });
    }
    const { id, method, params } = request.value;
    const handler = methods[method];
    if (handler === undefined) {
      return yield* write({ id, error: { code: "unknown_method", message: method } });
    }
    const exit = yield* handler(params ?? {}).pipe(Effect.result);
    yield* exit._tag === "Success"
      ? write({ id, result: exit.success })
      : write({ id, error: { code: exit.failure.code, message: exit.failure.detail } });
  });

  yield* emit({ type: "fulmetry.ready", fulmetryExt: FULMETRY_EXT_VERSION });
  yield* stdio.stdin.pipe(
    Stream.decodeText(),
    Stream.splitLines,
    Stream.runForEach((line) =>
      handle(line).pipe(Effect.ignore({ log: true }), Effect.forkIn(scope)),
    ),
  );
});

export { fulmetryCommand };
