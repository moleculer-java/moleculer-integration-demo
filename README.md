# Moleculer Integration Demo — Java ⇄ Node.js

A small, self-contained demo that **proves, automatically and in both directions**, that the
**Java** implementation of Moleculer ([`moleculer-java`](https://github.com/moleculer-java/moleculer-java))
and the original **Node.js** [Moleculer](https://moleculer.services/) framework can form **one cluster**
and interoperate over a shared message bus.

Two cooperating processes join the same Moleculer cluster:

```
        ┌────────────────────────┐         NATS          ┌────────────────────────┐
        │   Java node            │   nats://localhost     │   Node.js node         │
        │   (Spring Boot +       │◄─────────4222─────────►│   (Moleculer 0.15)     │
        │    moleculer-java)     │   JSON serializer      │                        │
        │                        │   protocol v5          │                        │
        │  services:             │                        │  services:             │
        │   • mathJava           │                        │   • mathNode           │
        │   • dataJava           │                        │   • dataNode           │
        └────────────────────────┘                        └────────────────────────┘
                 │                                                     │
                 └── calls mathNode/dataNode/$node ──┐   ┌── calls mathJava/dataJava/$node ──┘
                                                     ▼   ▼
                              each side verifies the other and prints [PASS]/[FAIL]
```

Because both frameworks default to the **JSON serializer** and the Moleculer **wire protocol**, a Java
node and a Node.js node discover each other, see each other's services, and call each other's actions as
if they were local.

## What it verifies

Each side runs the **same fourteen scenarios** against the *other* language's node:

| # | Scenario | What it proves |
|---|----------|----------------|
| 1 | **Discovery** via the built-in `$node` service (`$node.list`, `$node.services`) | The two nodes joined one cluster and can see each other's node + services |
| 2 | **Primitive request/response** (`add`, `greet`) | Plain request/response calls cross the boundary |
| 3 | **List round-trip** (`getList`) | Arrays (incl. nested arrays) survive intact |
| 4 | **Map round-trip** (`getMap`) | Nested objects/maps survive intact |
| 5 | **All primitive types** (`getPrimitives`) | `int`, large integer, `double`, `boolean`, `string` and a real `null` survive |
| 6 | **Deep echo** (`echo`) | A rich nested object **deep-equals** what was sent — the strongest single proof |
| 7 | **System characteristics** (`$node.health`) | One framework can introspect the other's runtime (CPU, OS, uptime, memory, framework + version) |
| 8 | **Event round-trip** (`demo.fromJava` / `demo.fromNode`) | Events are delivered across the language boundary |
| 9 | **Ping** (`broker.ping`) | One framework can ping the other's node and read the round-trip timing back |
| 10 | **Event bus: emit + broadcast** (`eventStats`) | Both `emit` (load-balanced) and `broadcast` (all listeners) reach the other language |
| 11 | **Cacher with TTL** (`getCachedSeq`) | An action cached by key returns a cache **hit**, distinguishes **keys**, and **expires** after its TTL — and a remote caller sees the owner-side cache |
| 12 | **Complex user structure** (`getUsers`) | A life-like, deeply nested object (user with `address`/`geo`, `emails`/`roles` arrays and a `phones` array of objects) **deep-equals** intact across the boundary |
| 13 | **Metadata visibility** (`echoMeta`) | Each framework **sees the other's metadata** — a structured `meta` block sent with a call is read on the remote side (`ctx.meta` ⇄ `ctx.params.getMeta()`), echoed back as data, and the remote's response-meta marker is **merged back** into the caller |
| 14 | **Binary streaming** (`receiveStream` / `produceStream`) | **Binary data crosses intact, both ways** — a dynamically generated ~100 KB buffer is *streamed to* the remote (verified by byte count + SHA-256) and a 64 KB stream is *downloaded from* the remote (content verified by SHA-256) |

The service/call code is written to read cleanly as **copy-pasteable reference samples** for building
hybrid Java + JavaScript systems joined by Moleculer.

## Prerequisites

- **JDK 17+** and **Maven** (the Java side compiles with `maven.compiler.release=17`).
- **Node.js ≥ 22** and **npm** (developed against Node v24, npm 11).
- A **NATS server** reachable at **`nats://localhost:4222`**. The demo *assumes NATS is already
  running* — it does not start it for you. Quick ways to get one:
  - Docker: `docker run -p 4222:4222 nats:latest`
  - Binary: download from <https://nats.io/download/> and run `nats-server`

> The Java side resolves `moleculer-java` and the `datatree` stack from your local Maven repository
> (`~/.m2`). Other dependencies (Spring Boot, the NATS client, …) download from Maven Central / npm.

## Build & run

Start NATS first, then bring up both sides (**order does not matter** — each side waits for the
other's services before it runs its checks).

**Terminal A — Java side**

```bash
cd java
mvn clean package         # build the Spring Boot app
mvn spring-boot:run       # start the Java node, run the Java -> Node checks, then stay up
```

**Terminal B — Node.js side**

```bash
cd node
npm install               # install moleculer + the nats client
npm start                 # start the Node node, run the Node -> Java checks
```

Each side registers its services, waits for the other, runs its checks, and prints `[PASS]`/`[FAIL]`
lines. Expected end state: **every scenario `[PASS]` on both terminals.**

### Sample output (Node → Java)

```
================ Node -> Java verification ================
[PASS] 1a $node.list shows remote node 'java-node' (available)
[PASS] 1b $node.list reports remote framework = java
[PASS] 1c $node.services lists remote 'mathJava' and 'dataJava'
[PASS] 2a mathJava.add({a:2,b:3}) === 5
[PASS] 2b mathJava.greet({name:'Ada'}) ends with 'from Java!'
[PASS] 3a dataJava.getList is an array of length 2
[PASS] 3b dataJava.getList nested 'roles' arrays survived
[PASS] 4a getMap.server.lang === 'java'
... (4b–4e, 5a–5f, 6) ...
[PASS] 7a remote $node.health reports framework = java
... (7b–7f) ...
[PASS] 8 event 'demo.fromNode' received by Java (dataJava.lastEvent)
[PASS] 9a broker.ping('java-node') resolved a response
[PASS] 9b ping response carries elapsedTime (>= 0)
[PASS] 10a emit 'demo.emit.fromNode' received by Java (eventStats.lastEmit)
[PASS] 10b broadcast 'demo.broadcast.fromNode' received by Java (eventStats.lastBroadcast)
[PASS] 11a getCachedSeq{id:1} twice returns same seq (cache hit)
[PASS] 11b getCachedSeq{id:2} returns a different seq (key matters)
[PASS] 11c getCachedSeq{id:1} after TTL returns a fresh seq (expiry)
[PASS] 12a getUsers returns 2 users
... (12b–12e) ...
[PASS] 13a dataJava.echoMeta saw the meta we sent (remote reads our metadata)
[PASS] 13b response meta merged back to caller (seenBy='java')
[PASS] 14a dataJava.receiveStream received all 100000 bytes
[PASS] 14b dataJava.receiveStream SHA-256 matches (bytes intact end-to-end)
[PASS] 14c dataJava.produceStream produced 65536 bytes
[PASS] 14d dataJava.produceStream content matches the expected pattern (SHA-256)
-----------------------------------------------------------
Node -> Java result: 44 passed, 0 failed
===========================================================
```

The Java side prints the mirror-image output (`Java -> Node result: 44 passed, 0 failed`).

## Run modes

| Goal | Command |
|------|---------|
| **Java**: run checks, then stay up (default) | `cd java && mvn spring-boot:run` |
| **Java**: run checks, then exit (non-zero on failure — CI) | `mvn spring-boot:run -Dspring-boot.run.arguments=--demo.exit-after-verify=true` |
| **Node**: run checks, linger briefly, then exit (CI) | `cd node && npm start` |
| **Node**: run checks, then stay up | `cd node && DEMO_STAY_ALIVE=true npm start` |
| **Node**: just run the node (no checks) | `cd node && npm run serve` |
| **Java** integration test (self-skips if the cluster is down) | `cd java && mvn verify` |

`mvn clean package` always succeeds without a running cluster. `mvn verify` runs `JavaToNodeIT`, which
**self-skips** (JUnit assumption) when NATS or the Node.js side is unavailable, so it never fails a
cluster-less build.

## Interop conventions & caveats

- **JSON serializer on both sides.** It is the common denominator that makes the two frameworks
  wire-compatible. Keep it.
- **Wire protocol version.** Moleculer JS **0.15** speaks protocol **`"5"`** and silently ignores
  packets stamped with another version. `moleculer-java` **2.0.0 also defaults to `"5"`**, so the two
  interoperate out of the box; the Java node sets it explicitly anyway (via the `demo.protocol-version`
  property → `ServiceBrokerConfig.setProtocolVersion(...)`) to document the knob and so the demo can talk
  to a legacy Moleculer JS **0.14** node by setting `demo.protocol-version=4` (v4 and v5 are wire-compatible
  for the JSON serializer).
- **Stick to JSON-safe types** in action params and responses: plain objects/maps, arrays/lists and the
  primitives `number`, `string`, `boolean`, `null`. **Avoid** `Date`, `Map`, `Set`, `BigInt` and
  `Buffer` — the native JSON serializer cannot represent them, so they would not round-trip identically.
  Large integers stay within JavaScript's safe-integer range (`Number.MAX_SAFE_INTEGER`). **Binary data
  is still fully supported** — but through the dedicated **streaming** channel (scenario 14), which
  chunks bytes over the wire as `{ type: "Buffer", data: [...] }` packets, *not* as JSON `params`.
- **Unique `nodeID` per process.** The two nodes use `java-node` and `node-node`; colliding ids break
  cluster discovery.
- **Service names are shared identifiers.** An action is addressed as `<serviceName>.<actionName>` on
  either side, regardless of the implementing language.

All twelve scenarios — including a real `null`, a large integer, and a deeply nested object — round-trip
**intact in both directions** with no special handling beyond the protocol-version alignment above.

A few notes on the feature scenarios (9–14):

- **Ping** uses `broker.ping(...)`; both frameworks return a small timing record (the Java side a
  `Tree` with `time`/`arrived`, the Node side `{ nodeID, elapsedTime, timeDiff }`).
- **emit vs broadcast** — with a single instance per node both delivery modes reach the one remote
  listener, so this demo can only prove *both APIs cross the boundary*; the *one-of-group* (`emit`) vs
  *all-listeners* (`broadcast`) distinction needs multiple instances in a group to observe.
- **Caching is owner-side.** Moleculer caches an action on the node that *owns* it, so a remote caller
  benefits from the owner's cache. The cached action returns a counter that only advances on a real
  (uncached) invocation, which makes the hit / per-key / TTL-expiry behaviour observable. The TTL
  sub-check waits ~4 s for the 2 s entry to expire, so each side's run is a few seconds longer.
- **Metadata is one block, two APIs.** Moleculer's request `meta` is read as `ctx.meta` in JavaScript
  and as `ctx.params.getMeta()` in Java — the same on-the-wire field. It is *merged* across nested
  calls and *sent back* with the response, so the caller sees both that the remote read its meta and
  the remote's additions. Keep meta values JSON-safe, like params.
- **Streaming carries the bytes, `meta` carries the rest.** A streamed *request* opens with **empty
  `params`** — put a filename or any side-data in `meta`, because the first packet's `params` would
  otherwise be consumed as stream content by the receiver. (A *response* stream has no such limit: the
  call's `params` are normal, and the action returns the stream.) The demo generates its payloads in
  memory; to stream a real file instead, Java has `PacketStream.transferFrom(new File(...))` /
  `transferTo(new File(...))` and Node has `fs.createReadStream` / `fs.createWriteStream`.

## Layout

```
moleculer-integration-demo/
├── README.md                 ← this file
├── .gitignore
├── .vscode/                  ← settings, recommended extensions, launch configs
├── java/                     ← the Java side (Spring Boot + moleculer-java)
│   ├── pom.xml
│   └── src/main/java/integration/demo/
│       ├── IntegrationDemoApplication.java   ← Spring Boot main (non-web)
│       ├── BrokerConfig.java                 ← @Bean ServiceBroker: nodeID, NATS, JSON, protocol v5
│       ├── services/MathJavaService.java     ← service "mathJava"
│       ├── services/DataJavaService.java     ← service "dataJava" + event listener
│       └── verify/JavaToNodeVerifier.java    ← the Java -> Node harness
│   └── src/test/java/integration/demo/
│       └── JavaToNodeIT.java                 ← optional JUnit 5 wrapper (self-skips if cluster down)
└── node/                     ← the Node.js side (Moleculer 0.15)
    ├── package.json
    ├── moleculer.config.js   ← nodeID, NATS transporter, JSON serializer
    ├── start.js              ← run the node only (no checks)
    ├── services/math-node.service.js   ← service "mathNode"
    ├── services/data-node.service.js   ← service "dataNode" + event listener
    └── verify/node-to-java.js          ← the Node -> Java harness
```

This is a demo / verification harness, not a published library — it is not deployed to Maven Central or
npm. Everything it needs runs on `localhost`.
