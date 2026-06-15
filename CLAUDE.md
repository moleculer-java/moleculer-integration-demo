# CLAUDE.md — Moleculer Integration Demo

This file provides guidance to Claude Code (claude.ai/code) when working in this repository.

## What this is

A small, self-contained **demo project** that shows and verifies how the **Java** implementation of
Moleculer (`moleculer-java`) and the original **Node.js** Moleculer framework interoperate over a
shared message bus.

Two cooperating processes form **one Moleculer cluster**:

- a **Java side** (`java/`) — a **Spring Boot** application that hosts `moleculer-java` services;
- a **Node.js side** (`node/`) — a **Moleculer 0.15** application that hosts JavaScript services.

They connect through a **NATS** server running locally at `nats://localhost:4222` and speak the
Moleculer **wire protocol v5** with the **JSON serializer**. Moleculer JS 0.15 speaks protocol `"5"`
and *silently drops* packets stamped with a different `"ver"`; **moleculer-java 2.0.0 also defaults to
`"5"`** (configurable via `ServiceBrokerConfig.setProtocolVersion(...)` — v4 and v5 are wire-compatible
for the JSON serializer, so set `"4"` for a legacy 0.14 node). With matching protocol version and
serializer, a Java node and a Node.js node discover each other, see each other's services, and call
each other's actions as if they were local.

## Purpose

Demonstrate — and **automatically verify** — that the two frameworks can communicate, in **both
directions**, without errors:

- **Call each other's actions** (request/response), Java→Node and Node→Java.
- **Query each other's system characteristics** — the remote node list, its published services, and its
  health/uptime/CPU/memory — through Moleculer's built-in `$node` introspection service.
- **Exchange structured data** — lists, maps/objects, and primitive values (integers, floating point,
  booleans, strings, null), plus a deeply nested, life-like object — and get them back intact in both
  directions.
- **Send and receive events** across the language boundary (both `emit` and `broadcast`).
- **Ping** the other node and read the round-trip timing back (`broker.ping`).
- **Cache** an action by key with a TTL and observe the owner-side cache from a remote caller.

The project doubles as a set of **copy-pasteable reference samples** for building hybrid systems where
one part of the application is written in JavaScript and another part in Java, joined by Moleculer.

## Layout

```
moleculer-integration-demo/
├── CLAUDE.md                 ← you are here
├── TODO.md                   ← build instructions for an isolated Claude Code in this directory
├── README.md                 ← human-facing quick start (generated while building)
├── .gitignore
├── .vscode/                  ← settings, recommended extensions, launch configs
├── java/                     ← the Java side (Spring Boot + moleculer-java 2.0.0)
│   ├── pom.xml
│   └── src/main/java/integration/demo/...
└── node/                     ← the Node.js side (Moleculer 0.15)
    ├── package.json
    ├── moleculer.config.js
    └── services/ ...
```

## Prerequisites

- **JDK 21** and **Maven** (the Java side targets `maven.compiler.release=21`).
- **Node.js >= 22** (developed and tested with **Node v24.11.1**, **npm 11.6.4**).
- A **NATS server** reachable at `nats://localhost:4222`. The demo **assumes NATS is already running**
  (start it however you prefer); the project only documents the connection URL, it does not start NATS.
- The **`moleculer-java` 2.0.0-SNAPSHOT** artifact (and the `datatree` stack it depends on) installed in
  the local Maven repository (`~/.m2`). The Java side resolves it from there.

## Build & run

```bash
# Java side
cd java
mvn clean package            # compile + build the Spring Boot app
mvn spring-boot:run          # start the Java node (registers services, joins the cluster)

# Node.js side
cd node
npm install                  # install moleculer + the nats client
npm start                    # start the Node node (registers services, joins the cluster)
```

Start NATS first, then bring up both sides (order does not matter — each side waits for the other's
services before it runs its checks).

## How it verifies

Each side ships an **automated check harness** that, once the other node's services are discovered,
calls across the boundary, compares the responses against expected values, and prints clear
`[PASS]` / `[FAIL]` lines per scenario, finishing with a non-zero exit code if anything failed. The very
same service and call code is written to read cleanly as **example snippets** — the harness *is* the
documentation sample. See `TODO.md` for the exact scenarios and the run flow.

## Interop conventions (read before changing service code)

- **JSON serializer on both sides.** Keep it. It is the common denominator that makes the two
  frameworks wire-compatible.
- **Stick to JSON-safe types** in action params and responses: plain objects/maps, arrays/lists, and the
  primitive values `number`, `string`, `boolean`, `null`. **Avoid** JavaScript `Date`, `Map`, `Set`,
  `BigInt`, and `Buffer` — the native JSON serializer cannot represent them, so they would not survive
  the round trip identically. On the Java side this maps to `io.datatree.Tree` values built from the same
  JSON-safe types. Document any deviation as a known limitation.
- **Unique `nodeID` per process.** The Java node and the Node.js node must use different node IDs (the
  demo uses `java-node` and `node-node`); colliding IDs break cluster discovery.
- **Service names are shared identifiers.** An action is addressed as `<serviceName>.<actionName>` on
  either side, regardless of the language that implements it — that is the whole point of the demo.

## Things to keep in mind

- This is a **demo / verification harness**, not a published library; it is **not** deployed to Maven
  Central or npm. It is, however, intended to be published to **GitHub** as a public example, so keep the
  code, comments, and docs clean, neutral, and generally useful to anyone integrating the two frameworks.
- There is no shared database or external broker beyond NATS; everything the demo needs runs on
  `localhost`.
