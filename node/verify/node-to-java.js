"use strict";

/**
 * The automated Node -> Java verification harness.
 *
 * This is a full Moleculer node: it registers the demo services (so the Java
 * side can call back into Node) and, once the Java node's services are
 * discovered, it calls across the language boundary, compares the responses
 * against expected values and prints a [PASS]/[FAIL] line per scenario. The same
 * call code is written to read cleanly as copy-pasteable documentation samples.
 *
 * The twelve scenarios (mirrored by the Java JavaToNodeVerifier) are:
 *   1. Discovery / cluster join (via the built-in $node service)
 *   2. Primitive request/response (mathJava.add, mathJava.greet)
 *   3. List round-trip (dataJava.getList)
 *   4. Map round-trip (dataJava.getMap)
 *   5. All primitive types (dataJava.getPrimitives)
 *   6. Deep echo of a nested object (dataJava.echo)
 *   7. System characteristics of the remote node ($node.health)
 *   8. Event round-trip (demo.fromNode -> dataJava.lastEvent)
 *   9. Ping the remote node (broker.ping)
 *   10. Event bus: emit and broadcast (dataJava.eventStats)
 *   11. Cacher with TTL (dataJava.getCachedSeq: hit / keying / expiry)
 *   12. Life-like nested structure round-trip (dataJava.getUsers)
 *   13. Metadata visibility both ways (dataJava.echoMeta)
 *   14. Binary streaming both ways (dataJava.receiveStream / dataJava.produceStream)
 *
 * Run modes:
 *   - default: after the checks, stay up a short grace period (so the Java side
 *     can finish ITS checks), then stop and exit (non-zero on failure) — CI-friendly.
 *   - DEMO_STAY_ALIVE=true: keep running after the checks (don't exit).
 */
const { ServiceBroker } = require("moleculer");
const crypto = require("crypto");
const { Readable } = require("stream");
const config = require("../moleculer.config.js");

// --- The remote (Java) side this harness talks to ---
const REMOTE_NODE = "java-node";
const MATH = "mathJava";
const DATA = "dataJava";
const GREET_SUFFIX = "from Java!";
const REMOTE_FRAMEWORK = "java";
const EVENT_TO_JAVA = "demo.fromNode";
const EMIT_TO_JAVA = "demo.emit.fromNode";
const BROADCAST_TO_JAVA = "demo.broadcast.fromNode";

const WAIT_MS = Number(process.env.DEMO_WAIT_MS || 30000);
const CALL_MS = Number(process.env.DEMO_CALL_MS || 10000);
const GRACE_MS = Number(process.env.DEMO_GRACE_MS || 8000);

// --- Build the node (with the demo services registered) ---
const broker = new ServiceBroker(config);
broker.createService(require("../services/math-node.service.js"));
broker.createService(require("../services/data-node.service.js"));

// ---------------------------------------------------------------------------
//  Tiny check helper
// ---------------------------------------------------------------------------
let passed = 0;
let failed = 0;

function pass(name) {
	passed++;
	console.log(`[PASS] ${name}`);
}

function fail(name, detail) {
	failed++;
	console.log(`[FAIL] ${name} — ${detail}`);
}

function check(name, condition, detailIfFail) {
	condition ? pass(name) : fail(name, detailIfFail);
}

function sleep(ms) {
	return new Promise(resolve => setTimeout(resolve, ms));
}

/** Wraps a Buffer in a binary Readable that emits it in `chunkSize` slices (for the streaming send test). */
function bufferToStream(buf, chunkSize = 16 * 1024) {
	let pos = 0;
	return new Readable({
		read() {
			if (pos >= buf.length) {
				this.push(null);
				return;
			}
			const end = Math.min(pos + chunkSize, buf.length);
			this.push(buf.subarray(pos, end));
			pos = end;
		}
	});
}

/** Collects an incoming Readable stream into a single Buffer. */
function collectStream(stream) {
	return new Promise((resolve, reject) => {
		const chunks = [];
		stream.on("data", chunk => chunks.push(chunk));
		stream.on("end", () => resolve(Buffer.concat(chunks)));
		stream.on("error", reject);
	});
}

/**
 * Deep structural equality for JSON-safe values (objects, arrays, primitives,
 * null). Used to verify the echo round trip. Numbers compare by value, which is
 * all we need since JSON numbers round-trip as JS numbers.
 */
function deepEqual(a, b) {
	if (a === b) {
		return true;
	}
	if (a === null || b === null) {
		return a === b;
	}
	if (typeof a !== typeof b) {
		return false;
	}
	if (typeof a !== "object") {
		return a === b;
	}
	const aArr = Array.isArray(a);
	const bArr = Array.isArray(b);
	if (aArr !== bArr) {
		return false;
	}
	if (aArr) {
		if (a.length !== b.length) {
			return false;
		}
		return a.every((item, i) => deepEqual(item, b[i]));
	}
	const aKeys = Object.keys(a);
	const bKeys = Object.keys(b);
	if (aKeys.length !== bKeys.length) {
		return false;
	}
	return aKeys.every(k => Object.prototype.hasOwnProperty.call(b, k) && deepEqual(a[k], b[k]));
}

// ---------------------------------------------------------------------------
//  Scenarios — each is a clean cross-language call sample
// ---------------------------------------------------------------------------

/** 1. Discovery: read the remote node + services through the $node service. */
async function scenarioDiscovery() {
	// $node.list returns every known node (local + remote), aggregated locally.
	const nodes = await broker.call("$node.list");
	const remote = nodes.find(n => n.id === REMOTE_NODE);
	check(`1a $node.list shows remote node '${REMOTE_NODE}' (available)`,
		!!remote && remote.available === true, "remote node not present/available");
	check(`1b $node.list reports remote framework = ${REMOTE_FRAMEWORK}`,
		!!remote && remote.client && remote.client.type === REMOTE_FRAMEWORK,
		`got '${remote && remote.client ? remote.client.type : "<none>"}'`);

	// $node.services aggregates the services advertised by every node.
	const services = await broker.call("$node.services");
	const names = services.map(s => s.name);
	check(`1c $node.services lists remote '${MATH}' and '${DATA}'`,
		names.includes(MATH) && names.includes(DATA),
		`got services: ${JSON.stringify(names)}`);
}

/** 2. Primitive request/response. */
async function scenarioPrimitiveCall() {
	const sum = await broker.call(`${MATH}.add`, { a: 2, b: 3 });
	check(`2a ${MATH}.add({a:2,b:3}) === 5`, sum === 5, `got ${sum}`);

	const greeting = await broker.call(`${MATH}.greet`, { name: "Ada" });
	check(`2b ${MATH}.greet({name:'Ada'}) ends with '${GREET_SUFFIX}'`,
		typeof greeting === "string" && greeting.endsWith(GREET_SUFFIX), `got '${greeting}'`);
}

/** 3. List round-trip (nested arrays survive). */
async function scenarioList() {
	const list = await broker.call(`${DATA}.getList`);
	const shapeOk = Array.isArray(list) && list.length === 2;
	const rolesOk = shapeOk && list.every(it =>
		typeof it.id === "number" && typeof it.name === "string"
		&& Array.isArray(it.roles) && it.roles.length >= 1);
	check(`3a ${DATA}.getList is an array of length 2`, shapeOk,
		`got ${JSON.stringify(list)}`);
	check(`3b ${DATA}.getList nested 'roles' arrays survived`, rolesOk,
		`got ${JSON.stringify(list)}`);
}

/** 4. Map round-trip (nested keys/values match). */
async function scenarioMap() {
	const map = await broker.call(`${DATA}.getMap`);
	check("4a getMap.server.lang === 'java'", map.server && map.server.lang === "java",
		`got ${JSON.stringify(map.server)}`);
	check("4b getMap.server.version is present",
		!!map.server && typeof map.server.version === "string" && map.server.version.length > 0,
		`got ${JSON.stringify(map.server)}`);
	check("4c getMap.counts.users === 2", !!map.counts && map.counts.users === 2,
		`got ${JSON.stringify(map.counts)}`);
	check("4d getMap.counts.active === 1", !!map.counts && map.counts.active === 1,
		`got ${JSON.stringify(map.counts)}`);
	check("4e getMap.enabled === true", map.enabled === true, `got ${map.enabled}`);
}

/** 5. Every JSON-safe primitive type comes back with the right type and value. */
async function scenarioPrimitives() {
	const p = await broker.call(`${DATA}.getPrimitives`);
	check("5a getPrimitives.i === 42 (int)", p.i === 42, `got ${p.i}`);
	check("5b getPrimitives.big === 9007199254740991 (large int)",
		p.big === 9007199254740991, `got ${p.big}`);
	check("5c getPrimitives.d === 3.5 (double)", p.d === 3.5 && !Number.isInteger(p.d), `got ${p.d}`);
	check("5d getPrimitives.b === true (boolean)", p.b === true, `got ${p.b}`);
	check("5e getPrimitives.s === 'hello' (string)", p.s === "hello", `got ${p.s}`);
	check("5f getPrimitives.n is a real null", p.n === null,
		`got ${p.n === undefined ? "<missing>" : JSON.stringify(p.n)}`);
}

/** 6. Deep echo: a rich nested object round-trips intact in both directions. */
async function scenarioDeepEcho() {
	const sent = buildRichPayload();
	const received = await broker.call(`${DATA}.echo`, sent);
	check(`6 ${DATA}.echo deep-equals the sent nested object`, deepEqual(sent, received),
		`sent ${JSON.stringify(sent)} but got ${JSON.stringify(received)}`);
}

/** 7. System characteristics: introspect the remote node's runtime via $node.health. */
async function scenarioSystemInfo() {
	// Target the remote node explicitly so we read ITS health, not ours.
	const health = await broker.call("$node.health", {}, { nodeID: REMOTE_NODE, timeout: CALL_MS });

	const framework = (health.client && health.client.type) || "";
	const version = (health.client && health.client.version) || "";
	const cores = (health.cpu && health.cpu.cores) || 0;
	const osType = (health.os && health.os.type) || "";
	const uptime = (health.process && health.process.uptime != null) ? health.process.uptime : -1;
	const heapUsed = (health.process && health.process.memory && health.process.memory.heapUsed) || 0;

	check(`7a remote $node.health reports framework = ${REMOTE_FRAMEWORK}`,
		framework === REMOTE_FRAMEWORK, `got '${framework}'`);
	check("7b remote health exposes framework version", version.length > 0, `got '${version}'`);
	check("7c remote health exposes CPU cores", cores > 0, `got ${cores}`);
	check("7d remote health exposes OS type", osType.length > 0, `got '${osType}'`);
	check("7e remote health exposes process uptime", uptime >= 0, `got ${uptime}`);
	check("7f remote health exposes memory (heapUsed)", heapUsed > 0, `got ${heapUsed}`);
}

/** 8. Event round-trip: emit to Java, then read it back via dataJava.lastEvent. */
async function scenarioEvent() {
	await broker.broadcast(EVENT_TO_JAVA, { from: "node", n: 7 });

	// Events are fire-and-forget, so poll briefly for delivery.
	let got = null;
	for (let i = 0; i < 20; i++) {
		got = await broker.call(`${DATA}.lastEvent`);
		if (got && got.from === "node") {
			break;
		}
		await sleep(250);
	}
	check(`8 event '${EVENT_TO_JAVA}' received by Java (dataJava.lastEvent)`,
		!!got && got.from === "node" && got.n === 7, `got ${JSON.stringify(got)}`);
}

/** 9. Ping: probe the remote node with broker.ping and read the timing back. */
async function scenarioPing() {
	// broker.ping(nodeID, timeout) resolves to { nodeID, elapsedTime, timeDiff }
	// for a reachable node, or null on timeout.
	const pong = await broker.ping(REMOTE_NODE, CALL_MS);
	check(`9a broker.ping('${REMOTE_NODE}') resolved a response`,
		!!pong && pong.nodeID === REMOTE_NODE, `got ${JSON.stringify(pong)}`);
	check("9b ping response carries elapsedTime (>= 0)",
		!!pong && typeof pong.elapsedTime === "number" && pong.elapsedTime >= 0,
		`got ${pong ? pong.elapsedTime : "<null>"}`);
}

/** 10. Event bus: prove both emit and broadcast cross to Java. */
async function scenarioEvents() {
	await broker.emit(EMIT_TO_JAVA, { via: "emit", n: 10 });            // load-balanced to one listener per group
	await broker.broadcast(BROADCAST_TO_JAVA, { via: "broadcast", n: 11 }); // all listeners on all nodes

	// Events are fire-and-forget, so poll the remote stats until both arrive.
	let stats = null;
	let emitOk = false;
	let bcastOk = false;
	for (let i = 0; i < 20; i++) {
		stats = await broker.call(`${DATA}.eventStats`);
		emitOk = !!stats && !!stats.lastEmit && stats.lastEmit.via === "emit";
		bcastOk = !!stats && !!stats.lastBroadcast && stats.lastBroadcast.via === "broadcast";
		if (emitOk && bcastOk) {
			break;
		}
		await sleep(250);
	}
	check(`10a emit '${EMIT_TO_JAVA}' received by Java (eventStats.lastEmit)`, emitOk,
		`got ${JSON.stringify(stats)}`);
	check(`10b broadcast '${BROADCAST_TO_JAVA}' received by Java (eventStats.lastBroadcast)`, bcastOk,
		`got ${JSON.stringify(stats)}`);
}

/** 11. Cacher with TTL: prove cache hit, per-key caching and TTL expiry through Java. */
async function scenarioCache() {
	// Two calls with the SAME id within the TTL: the second is a cache hit, so
	// the remote action body does not run and the seq is unchanged.
	const a = await broker.call(`${DATA}.getCachedSeq`, { id: 1 });
	const b = await broker.call(`${DATA}.getCachedSeq`, { id: 1 });
	check("11a getCachedSeq{id:1} twice returns same seq (cache hit)",
		a.seq > 0 && a.seq === b.seq, `got seqA=${a.seq}, seqB=${b.seq}`);

	// A different id is a different cache key, so the body runs again.
	const c = await broker.call(`${DATA}.getCachedSeq`, { id: 2 });
	check("11b getCachedSeq{id:2} returns a different seq (key matters)",
		c.seq !== a.seq, `got seqC=${c.seq} (id:1 was ${a.seq})`);

	// After the TTL (2 s) expires, the entry is evicted and the body re-runs.
	await sleep(4000);
	const d = await broker.call(`${DATA}.getCachedSeq`, { id: 1 });
	check("11c getCachedSeq{id:1} after TTL returns a fresh seq (expiry)",
		d.seq > a.seq, `got seqD=${d.seq} (was ${a.seq})`);
}

/** 12. Life-like nested structure: a users list with address/geo, email + role arrays and phone objects. */
async function scenarioUsers() {
	const result = await broker.call(`${DATA}.getUsers`);
	const users = result && result.users;

	check("12a getUsers returns 2 users", Array.isArray(users) && users.length === 2,
		`got ${JSON.stringify(result)}`);
	check("12b users[0].address.geo.lat === 51.5074 (nested object)",
		!!users && users[0] && users[0].address && users[0].address.geo
		&& users[0].address.geo.lat === 51.5074,
		`got ${JSON.stringify(users && users[0] && users[0].address)}`);
	check("12c users[0].emails is a string array, intact",
		!!users && Array.isArray(users[0].emails) && users[0].emails.length === 2
		&& users[0].emails[0] === "ada@analytical.io",
		`got ${JSON.stringify(users && users[0] && users[0].emails)}`);
	check("12d users[1].phones is an array of objects, intact",
		!!users && Array.isArray(users[1].phones) && users[1].phones.length === 1
		&& users[1].phones[0].type === "work" && !!users[1].phones[0].number,
		`got ${JSON.stringify(users && users[1] && users[1].phones)}`);

	// Strongest proof: the whole structure deep-equals what we expect.
	check("12e getUsers deep-equals the expected nested structure",
		deepEqual(result, buildExpectedUsers()), `got ${JSON.stringify(result)}`);
}

/**
 * 13. Metadata: prove the remote node SEES the metadata we send, and that response
 * metadata is merged back into the caller. In Moleculer JS the metadata is
 * `ctx.meta`; in moleculer-java it is `ctx.params.getMeta()`. Both serialize to the
 * request's top-level `meta` field, so they are interchangeable.
 */
async function scenarioMeta() {
	// A structured meta block: scalars plus a nested array and a nested object.
	const sentMeta = { tenant: "acme", trace: "trace-001", n: 7, tags: ["a", "b"], ctx: { k: "v" } };
	// The object we pass as opts.meta is the SAME one Moleculer merges response meta into.
	const meta = JSON.parse(JSON.stringify(sentMeta));

	const rsp = await broker.call(`${DATA}.echoMeta`, null, { meta });

	// 13a: the remote echoed back, as response data, exactly the meta we sent.
	check(`13a ${DATA}.echoMeta saw the meta we sent (remote reads our metadata)`,
		deepEqual(rsp && rsp.receivedMeta, sentMeta), `got ${JSON.stringify(rsp && rsp.receivedMeta)}`);

	// 13b: the remote's response meta (its 'seenBy' marker) merged back into our meta.
	check("13b response meta merged back to caller (seenBy='java')",
		meta.seenBy === "java", `got meta=${JSON.stringify(meta)}`);
}

/**
 * 14. Streaming: prove binary data crosses the language boundary intact, in both
 * directions. First a dynamically generated buffer is SENT to the remote
 * `receiveStream` (which returns the byte count + SHA-256 it saw); then a stream is
 * DOWNLOADED from the remote `produceStream` and its content verified.
 *
 * A streamed request opens with empty params; side-data must travel in `meta`,
 * never in `params` (a non-empty first packet would be treated as stream content).
 */
async function scenarioStreaming() {
	const streamTimeout = CALL_MS + 8000;

	// --- 14a/b: SEND a dynamically generated binary stream to the remote ---
	const sendSize = 100000;
	const sent = crypto.randomBytes(sendSize);
	const sentHash = crypto.createHash("sha256").update(sent).digest("hex");

	const ack = await broker.call(`${DATA}.receiveStream`, null, {
		stream: bufferToStream(sent),
		timeout: streamTimeout
	});
	check(`14a ${DATA}.receiveStream received all ${sendSize} bytes`,
		!!ack && ack.bytes === sendSize, `got ${ack && ack.bytes}`);
	check(`14b ${DATA}.receiveStream SHA-256 matches (bytes intact end-to-end)`,
		!!ack && ack.sha256 === sentHash, `expected ${sentHash} but got ${ack && ack.sha256}`);

	// --- 14c/d: DOWNLOAD a binary stream produced by the remote ---
	const prodSize = 64 * 1024;
	const stream = await broker.call(`${DATA}.produceStream`, { size: prodSize }, { timeout: streamTimeout });
	const got = await collectStream(stream);
	check(`14c ${DATA}.produceStream produced ${prodSize} bytes`,
		got.length === prodSize, `got ${got.length} bytes`);

	const expected = Buffer.allocUnsafe(prodSize);
	for (let i = 0; i < prodSize; i++) {
		expected[i] = i & 0xFF;
	}
	const gotHash = crypto.createHash("sha256").update(got).digest("hex");
	const expHash = crypto.createHash("sha256").update(expected).digest("hex");
	check(`14d ${DATA}.produceStream content matches the expected pattern (SHA-256)`,
		gotHash === expHash, "downloaded content did not match the deterministic pattern");
}

/** Builds the users structure we expect dataJava.getUsers to return (by-value mirror). */
function buildExpectedUsers() {
	return {
		users: [
			{
				id: 1,
				name: "Ada Lovelace",
				active: true,
				address: {
					street: "12 Analytical Ave",
					city: "London",
					zip: "EC1A 1AA",
					geo: { lat: 51.5074, lng: -0.1278 }
				},
				emails: ["ada@analytical.io", "lovelace@maths.org"],
				roles: ["admin", "author"],
				phones: [
					{ type: "home", number: "+44-20-7946-0001" },
					{ type: "mobile", number: "+44-7700-900002" }
				]
			},
			{
				id: 2,
				name: "Linus Torvalds",
				active: false,
				address: {
					street: "1 Kernel Way",
					city: "Portland",
					zip: "97201",
					geo: { lat: 45.5152, lng: -122.6784 }
				},
				emails: ["linus@kernel.org"],
				roles: ["maintainer"],
				phones: [
					{ type: "work", number: "+1-503-555-0100" }
				]
			}
		]
	};
}

/** Builds a rich, nested, JSON-safe object for the deep-echo test. */
function buildRichPayload() {
	return {
		msg: "ping",
		when: 123,
		ok: true,
		nada: null,
		nums: [1, 2, 3],
		items: [
			{ id: 1, tags: ["x", "y"] },
			{ id: 2, tags: [] }
		],
		nested: { a: { b: { c: "deep" } } }
	};
}

// ---------------------------------------------------------------------------
//  Orchestration
// ---------------------------------------------------------------------------

/** Runs one scenario, turning any thrown error into a single FAIL line. */
async function run(label, body) {
	try {
		await body();
	} catch (err) {
		fail(`scenario ${label}`, `threw ${err.message || err}`);
	}
}

async function main() {
	await broker.start();

	console.log();
	console.log("================ Node -> Java verification ================");

	// Wait for the Java services before starting (scenario 1 precondition).
	try {
		await broker.waitForServices([MATH, DATA], WAIT_MS);
	} catch (err) {
		console.log(`[FAIL] java side not discovered — ${MATH}/${DATA} not available within ${WAIT_MS} ms`);
		console.log("-----------------------------------------------------------");
		console.log("Node -> Java result: 0 passed, 1 failed");
		console.log("===========================================================");
		await broker.stop();
		process.exit(1);
	}

	await run("1 discovery", scenarioDiscovery);
	await run("2 primitive call", scenarioPrimitiveCall);
	await run("3 list round-trip", scenarioList);
	await run("4 map round-trip", scenarioMap);
	await run("5 all primitive types", scenarioPrimitives);
	await run("6 deep echo", scenarioDeepEcho);
	await run("7 system characteristics", scenarioSystemInfo);
	await run("8 event round-trip", scenarioEvent);
	await run("9 ping", scenarioPing);
	await run("10 event bus emit/broadcast", scenarioEvents);
	await run("11 cacher with TTL", scenarioCache);
	await run("12 complex user structure", scenarioUsers);
	await run("13 metadata visibility", scenarioMeta);
	await run("14 binary streaming", scenarioStreaming);

	console.log("-----------------------------------------------------------");
	console.log(`Node -> Java result: ${passed} passed, ${failed} failed`);
	console.log("===========================================================");
	console.log();

	if (process.env.DEMO_STAY_ALIVE === "true") {
		console.log("Node node staying up (DEMO_STAY_ALIVE=true). Press Ctrl+C to stop.");
		process.on("SIGINT", async () => {
			await broker.stop();
			process.exit(failed > 0 ? 1 : 0);
		});
		return; // keep the event loop alive via the broker
	}

	console.log(`Staying up ${GRACE_MS} ms so the Java side can finish its checks, then exiting...`);
	await sleep(GRACE_MS);
	await broker.stop();
	process.exit(failed > 0 ? 1 : 0);
}

main().catch(async err => {
	console.error("Fatal error in verifier:", err);
	try {
		await broker.stop();
	} catch (ignored) {
		// already stopping
	}
	process.exit(1);
});
