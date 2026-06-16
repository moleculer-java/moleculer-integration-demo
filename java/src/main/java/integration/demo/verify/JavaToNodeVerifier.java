package integration.demo.verify;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import io.datatree.Promise;
import io.datatree.Tree;
import services.moleculer.ServiceBroker;
import services.moleculer.context.CallOptions;
import services.moleculer.stream.PacketStream;

/**
 * The automated <b>Java &rarr; Node</b> verification harness.
 * <p>
 * Once the Node.js node's services are discovered, this component calls across
 * the language boundary and checks the responses against expected values,
 * printing a {@code [PASS]}/{@code [FAIL]} line per scenario. The same call code
 * is written to read cleanly as copy-pasteable documentation samples.
 * <p>
 * The twelve scenarios (mirrored by {@code node/verify/node-to-java.js}) are:
 * <ol>
 *   <li>Discovery / cluster join (via the built-in {@code $node} service)</li>
 *   <li>Primitive request/response ({@code mathNode.add}, {@code mathNode.greet})</li>
 *   <li>List round-trip ({@code dataNode.getList})</li>
 *   <li>Map round-trip ({@code dataNode.getMap})</li>
 *   <li>All primitive types ({@code dataNode.getPrimitives})</li>
 *   <li>Deep echo of a nested object ({@code dataNode.echo})</li>
 *   <li>System characteristics of the remote node ({@code $node.health})</li>
 *   <li>Event round-trip ({@code demo.fromJava} &rarr; {@code dataNode.lastEvent})</li>
 *   <li>Ping the remote node ({@code broker.ping})</li>
 *   <li>Event bus: {@code emit} and {@code broadcast} ({@code dataNode.eventStats})</li>
 *   <li>Cacher with TTL ({@code dataNode.getCachedSeq}: hit / keying / expiry)</li>
 *   <li>Life-like nested structure round-trip ({@code dataNode.getUsers})</li>
 *   <li>Metadata visibility both ways ({@code dataNode.echoMeta})</li>
 *   <li>Binary streaming both ways ({@code dataNode.receiveStream} / {@code dataNode.produceStream})</li>
 * </ol>
 *
 * <h2>Run modes</h2>
 * <ul>
 *   <li>Default: after running the checks the node stays up so the Node.js side
 *       can call back into Java (the two-terminal demo flow).</li>
 *   <li>{@code demo.exit-after-verify=true}: the process exits with a non-zero
 *       code if any check failed &mdash; handy for CI.</li>
 *   <li>{@code demo.auto-verify=false}: do not run automatically on start-up
 *       (used by the integration test, which drives {@link #runScenarios()}).</li>
 * </ul>
 */
@Component
public class JavaToNodeVerifier {

	// --- The remote (Node.js) side this harness talks to ---
	private static final String REMOTE_NODE = "node-node";
	private static final String MATH = "mathNode";
	private static final String DATA = "dataNode";
	private static final String GREET_SUFFIX = "from Node!";
	private static final String REMOTE_FRAMEWORK = "nodejs";
	private static final String EVENT_TO_NODE = "demo.fromJava";
	private static final String EMIT_TO_NODE = "demo.emit.fromJava";
	private static final String BROADCAST_TO_NODE = "demo.broadcast.fromJava";

	private final ServiceBroker broker;
	private final ConfigurableApplicationContext appContext;

	/** Source of the dynamically generated binary payload used by scenario 14. */
	private final Random rnd = new Random();

	@Value("${demo.auto-verify:true}")
	private boolean autoVerify;

	@Value("${demo.exit-after-verify:false}")
	private boolean exitAfterVerify;

	@Value("${demo.wait-timeout-ms:30000}")
	private long waitTimeoutMs;

	@Value("${demo.call-timeout-ms:10000}")
	private long callTimeoutMs;

	public JavaToNodeVerifier(ServiceBroker broker, ConfigurableApplicationContext appContext) {
		this.broker = broker;
		this.appContext = appContext;
	}

	// ===================================================================
	//  Orchestration
	// ===================================================================

	/** Runs automatically once the Spring application is ready. */
	@EventListener(ApplicationReadyEvent.class)
	public void onApplicationReady() {
		if (!autoVerify) {
			return;
		}

		System.out.println();
		System.out.println("================ Java -> Node verification ================");

		// Wait for the Node.js services before starting (scenario 1 precondition).
		try {
			broker.waitForServices(waitTimeoutMs, MATH, DATA).waitFor(waitTimeoutMs + 2000);
		} catch (Exception cause) {
			System.out.println("[FAIL] node side not discovered — " + MATH + "/" + DATA
					+ " not available within " + waitTimeoutMs + " ms (" + cause + ")");
			System.out.println("-----------------------------------------------------------");
			System.out.println("Java -> Node result: 0 passed, 1 failed");
			System.out.println("===========================================================");
			if (exitAfterVerify) {
				System.exit(SpringApplication.exit(appContext, () -> 1));
			}
			return;
		}

		CheckRunner cr = runScenarios();
		finish(cr);
	}

	/**
	 * Runs all eight verification scenarios against the Node.js side and returns
	 * the tally. Safe to call from a test (each scenario is isolated, so one
	 * failure never aborts the rest).
	 */
	public CheckRunner runScenarios() {
		CheckRunner cr = new CheckRunner();
		long t = callTimeoutMs;

		run(cr, "1 discovery", () -> scenarioDiscovery(cr, t));
		run(cr, "2 primitive call", () -> scenarioPrimitiveCall(cr, t));
		run(cr, "3 list round-trip", () -> scenarioList(cr, t));
		run(cr, "4 map round-trip", () -> scenarioMap(cr, t));
		run(cr, "5 all primitive types", () -> scenarioPrimitives(cr, t));
		run(cr, "6 deep echo", () -> scenarioDeepEcho(cr, t));
		run(cr, "7 system characteristics", () -> scenarioSystemInfo(cr, t));
		run(cr, "8 event round-trip", () -> scenarioEvent(cr, t));
		run(cr, "9 ping", () -> scenarioPing(cr, t));
		run(cr, "10 event bus emit/broadcast", () -> scenarioEvents(cr, t));
		run(cr, "11 cacher with TTL", () -> scenarioCache(cr, t));
		run(cr, "12 complex user structure", () -> scenarioUsers(cr, t));
		run(cr, "13 metadata visibility", () -> scenarioMeta(cr, t));
		run(cr, "14 binary streaming", () -> scenarioStreaming(cr, t));

		return cr;
	}

	// ===================================================================
	//  Scenarios — each is a clean cross-language call sample
	// ===================================================================

	/** 1. Discovery: read the remote node + services through the {@code $node} service. */
	private void scenarioDiscovery(CheckRunner cr, long t) throws Exception {
		// $node.list returns every known node (local + remote), aggregated locally.
		Tree nodes = broker.call("$node.list").waitFor(t);
		boolean remoteAvailable = false;
		String remoteFramework = "";
		for (Tree node : nodes) {
			if (REMOTE_NODE.equals(node.get("id", ""))) {
				remoteAvailable = node.get("available", false);
				remoteFramework = node.get("client.type", "");
			}
		}
		cr.check("1a $node.list shows remote node '" + REMOTE_NODE + "' (available)",
				remoteAvailable, "remote node not present/available");
		cr.check("1b $node.list reports remote framework = " + REMOTE_FRAMEWORK,
				REMOTE_FRAMEWORK.equals(remoteFramework), "got '" + remoteFramework + "'");

		// $node.services aggregates the services advertised by every node.
		Tree services = broker.call("$node.services").waitFor(t);
		boolean hasMath = false;
		boolean hasData = false;
		for (Tree svc : services) {
			String name = svc.get("name", "");
			hasMath |= MATH.equals(name);
			hasData |= DATA.equals(name);
		}
		cr.check("1c $node.services lists remote '" + MATH + "' and '" + DATA + "'",
				hasMath && hasData, "mathNode=" + hasMath + ", dataNode=" + hasData);
	}

	/** 2. Primitive request/response. */
	private void scenarioPrimitiveCall(CheckRunner cr, long t) throws Exception {
		Tree addParams = new Tree();
		addParams.put("a", 2);
		addParams.put("b", 3);
		int sum = broker.call(MATH + ".add", addParams).waitFor(t).asInteger();
		cr.check("2a " + MATH + ".add({a:2,b:3}) == 5", sum == 5, "got " + sum);

		Tree greetParams = new Tree();
		greetParams.put("name", "Ada");
		String greeting = broker.call(MATH + ".greet", greetParams).waitFor(t).asString();
		cr.check("2b " + MATH + ".greet({name:'Ada'}) ends with '" + GREET_SUFFIX + "'",
				greeting != null && greeting.endsWith(GREET_SUFFIX), "got '" + greeting + "'");
	}

	/** 3. List round-trip (nested arrays survive). */
	private void scenarioList(CheckRunner cr, long t) throws Exception {
		Tree list = broker.call(DATA + ".getList").waitFor(t);

		boolean shapeOk = list.isList() && list.size() == 2;
		boolean rolesOk = shapeOk;
		for (Tree item : list) {
			Tree roles = item.get("roles");
			if (item.get("id", -1) < 0 || item.get("name", "").isEmpty()
					|| roles == null || !roles.isList() || roles.isEmpty()) {
				rolesOk = false;
			}
		}
		cr.check("3a " + DATA + ".getList is an array of length 2", shapeOk,
				"isList=" + list.isList() + ", size=" + list.size());
		cr.check("3b " + DATA + ".getList nested 'roles' arrays survived", rolesOk,
				"one or more items missing id/name/roles");
	}

	/** 4. Map round-trip (nested keys/values match). */
	private void scenarioMap(CheckRunner cr, long t) throws Exception {
		Tree map = broker.call(DATA + ".getMap").waitFor(t);
		cr.check("4a getMap.server.lang == 'node'", "node".equals(map.get("server.lang", "")),
				"got '" + map.get("server.lang", "") + "'");
		cr.check("4b getMap.server.version is present", !map.get("server.version", "").isEmpty(),
				"got '" + map.get("server.version", "") + "'");
		cr.check("4c getMap.counts.users == 2", map.get("counts.users", -1) == 2,
				"got " + map.get("counts.users", -1));
		cr.check("4d getMap.counts.active == 1", map.get("counts.active", -1) == 1,
				"got " + map.get("counts.active", -1));
		cr.check("4e getMap.enabled == true", map.get("enabled", false), "got " + map.get("enabled", false));
	}

	/** 5. Every JSON-safe primitive type comes back with the right type and value. */
	private void scenarioPrimitives(CheckRunner cr, long t) throws Exception {
		Tree p = broker.call(DATA + ".getPrimitives").waitFor(t);

		cr.check("5a getPrimitives.i == 42 (int)", p.get("i", -1) == 42, "got " + p.get("i", -1));
		cr.check("5b getPrimitives.big == 9007199254740991 (large int)",
				p.get("big", -1L) == 9007199254740991L, "got " + p.get("big", -1L));
		double d = p.get("d", 0.0);
		cr.check("5c getPrimitives.d == 3.5 (double)", d == 3.5 && d != Math.floor(d), "got " + d);
		cr.check("5d getPrimitives.b == true (boolean)", p.get("b", false), "got " + p.get("b", false));
		cr.check("5e getPrimitives.s == 'hello' (string)", "hello".equals(p.get("s", "")),
				"got '" + p.get("s", "") + "'");
		Tree n = p.get("n");
		cr.check("5f getPrimitives.n is a real null", n != null && n.isNull(),
				"got " + (n == null ? "<missing>" : n.asObject()));
	}

	/** 6. Deep echo: a rich nested object round-trips intact in both directions. */
	private void scenarioDeepEcho(CheckRunner cr, long t) throws Exception {
		Tree sent = buildRichPayload();
		Tree received = broker.call(DATA + ".echo", sent).waitFor(t);
		cr.check("6 " + DATA + ".echo deep-equals the sent nested object",
				cr.deepEquals(sent, received),
				"sent " + sent.toString(false) + " but got " + received.toString(false));
	}

	/** 7. System characteristics: introspect the remote node's runtime via {@code $node.health}. */
	private void scenarioSystemInfo(CheckRunner cr, long t) throws Exception {
		// Target the remote node explicitly so we read ITS health, not ours.
		Tree health = broker.call("$node.health", new Tree(), CallOptions.nodeID(REMOTE_NODE).timeout(t)).waitFor(t);

		String framework = health.get("client.type", "");
		String version = health.get("client.version", "");
		long cores = health.get("cpu.cores", 0L);
		String osType = health.get("os.type", "");
		long uptime = health.get("process.uptime", -1L);
		long heapUsed = health.get("process.memory.heapUsed", 0L);

		cr.check("7a remote $node.health reports framework = " + REMOTE_FRAMEWORK,
				REMOTE_FRAMEWORK.equals(framework), "got '" + framework + "'");
		cr.check("7b remote health exposes framework version", !version.isEmpty(), "got '" + version + "'");
		cr.check("7c remote health exposes CPU cores", cores > 0, "got " + cores);
		cr.check("7d remote health exposes OS type", !osType.isEmpty(), "got '" + osType + "'");
		cr.check("7e remote health exposes process uptime", uptime >= 0, "got " + uptime);
		cr.check("7f remote health exposes memory (heapUsed)", heapUsed > 0, "got " + heapUsed);
	}

	/** 8. Event round-trip: emit to Node, then read it back via {@code dataNode.lastEvent}. */
	private void scenarioEvent(CheckRunner cr, long t) throws Exception {
		Tree payload = new Tree();
		payload.put("from", "java");
		payload.put("n", 7);
		broker.broadcast(EVENT_TO_NODE, payload);

		// Events are fire-and-forget, so poll briefly for delivery.
		Tree got = null;
		for (int i = 0; i < 20; i++) {
			got = broker.call(DATA + ".lastEvent").waitFor(t);
			if (got != null && !got.isNull() && "java".equals(got.get("from", ""))) {
				break;
			}
			Thread.sleep(250);
		}
		boolean ok = got != null && !got.isNull()
				&& "java".equals(got.get("from", "")) && got.get("n", -1) == 7;
		cr.check("8 event '" + EVENT_TO_NODE + "' received by Node (dataNode.lastEvent)", ok,
				"got " + (got == null ? "<null>" : got.toString(false)));
	}

	/** 9. Ping: probe the remote node with {@code broker.ping} and read the timing back. */
	private void scenarioPing(CheckRunner cr, long t) throws Exception {
		// broker.ping(timeout, nodeID) resolves to a Tree carrying the remote
		// node's timing ("time"/"arrived"); a timeout would reject the promise.
		Tree pong = broker.ping(t, REMOTE_NODE).waitFor(t + 2000);
		cr.check("9a broker.ping('" + REMOTE_NODE + "') resolved a response",
				pong != null && !pong.isNull(), "got " + (pong == null ? "<null>" : pong.toString(false)));
		long arrived = pong == null ? 0L : pong.get("arrived", 0L);
		cr.check("9b ping response carries remote timing (arrived > 0)", arrived > 0L,
				"got arrived=" + arrived);
	}

	/** 10. Event bus: prove both {@code emit} and {@code broadcast} cross to Node. */
	private void scenarioEvents(CheckRunner cr, long t) throws Exception {
		Tree emitPayload = new Tree();
		emitPayload.put("via", "emit");
		emitPayload.put("n", 10);
		broker.emit(EMIT_TO_NODE, emitPayload);          // load-balanced to one listener per group

		Tree bcastPayload = new Tree();
		bcastPayload.put("via", "broadcast");
		bcastPayload.put("n", 11);
		broker.broadcast(BROADCAST_TO_NODE, bcastPayload); // delivered to all listeners on all nodes

		// Events are fire-and-forget, so poll the remote stats until both arrive.
		Tree stats = null;
		boolean emitOk = false;
		boolean bcastOk = false;
		for (int i = 0; i < 20; i++) {
			stats = broker.call(DATA + ".eventStats").waitFor(t);
			emitOk = stats != null && "emit".equals(stats.get("lastEmit.via", ""));
			bcastOk = stats != null && "broadcast".equals(stats.get("lastBroadcast.via", ""));
			if (emitOk && bcastOk) {
				break;
			}
			Thread.sleep(250);
		}
		cr.check("10a emit '" + EMIT_TO_NODE + "' received by Node (eventStats.lastEmit)", emitOk,
				"got " + (stats == null ? "<null>" : stats.toString(false)));
		cr.check("10b broadcast '" + BROADCAST_TO_NODE + "' received by Node (eventStats.lastBroadcast)",
				bcastOk, "got " + (stats == null ? "<null>" : stats.toString(false)));
	}

	/** 11. Cacher with TTL: prove cache hit, per-key caching and TTL expiry through Node. */
	private void scenarioCache(CheckRunner cr, long t) throws Exception {
		// Two calls with the SAME id within the TTL: the second is a cache hit,
		// so the remote action body does not run and the seq is unchanged.
		Tree id1 = new Tree();
		id1.put("id", 1);
		int seqA = broker.call(DATA + ".getCachedSeq", id1).waitFor(t).get("seq", -1);
		int seqB = broker.call(DATA + ".getCachedSeq", id1).waitFor(t).get("seq", -1);
		cr.check("11a getCachedSeq{id:1} twice returns same seq (cache hit)", seqA > 0 && seqA == seqB,
				"got seqA=" + seqA + ", seqB=" + seqB);

		// A different id is a different cache key, so the body runs again.
		Tree id2 = new Tree();
		id2.put("id", 2);
		int seqC = broker.call(DATA + ".getCachedSeq", id2).waitFor(t).get("seq", -1);
		cr.check("11b getCachedSeq{id:2} returns a different seq (key matters)", seqC != seqA,
				"got seqC=" + seqC + " (id:1 was " + seqA + ")");

		// After the TTL (2 s) expires, the entry is evicted and the body re-runs.
		Thread.sleep(4000);
		int seqD = broker.call(DATA + ".getCachedSeq", id1).waitFor(t).get("seq", -1);
		cr.check("11c getCachedSeq{id:1} after TTL returns a fresh seq (expiry)", seqD > seqA,
				"got seqD=" + seqD + " (was " + seqA + ")");
	}

	/** 12. Life-like nested structure: a users list with address/geo, email + role arrays and phone objects. */
	private void scenarioUsers(CheckRunner cr, long t) throws Exception {
		Tree result = broker.call(DATA + ".getUsers").waitFor(t);
		Tree users = result.get("users");

		boolean countOk = users != null && users.isList() && users.size() == 2;
		cr.check("12a getUsers returns 2 users", countOk,
				"got " + (users == null ? "<null>" : users.size() + " users"));

		double lat = result.get("users[0].address.geo.lat", 0.0);
		cr.check("12b users[0].address.geo.lat == 51.5074 (nested object)", lat == 51.5074, "got " + lat);

		Tree emails = result.get("users[0].emails");
		boolean emailsOk = emails != null && emails.isList() && emails.size() == 2
				&& "ada@analytical.io".equals(emails.get(0).asString());
		cr.check("12c users[0].emails is a string array, intact", emailsOk,
				"got " + (emails == null ? "<null>" : emails.toString(false)));

		Tree phones = result.get("users[1].phones");
		boolean phonesOk = phones != null && phones.isList() && phones.size() == 1
				&& "work".equals(phones.get(0).get("type", ""))
				&& !phones.get(0).get("number", "").isEmpty();
		cr.check("12d users[1].phones is an array of objects, intact", phonesOk,
				"got " + (phones == null ? "<null>" : phones.toString(false)));

		// Strongest proof: the whole structure deep-equals what we expect.
		cr.check("12e getUsers deep-equals the expected nested structure",
				cr.deepEquals(buildExpectedUsers(), result),
				"got " + result.toString(false));
	}

	/**
	 * 13. Metadata: prove the remote node <b>sees the metadata we send</b>, and
	 * that response metadata is <b>merged back</b> into the caller.
	 * <p>
	 * In {@code moleculer-java} the metadata travels alongside the params and is
	 * read with {@code ctx.params.getMeta()}; in Moleculer JS it is {@code ctx.meta}.
	 * Both serialize to the request's top-level {@code meta} field, so they are
	 * interchangeable across the language boundary.
	 */
	private void scenarioMeta(CheckRunner cr, long t) throws Exception {
		// A structured meta block: scalars plus a nested list and a nested map.
		Tree sentMeta = new Tree();
		sentMeta.put("tenant", "acme");
		sentMeta.put("trace", "trace-001");
		sentMeta.put("n", 7);
		sentMeta.putList("tags").add("a").add("b");
		sentMeta.putMap("ctx").put("k", "v");

		// Attach it as the call's meta (the params themselves stay empty here).
		Tree params = new Tree();
		params.getMeta().copyFrom(sentMeta);

		Tree rsp = broker.call(DATA + ".echoMeta", params).waitFor(t);

		// 13a: the remote echoed back, as response data, exactly the meta we sent.
		Tree received = rsp.get("receivedMeta");
		cr.check("13a " + DATA + ".echoMeta saw the meta we sent (remote reads our metadata)",
				cr.deepEquals(sentMeta, received),
				"sent " + sentMeta.toString(false) + " but got " + (received == null ? "<null>" : received.toString(false)));

		// 13b: the remote's response meta (its 'seenBy' marker) merged back to us.
		Tree rspMeta = rsp.getMeta();
		String seenBy = rspMeta == null ? "" : rspMeta.get("seenBy", "");
		cr.check("13b response meta merged back to caller (seenBy='node')",
				"node".equals(seenBy),
				"got meta=" + (rspMeta == null ? "<null>" : rspMeta.toString(false)));
	}

	/**
	 * 14. Streaming: prove <b>binary data crosses the language boundary intact</b>,
	 * in both directions. First a dynamically generated buffer is <i>sent</i> to the
	 * remote {@code receiveStream} (which returns the byte count + SHA-256 it saw);
	 * then a stream is <i>downloaded</i> from the remote {@code produceStream} and
	 * its content verified.
	 * <p>
	 * A streamed request opens with empty {@code params}; any side-data must travel
	 * in {@code meta}, never in {@code params} (a non-empty first packet would be
	 * treated as stream content by the receiver).
	 */
	private void scenarioStreaming(CheckRunner cr, long t) throws Exception {
		long streamTimeout = t + 8000;

		// --- 14a/b: SEND a dynamically generated binary stream to the remote ---
		int sendSize = 100_000;
		byte[] sent = randomBytes(sendSize);
		String sentHash = sha256Hex(sent);

		PacketStream out = broker.createStream();
		Promise ackPromise = broker.call(DATA + ".receiveStream", out); // stream only, no params
		out.transferFrom(new ByteArrayInputStream(sent));               // chunked send, auto-closes
		Tree ack = ackPromise.waitFor(streamTimeout);

		cr.check("14a " + DATA + ".receiveStream received all " + sendSize + " bytes",
				ack.get("bytes", -1L) == sendSize, "got " + ack.get("bytes", -1L));
		cr.check("14b " + DATA + ".receiveStream SHA-256 matches (bytes intact end-to-end)",
				sentHash.equals(ack.get("sha256", "")),
				"expected " + sentHash + " but got " + ack.get("sha256", ""));

		// --- 14c/d: DOWNLOAD a binary stream produced by the remote ---
		int prodSize = 64 * 1024;
		Tree prodParams = new Tree();
		prodParams.put("size", prodSize);
		Tree rsp = broker.call(DATA + ".produceStream", prodParams).waitFor(t);

		PacketStream in = (PacketStream) rsp.asObject();
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		in.transferTo(buffer).waitFor(streamTimeout);
		byte[] got = buffer.toByteArray();

		cr.check("14c " + DATA + ".produceStream produced " + prodSize + " bytes",
				got.length == prodSize, "got " + got.length + " bytes");
		cr.check("14d " + DATA + ".produceStream content matches the expected pattern (SHA-256)",
				sha256Hex(got).equals(sha256Hex(deterministicBytes(prodSize))),
				"downloaded content did not match the deterministic pattern");
	}

	// ===================================================================
	//  Helpers
	// ===================================================================

	/** Random binary payload for the streaming send test (dynamically generated). */
	private byte[] randomBytes(int size) {
		byte[] b = new byte[size];
		rnd.nextBytes(b);
		return b;
	}

	/** Deterministic content ({@code byte[i] == i & 0xFF}); mirrors the Node.js {@code produceStream}. */
	private static byte[] deterministicBytes(int size) {
		byte[] b = new byte[size];
		for (int i = 0; i < size; i++) {
			b[i] = (byte) (i & 0xFF);
		}
		return b;
	}

	/** Lower-case hex SHA-256 of the given bytes (matches Node's {@code createHash('sha256').digest('hex')}). */
	private static String sha256Hex(byte[] data) throws Exception {
		return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
	}

	/** Builds a rich, nested, JSON-safe object for the deep-echo test. */
	private static Tree buildRichPayload() {
		Tree p = new Tree();
		p.put("msg", "ping");
		p.put("when", 123);
		p.put("ok", true);
		p.putObject("nada", null);

		Tree nums = p.putList("nums");
		nums.add(1);
		nums.add(2);
		nums.add(3);

		Tree items = p.putList("items");
		Tree first = items.addMap();
		first.put("id", 1);
		Tree firstTags = first.putList("tags");
		firstTags.add("x");
		firstTags.add("y");
		Tree second = items.addMap();
		second.put("id", 2);
		second.putList("tags"); // intentionally empty

		p.putMap("nested").putMap("a").putMap("b").put("c", "deep");
		return p;
	}

	/**
	 * Builds the {@code users} structure we expect {@code dataNode.getUsers} to
	 * return &mdash; the by-value mirror of the Node.js (and Java) implementation.
	 * Used for the scenario-12 deep-equality check.
	 */
	private static Tree buildExpectedUsers() {
		Tree root = new Tree();
		Tree users = root.putList("users");

		Tree ada = users.addMap();
		ada.put("id", 1);
		ada.put("name", "Ada Lovelace");
		ada.put("active", true);
		Tree adaAddr = ada.putMap("address");
		adaAddr.put("street", "12 Analytical Ave");
		adaAddr.put("city", "London");
		adaAddr.put("zip", "EC1A 1AA");
		Tree adaGeo = adaAddr.putMap("geo");
		adaGeo.put("lat", 51.5074);
		adaGeo.put("lng", -0.1278);
		ada.putList("emails").add("ada@analytical.io").add("lovelace@maths.org");
		ada.putList("roles").add("admin").add("author");
		Tree adaPhones = ada.putList("phones");
		adaPhones.addMap().put("type", "home").put("number", "+44-20-7946-0001");
		adaPhones.addMap().put("type", "mobile").put("number", "+44-7700-900002");

		Tree linus = users.addMap();
		linus.put("id", 2);
		linus.put("name", "Linus Torvalds");
		linus.put("active", false);
		Tree linusAddr = linus.putMap("address");
		linusAddr.put("street", "1 Kernel Way");
		linusAddr.put("city", "Portland");
		linusAddr.put("zip", "97201");
		Tree linusGeo = linusAddr.putMap("geo");
		linusGeo.put("lat", 45.5152);
		linusGeo.put("lng", -122.6784);
		linus.putList("emails").add("linus@kernel.org");
		linus.putList("roles").add("maintainer");
		linus.putList("phones").addMap().put("type", "work").put("number", "+1-503-555-0100");

		return root;
	}

	private interface ThrowingRunnable {
		void run() throws Exception;
	}

	/** Runs one scenario, turning any thrown exception into a single FAIL line. */
	private void run(CheckRunner cr, String label, ThrowingRunnable body) {
		try {
			body.run();
		} catch (Exception cause) {
			cr.fail("scenario " + label, "threw " + cause);
		}
	}

	private void finish(CheckRunner cr) {
		System.out.println("-----------------------------------------------------------");
		System.out.println("Java -> Node result: " + cr.getPassed() + " passed, " + cr.getFailed() + " failed");
		System.out.println("===========================================================");
		System.out.println();

		if (exitAfterVerify) {
			int code = cr.getFailed() > 0 ? 1 : 0;
			System.out.println("Exiting Java node with code " + code + " (demo.exit-after-verify=true).");
			System.exit(SpringApplication.exit(appContext, () -> code));
		} else {
			System.out.println("Java node staying up so the Node.js side can call back into it. Press Ctrl+C to stop.");
			startKeepAlive();
		}
	}

	/**
	 * Keeps the JVM alive (a non-web Spring Boot app would otherwise exit once
	 * start-up finishes) so the Node.js side can keep calling into Java. A
	 * Ctrl+C still triggers a clean shutdown (Spring closes the context and
	 * stops the broker).
	 */
	private void startKeepAlive() {
		Thread keepAlive = new Thread(() -> {
			try {
				new CountDownLatch(1).await();
			} catch (InterruptedException ignored) {
				Thread.currentThread().interrupt();
			}
		}, "demo-keep-alive");
		keepAlive.setDaemon(false);
		keepAlive.start();
	}

}
