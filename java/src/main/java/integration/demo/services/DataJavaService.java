package integration.demo.services;

import java.io.ByteArrayInputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.datatree.Promise;
import io.datatree.Tree;
import services.moleculer.cacher.Cache;
import services.moleculer.eventbus.Listener;
import services.moleculer.eventbus.Subscribe;
import services.moleculer.service.Action;
import services.moleculer.service.Service;
import services.moleculer.stream.PacketStream;

/**
 * Demo service {@code "dataJava"} &mdash; the structured-data and event side of the
 * Java node. Its actions hand back lists, nested maps and every JSON-safe
 * primitive type so the Node.js side can prove that structured data survives the
 * cross-language round trip intact.
 * <p>
 * Everything returned here is <b>JSON-safe</b> (plain maps, lists, numbers,
 * strings, booleans and {@code null}). We deliberately avoid {@code Date},
 * binary data, etc., because the native JSON serializer used on both sides
 * cannot represent them identically.
 * <p>
 * Mirror of the Node.js service {@code dataNode} in
 * {@code node/services/data-node.service.js}.
 */
public class DataJavaService extends Service {

	/**
	 * Last payload received from the {@code demo.fromNode} event. Written by the
	 * event listener thread and read by the {@code lastEvent} action thread, so
	 * it is {@code volatile}.
	 */
	private volatile Tree lastEventPayload;

	// --- Event-bus statistics (scenario 10: emit vs broadcast) ---
	// Separate counters/payloads for the two delivery modes let the caller prove
	// that BOTH an emit() and a broadcast() crossed the language boundary.
	private final AtomicInteger emitCount = new AtomicInteger();
	private final AtomicInteger broadcastCount = new AtomicInteger();
	private volatile Tree lastEmit;
	private volatile Tree lastBroadcast;

	/**
	 * Real (uncached) invocation counter for {@link #getCachedSeq} (scenario 11).
	 * It increments only when the action body actually runs, so a cache <i>hit</i>
	 * (body skipped) returns the same {@code seq} as the first call, and a value
	 * served after the TTL expired returns a larger {@code seq}.
	 */
	private final AtomicInteger cacheSeq = new AtomicInteger();

	public DataJavaService() {
		super("dataJava");
	}

	/**
	 * {@code dataJava.getList} &mdash; returns a list of objects with mixed fields,
	 * including nested {@code roles} arrays, to test list round-tripping.
	 */
	public Action getList = ctx -> {
		Tree root = new Tree();
		Tree list = root.putList("list");

		Tree ada = list.addMap();
		ada.put("id", 1);
		ada.put("name", "Ada");
		Tree adaRoles = ada.putList("roles");
		adaRoles.add("admin");
		adaRoles.add("dev");

		Tree linus = list.addMap();
		linus.put("id", 2);
		linus.put("name", "Linus");
		Tree linusRoles = linus.putList("roles");
		linusRoles.add("dev");

		return list; // serialized as a JSON array
	};

	/**
	 * {@code dataJava.getMap} &mdash; returns a nested map/object to test that nested
	 * keys and values survive the round trip.
	 */
	public Action getMap = ctx -> {
		Tree root = new Tree();

		Tree server = root.putMap("server");
		server.put("lang", "java");
		server.put("version", System.getProperty("java.specification.version", "21"));

		Tree counts = root.putMap("counts");
		counts.put("users", 2);
		counts.put("active", 1);

		root.put("enabled", true);
		return root;
	};

	/**
	 * {@code dataJava.getPrimitives} &mdash; returns one object containing each
	 * JSON-safe primitive type, so the caller can assert type and value:
	 * an int, a large (still JS-safe) integer, a double, a boolean, a string
	 * and a real {@code null}.
	 */
	public Action getPrimitives = ctx -> {
		Tree out = new Tree();
		out.put("i", 42);                        // int
		out.put("big", 9007199254740991L);       // large integer (Number.MAX_SAFE_INTEGER)
		out.put("d", 3.5);                        // double (exactly representable)
		out.put("b", true);                       // boolean
		out.put("s", "hello");                    // string
		out.putObject("n", null);                 // real JSON null
		return out;
	};

	/**
	 * {@code dataJava.echo} &mdash; returns {@code ctx.params} unchanged. The caller
	 * sends a rich nested object and asserts the response deep-equals what it
	 * sent: the strongest single proof that structured data crosses intact.
	 */
	public Action echo = ctx -> ctx.params;

	/**
	 * {@code dataJava.lastEvent} &mdash; returns the payload last received via the
	 * {@code demo.fromNode} event (or {@code null} if none yet), letting a
	 * verifier emit an event and then read it back.
	 */
	public Action lastEvent = ctx -> lastEventPayload;

	/**
	 * Event listener for {@code demo.fromNode} (emitted by the Node.js side).
	 * Stores the payload so it can be read back through the {@code lastEvent}
	 * action.
	 */
	@Subscribe("demo.fromNode")
	public Listener onFromNode = ctx -> lastEventPayload = ctx.params;

	// ===================================================================
	//  Scenario 11 — Cacher with TTL
	// ===================================================================

	/**
	 * {@code dataJava.getCachedSeq} &mdash; a cached action used to prove the
	 * <b>Cacher</b> works across the language boundary. The result is cached by
	 * the {@code "id"} parameter for {@code ttl} seconds.
	 * <p>
	 * Caching happens on the node that <i>owns</i> the action (here, Java), so a
	 * remote Node.js caller still benefits from this cache. The returned
	 * {@code seq} comes from {@link #cacheSeq}, which only advances on a real
	 * (uncached) invocation &mdash; so two calls with the same {@code id} within
	 * the TTL return the same {@code seq} (a cache hit), a different {@code id}
	 * returns a fresh {@code seq} (the key matters), and a call after the TTL
	 * expired returns a larger {@code seq} (the entry was evicted).
	 */
	@Cache(keys = { "id" }, ttl = 2)
	public Action getCachedSeq = ctx -> {
		Tree out = new Tree();
		out.put("id", ctx.params.get("id", 0));
		out.put("seq", cacheSeq.incrementAndGet());
		return out;
	};

	// ===================================================================
	//  Scenario 12 — Life-like complex structure (Tree-API showcase)
	// ===================================================================

	/**
	 * {@code dataJava.getUsers} &mdash; returns a small, life-like list of users to
	 * prove that a deeply nested structure survives the cross-language round trip
	 * intact. It also serves as a copy-pasteable example of how to build such a
	 * structure with the {@link io.datatree.Tree} API: a list of maps, each with a
	 * nested {@code address} object (itself holding a {@code geo} sub-object), an
	 * {@code emails} array of strings, a {@code roles} array of strings and a
	 * {@code phones} array of objects.
	 * <p>
	 * The data is static and identical (by value) to the Node.js {@code getUsers},
	 * so the caller can assert a full deep-equality of the whole structure.
	 */
	public Action getUsers = ctx -> {
		Tree root = new Tree();
		Tree users = root.putList("users");

		// --- User #1 ---
		Tree ada = users.addMap();
		ada.put("id", 1);
		ada.put("name", "Ada Lovelace");
		ada.put("active", true);

		Tree adaAddr = ada.putMap("address");      // nested object
		adaAddr.put("street", "12 Analytical Ave");
		adaAddr.put("city", "London");
		adaAddr.put("zip", "EC1A 1AA");
		Tree adaGeo = adaAddr.putMap("geo");        // object nested inside an object
		adaGeo.put("lat", 51.5074);
		adaGeo.put("lng", -0.1278);

		Tree adaEmails = ada.putList("emails");     // array of strings
		adaEmails.add("ada@analytical.io");
		adaEmails.add("lovelace@maths.org");

		Tree adaRoles = ada.putList("roles");       // array of strings
		adaRoles.add("admin");
		adaRoles.add("author");

		Tree adaPhones = ada.putList("phones");     // array of objects
		Tree adaPhone1 = adaPhones.addMap();
		adaPhone1.put("type", "home");
		adaPhone1.put("number", "+44-20-7946-0001");
		Tree adaPhone2 = adaPhones.addMap();
		adaPhone2.put("type", "mobile");
		adaPhone2.put("number", "+44-7700-900002");

		// --- User #2 ---
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

		Tree linusEmails = linus.putList("emails");
		linusEmails.add("linus@kernel.org");

		Tree linusRoles = linus.putList("roles");
		linusRoles.add("maintainer");

		Tree linusPhones = linus.putList("phones");
		Tree linusPhone1 = linusPhones.addMap();
		linusPhone1.put("type", "work");
		linusPhone1.put("number", "+1-503-555-0100");

		return root; // { users: [ {...}, {...} ] }
	};

	// ===================================================================
	//  Scenario 10 — Event bus: emit vs broadcast
	// ===================================================================

	/**
	 * {@code dataJava.eventStats} &mdash; reports how many {@code emit}ted and
	 * {@code broadcast}ed events this node has received (and the last payload of
	 * each), so a verifier can prove both delivery modes crossed from Node.js.
	 */
	public Action eventStats = ctx -> {
		Tree out = new Tree();
		out.put("emitCount", emitCount.get());
		out.put("broadcastCount", broadcastCount.get());
		out.putObject("lastEmit", lastEmit == null ? null : lastEmit.asObject());
		out.putObject("lastBroadcast", lastBroadcast == null ? null : lastBroadcast.asObject());
		return out;
	};

	/** Listener for an {@code emit}ted event from the Node.js side (scenario 10). */
	@Subscribe("demo.emit.fromNode")
	public Listener onEmitFromNode = ctx -> {
		lastEmit = ctx.params;
		emitCount.incrementAndGet();
	};

	/** Listener for a {@code broadcast}ed event from the Node.js side (scenario 10). */
	@Subscribe("demo.broadcast.fromNode")
	public Listener onBroadcastFromNode = ctx -> {
		lastBroadcast = ctx.params;
		broadcastCount.incrementAndGet();
	};

	// ===================================================================
	//  Scenario 13 — Metadata visibility across the language boundary
	// ===================================================================

	/**
	 * {@code dataJava.echoMeta} &mdash; proves the two frameworks <b>see each
	 * other's metadata</b>. In {@code moleculer-java} the metadata rides alongside
	 * the params and is read with {@link io.datatree.Tree#getMeta() ctx.params.getMeta()};
	 * in Moleculer JS it is {@code ctx.meta}. On the wire both map to the request's
	 * top-level {@code meta} field, so they are interchangeable.
	 * <p>
	 * This action returns, as ordinary response <i>data</i>, an exact copy of the
	 * meta it received (so the caller can assert the remote saw what it sent), and
	 * it also adds a {@code seenBy} entry to the meta. Moleculer merges response
	 * meta back into the caller's context, so the caller can additionally assert
	 * that the meta round-tripped back &mdash; in both directions.
	 */
	public Action echoMeta = ctx -> {
		Tree meta = ctx.params.getMeta();

		// Echo back (as data) exactly what we received, BEFORE we touch the meta.
		Tree out = new Tree();
		out.putMap("receivedMeta").copyFrom(meta);

		// Add a marker to the meta; it is merged back into the caller's context.
		meta.put("seenBy", "java");
		return out;
	};

	// ===================================================================
	//  Scenario 14 — Streaming: binary transfer across the boundary
	// ===================================================================

	/**
	 * {@code dataJava.receiveStream} &mdash; <b>receives</b> a binary stream and
	 * reports how many bytes arrived and their SHA-256, so the caller can prove the
	 * exact bytes crossed intact. The stream is consumed in an event-driven manner
	 * ({@code ctx.stream.onPacket(...)}); the action returns a {@link Promise} that
	 * resolves only once the stream is fully closed.
	 * <p>
	 * Note on interop: a streamed request carries its first ("open") packet with
	 * empty {@code params} &mdash; any side-data must travel in the {@code meta}
	 * block, never in {@code params}, because the receiving stream would otherwise
	 * treat the params as stream content.
	 */
	public Action receiveStream = ctx -> {
		if (ctx.stream == null) {
			// Not invoked with a stream — return an empty digest.
			return digestResult(0L, emptySha256());
		}
		return new Promise(res -> {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			AtomicLong byteCount = new AtomicLong();
			ctx.stream.onPacket((bytes, cause, close) -> {
				if (bytes != null) {
					md.update(bytes);
					byteCount.addAndGet(bytes.length);
				}
				if (cause != null) {
					res.reject(cause);
				} else if (close) {
					res.resolve(digestResult(byteCount.get(), HexFormat.of().formatHex(md.digest())));
				}
			});
		});
	};

	/**
	 * {@code dataJava.produceStream} &mdash; <b>returns</b> a binary stream of
	 * {@code size} bytes (default 64&nbsp;KB) of deterministic content
	 * ({@code byte[i] == i & 0xFF}), so a remote caller can download it and verify
	 * the content. Returning a {@link PacketStream} from an action makes the
	 * framework stream the response back chunk-by-chunk.
	 */
	public Action produceStream = ctx -> {
		int size = ctx.params.get("size", 64 * 1024);
		byte[] content = deterministicBytes(size);

		PacketStream stream = ctx.createStream();
		// Push the bytes asynchronously, in packet-sized chunks; transferFrom
		// closes the stream when the source is exhausted.
		stream.transferFrom(new ByteArrayInputStream(content));
		return stream;
	};

	// --- Streaming/metadata helpers ---

	/** Builds the {@code { bytes, sha256 }} acknowledgement returned by {@link #receiveStream}. */
	private static Tree digestResult(long bytes, String sha256) {
		Tree out = new Tree();
		out.put("bytes", bytes);
		out.put("sha256", sha256);
		return out;
	}

	/** Deterministic content shared (by value) with the Node.js {@code produceStream}. */
	private static byte[] deterministicBytes(int size) {
		byte[] content = new byte[size];
		for (int i = 0; i < size; i++) {
			content[i] = (byte) (i & 0xFF);
		}
		return content;
	}

	/** SHA-256 hex of an empty byte stream (the {@code ctx.stream == null} fallback). */
	private static String emptySha256() throws Exception {
		return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(new byte[0]));
	}

}
