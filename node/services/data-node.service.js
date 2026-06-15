"use strict";

/**
 * Demo service "dataNode" — the structured-data and event side of the Node.js
 * node. Its actions hand back lists, nested objects and every JSON-safe
 * primitive type so the Java side can prove that structured data survives the
 * cross-language round trip intact.
 *
 * Everything returned here is JSON-safe (plain objects, arrays, numbers,
 * strings, booleans and null). We deliberately avoid Date, Map, Set, BigInt and
 * Buffer, because the native JSON serializer used on both sides cannot represent
 * them identically.
 *
 * Mirror of the Java service `DataJavaService` (service name "dataJava").
 */
module.exports = {
	name: "dataNode",

	actions: {
		/**
		 * dataNode.getList — a list of objects with mixed fields, including
		 * nested `roles` arrays, to test list round-tripping.
		 */
		getList(ctx) {
			return [
				{ id: 1, name: "Ada", roles: ["admin", "dev"] },
				{ id: 2, name: "Linus", roles: ["dev"] }
			];
		},

		/**
		 * dataNode.getMap — a nested object to test that nested keys and values
		 * survive the round trip.
		 */
		getMap(ctx) {
			return {
				server: { lang: "node", version: process.versions.node },
				counts: { users: 2, active: 1 },
				enabled: true
			};
		},

		/**
		 * dataNode.getPrimitives — one object containing each JSON-safe primitive
		 * type, so the caller can assert type and value: an int, a large (still
		 * JS-safe) integer, a double, a boolean, a string and a real null.
		 */
		getPrimitives(ctx) {
			return {
				i: 42,                  // int
				big: 9007199254740991,  // large integer (Number.MAX_SAFE_INTEGER)
				d: 3.5,                 // double (exactly representable)
				b: true,                // boolean
				s: "hello",             // string
				n: null                 // real JSON null
			};
		},

		/**
		 * dataNode.echo — returns ctx.params unchanged. The caller sends a rich
		 * nested object and asserts the response deep-equals what it sent: the
		 * strongest single proof that structured data crosses intact.
		 */
		echo(ctx) {
			return ctx.params;
		},

		/**
		 * dataNode.lastEvent — returns the payload last received via the
		 * "demo.fromJava" event (or null if none yet), letting a verifier emit an
		 * event and then read it back.
		 */
		lastEvent(ctx) {
			return this.lastEventPayload ?? null;
		},

		/**
		 * dataNode.getCachedSeq — a cached action (scenario 11) proving the Cacher
		 * works across the language boundary. The result is cached by the "id"
		 * param for `ttl` seconds. Caching happens on the node that OWNS the action
		 * (here, Node.js), so a remote Java caller benefits from this cache too.
		 *
		 * `seq` only advances on a real (uncached) invocation, so two calls with
		 * the same id within the TTL return the same seq (cache hit), a different
		 * id returns a fresh seq (the key matters), and a call after the TTL
		 * expired returns a larger seq (the entry was evicted).
		 */
		getCachedSeq: {
			cache: { keys: ["id"], ttl: 2 },
			handler(ctx) {
				return { id: ctx.params.id, seq: ++this.cacheSeq };
			}
		},

		/**
		 * dataNode.eventStats — reports how many emitted and broadcast events this
		 * node has received (and the last payload of each), so a verifier can prove
		 * both delivery modes crossed from Java (scenario 10).
		 */
		eventStats(ctx) {
			return {
				emitCount: this.emitCount,
				broadcastCount: this.broadcastCount,
				lastEmit: this.lastEmit ?? null,
				lastBroadcast: this.lastBroadcast ?? null
			};
		},

		/**
		 * dataNode.getUsers — a small, life-like list of users (scenario 12) to
		 * prove a deeply nested structure survives the cross-language round trip:
		 * a list of objects, each with a nested `address` (holding a `geo`
		 * sub-object), an `emails` array of strings, a `roles` array of strings and
		 * a `phones` array of objects. By-value mirror of the Java getUsers.
		 */
		getUsers(ctx) {
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
	},

	events: {
		/**
		 * Event listener for "demo.fromJava" (emitted by the Java side). Stores
		 * the payload so it can be read back through the lastEvent action.
		 */
		"demo.fromJava"(ctx) {
			this.lastEventPayload = ctx.params;
		},

		/** Listener for an emitted event from the Java side (scenario 10). */
		"demo.emit.fromJava"(ctx) {
			this.lastEmit = ctx.params;
			this.emitCount++;
		},

		/** Listener for a broadcast event from the Java side (scenario 10). */
		"demo.broadcast.fromJava"(ctx) {
			this.lastBroadcast = ctx.params;
			this.broadcastCount++;
		}
	},

	created() {
		// Holds the last payload received from the "demo.fromJava" event.
		this.lastEventPayload = null;

		// Event-bus statistics (scenario 10).
		this.emitCount = 0;
		this.broadcastCount = 0;
		this.lastEmit = null;
		this.lastBroadcast = null;

		// Real (uncached) invocation counter for getCachedSeq (scenario 11).
		this.cacheSeq = 0;
	}
};
