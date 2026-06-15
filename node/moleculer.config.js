"use strict";

/**
 * Moleculer broker configuration for the Node.js side of the integration demo.
 *
 * The three settings that make this node interoperable with the Java node are:
 *   - transporter: the shared NATS bus both nodes connect to,
 *   - serializer:  "JSON" (the default on both frameworks; the common denominator
 *                  that makes moleculer-java and Moleculer JS wire-compatible),
 *   - nodeID:      a UNIQUE id ("node-node") that must differ from the Java node's
 *                  ("java-node"); colliding ids break cluster discovery.
 *
 * This node uses Moleculer JS 0.15, which speaks wire protocol "5" and silently
 * drops packets stamped with a different "ver". The Java side is aligned to "5"
 * (moleculer-java 2.0.0's default), so no extra protocol configuration is needed
 * here on the Node side.
 */
module.exports = {
	// Unique id of this node within the cluster.
	nodeID: "node-node",

	// Connect to the same NATS server as the Java node.
	transporter: process.env.DEMO_NATS_URL || "nats://localhost:4222",

	// JSON is the default serializer; set explicitly to mirror the Java side.
	serializer: "JSON",

	// In-memory cacher, used by actions that declare `cache: {...}` (scenario 11).
	// Caching happens on the node that owns the action, so a remote Java caller of
	// dataNode.getCachedSeq benefits from this cache too. Per-action `ttl` overrides.
	cacher: "Memory",

	logger: true,
	logLevel: "info"
};
