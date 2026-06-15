"use strict";

/**
 * Starts the Node.js Moleculer node and keeps it running, WITHOUT running any
 * checks. Use this when you just want a live Node node that the Java side can
 * call into (`npm run serve`).
 *
 * For the automated cross-language verification, use `npm start` / `npm run
 * verify`, which runs ./verify/node-to-java.js instead.
 */
const { ServiceBroker } = require("moleculer");
const config = require("./moleculer.config.js");

const broker = new ServiceBroker(config);

// Register the demo services so the Java node can call into them.
broker.createService(require("./services/math-node.service.js"));
broker.createService(require("./services/data-node.service.js"));

broker.start().then(() => {
	broker.logger.info("Node node is up. Registered services: mathNode, dataNode.");
	broker.logger.info("Press Ctrl+C to stop.");
});

// Clean shutdown on Ctrl+C.
process.on("SIGINT", async () => {
	await broker.stop();
	process.exit(0);
});
