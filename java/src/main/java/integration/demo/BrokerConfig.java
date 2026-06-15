package integration.demo;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import integration.demo.services.DataJavaService;
import integration.demo.services.MathJavaService;
import services.moleculer.ServiceBroker;
import services.moleculer.cacher.MemoryCacher;
import services.moleculer.config.ServiceBrokerConfig;
import services.moleculer.serializer.JsonSerializer;
import services.moleculer.transporter.NatsTransporter;

/**
 * Creates and configures the single {@link ServiceBroker} of the Java node.
 * <p>
 * The broker is exposed as a Spring bean so other components (for example the
 * {@link integration.demo.verify.JavaToNodeVerifier}) can inject it. Spring
 * starts the broker for us via {@code initMethod = "start"} and stops it
 * cleanly on shutdown via {@code destroyMethod = "stop"}.
 *
 * <h2>Why these settings?</h2>
 * <ul>
 *   <li><b>nodeID</b> {@code "java-node"} &mdash; must differ from the Node.js
 *       node's id ({@code "node-node"}); colliding ids break cluster discovery.</li>
 *   <li><b>NATS transporter</b> &mdash; the shared message bus both nodes connect
 *       to ({@code nats://localhost:4222}).</li>
 *   <li><b>JSON serializer</b> &mdash; the default on both frameworks. Setting it
 *       explicitly documents the common denominator that makes a Java node and a
 *       Node.js node wire-compatible out of the box.</li>
 * </ul>
 */
@Configuration
public class BrokerConfig {

	/** This node's unique id within the cluster. */
	@Value("${demo.node-id:java-node}")
	private String nodeID;

	/** URL of the shared NATS server both nodes connect to. */
	@Value("${demo.nats-url:nats://localhost:4222}")
	private String natsUrl;

	/**
	 * Moleculer wire-protocol version to speak. Moleculer JS <b>0.15</b> uses
	 * protocol <b>"5"</b> and silently rejects packets stamped with a different
	 * version. moleculer-java 2.0.0 also defaults to "5", so this is normally
	 * redundant &mdash; it is set explicitly to document the one knob you must
	 * align when integrating, and so the demo keeps working against a legacy
	 * Moleculer JS 0.14 node by setting {@code demo.protocol-version=4}.
	 */
	@Value("${demo.protocol-version:5}")
	private String protocolVersion;

	/**
	 * Builds, configures and (via {@code initMethod}) starts the broker.
	 *
	 * @return the started {@link ServiceBroker}
	 */
	@Bean(initMethod = "start", destroyMethod = "stop")
	public ServiceBroker serviceBroker() {

		// --- Broker configuration ---
		ServiceBrokerConfig cfg = new ServiceBrokerConfig();
		cfg.setNodeID(nodeID);

		// Speak the same protocol version as the Node.js side (see field doc).
		// First-class per-broker setting; no global System property needed.
		cfg.setProtocolVersion(protocolVersion);

		// --- Transporter: NATS with the JSON serializer ---
		NatsTransporter nats = new NatsTransporter(natsUrl);
		nats.setSerializer(new JsonSerializer()); // JSON is the default; set explicitly for clarity
		cfg.setTransporter(nats);

		// --- Cacher: in-memory, used by @Cache-annotated actions (scenario 11) ---
		// Caching happens on the node that OWNS the action, so a remote Node.js
		// caller of dataJava.getCachedSeq benefits from this cache too. Args:
		// capacity per partition, default TTL in seconds (0 = none; per-action
		// @Cache(ttl=...) overrides), cleanup interval in seconds (1 = evict
		// expired entries promptly, keeping the TTL test deterministic).
		cfg.setCacher(new MemoryCacher(2048, 0, 1));

		// --- Create the broker and register the demo services ---
		// Services must be registered BEFORE the broker starts.
		ServiceBroker broker = new ServiceBroker(cfg);
		broker.createService(new MathJavaService());
		broker.createService(new DataJavaService());

		return broker; // Spring calls broker.start() (initMethod) right after this
	}

}
