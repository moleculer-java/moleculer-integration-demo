package integration.demo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.InetSocketAddress;
import java.net.Socket;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import integration.demo.verify.CheckRunner;
import integration.demo.verify.JavaToNodeVerifier;
import services.moleculer.ServiceBroker;

/**
 * Optional JUnit 5 integration test that boots the Spring context (and thus the
 * broker), waits for the Node.js side, runs the same {@link JavaToNodeVerifier}
 * scenarios and asserts that <b>zero</b> checks failed.
 * <p>
 * It needs both a running NATS server <em>and</em> a running Node.js node, so it
 * <b>self-skips</b> (via JUnit assumptions) when either is unavailable &mdash;
 * mirroring how the workspace excludes broker-dependent tests by default. It
 * runs in the {@code verify} phase via the failsafe plugin, so plain
 * {@code mvn clean package} never touches the cluster.
 * <p>
 * The auto-verify behaviour is switched off ({@code demo.auto-verify=false}) so
 * the test drives the verifier itself, and a distinct {@code nodeID} is used so
 * it never collides with a separately-running {@code java-node}.
 */
@SpringBootTest(properties = {
		"demo.auto-verify=false",
		"demo.exit-after-verify=false",
		"demo.node-id=java-node-it",
		"demo.wait-timeout-ms=15000"
})
class JavaToNodeIT {

	@Autowired
	private ServiceBroker broker;

	@Autowired
	private JavaToNodeVerifier verifier;

	@Test
	void allScenariosPass() {
		// Skip cleanly if NATS is not reachable.
		assumeTrue(natsReachable(), "NATS not reachable at localhost:4222 — skipping integration test");

		// Skip cleanly if the Node.js side is not up.
		boolean nodeUp;
		try {
			broker.waitForServices(15000, "mathNode", "dataNode").waitFor(17000);
			nodeUp = true;
		} catch (Exception cause) {
			nodeUp = false;
		}
		assumeTrue(nodeUp, "Node.js side (mathNode/dataNode) not discovered — skipping integration test");

		CheckRunner result = verifier.runScenarios();
		assertEquals(0, result.getFailed(),
				result.getFailed() + " cross-language check(s) failed (" + result.getPassed() + " passed)");
	}

	private static boolean natsReachable() {
		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress("localhost", 4222), 1500);
			return true;
		} catch (Exception cause) {
			return false;
		}
	}

}
