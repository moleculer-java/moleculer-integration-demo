package integration.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point of the <b>Java side</b> of the Moleculer integration demo.
 * <p>
 * This is a plain (non-web) Spring Boot application. On start-up it builds and
 * starts one {@code moleculer-java} {@link services.moleculer.ServiceBroker}
 * (see {@link BrokerConfig}), registers a couple of demo services
 * ({@code mathJava} and {@code dataJava}) and joins a Moleculer cluster over
 * NATS. Once the Node.js node's services are discovered, the
 * {@link integration.demo.verify.JavaToNodeVerifier} runs an automated set of
 * cross-language checks and prints {@code [PASS]} / {@code [FAIL]} lines.
 * <p>
 * The two nodes are wire-compatible because both speak Moleculer protocol v5
 * (moleculer-java 2.0.0's default, matching Moleculer JS 0.15) with the JSON
 * serializer.
 */
@SpringBootApplication
public class IntegrationDemoApplication {

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(IntegrationDemoApplication.class);

		// This node speaks to the cluster over NATS; it does not serve HTTP,
		// so there is no embedded web server to start.
		app.setWebApplicationType(WebApplicationType.NONE);

		app.run(args);
	}

}
