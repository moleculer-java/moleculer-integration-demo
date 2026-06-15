package integration.demo.services;

import services.moleculer.service.Action;
import services.moleculer.service.Service;

/**
 * Demo service {@code "mathJava"} &mdash; simple request/response actions that the
 * Node.js side calls to prove primitive values cross the language boundary.
 * <p>
 * In {@code moleculer-java} a service extends {@link Service} and its actions are
 * declared as {@code public} {@link Action} fields (lambdas {@code ctx -> ...}),
 * discovered by reflection. {@code ctx.params} is an {@link io.datatree.Tree};
 * an action may return a {@code Tree}, a boxed primitive, a {@code String} or
 * {@code null}.
 * <p>
 * Mirror of the Node.js service {@code mathNode} in
 * {@code node/services/math-node.service.js}.
 */
public class MathJavaService extends Service {

	public MathJavaService() {
		super("mathJava");
	}

	/**
	 * {@code mathJava.add} &mdash; adds two integers.
	 * Example: {@code {a: 2, b: 3}} &rarr; {@code 5}.
	 */
	public Action add = ctx ->
			ctx.params.get("a", 0) + ctx.params.get("b", 0); // int + int -> int

	/**
	 * {@code mathJava.greet} &mdash; returns a greeting string.
	 * Example: {@code {name: "Ada"}} &rarr; {@code "Hello Ada from Java!"}.
	 */
	public Action greet = ctx ->
			"Hello " + ctx.params.get("name", "world") + " from Java!";

}
