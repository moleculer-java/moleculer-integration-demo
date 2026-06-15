"use strict";

/**
 * Demo service "mathNode" — simple request/response actions that the Java side
 * calls to prove primitive values cross the language boundary.
 *
 * Mirror of the Java service `MathJavaService` (service name "mathJava").
 * An action is addressed as `<serviceName>.<actionName>` regardless of which
 * language implements it — that is the whole point of the demo.
 */
module.exports = {
	name: "mathNode",

	actions: {
		/**
		 * mathNode.add — adds two integers.
		 * Example: { a: 2, b: 3 } -> 5
		 */
		add(ctx) {
			return Number(ctx.params.a) + Number(ctx.params.b);
		},

		/**
		 * mathNode.greet — returns a greeting string.
		 * Example: { name: "Ada" } -> "Hello Ada from Node!"
		 */
		greet(ctx) {
			return `Hello ${ctx.params.name ?? "world"} from Node!`;
		}
	}
};
