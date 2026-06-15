package integration.demo.verify;

import io.datatree.Tree;

/**
 * Tiny, dependency-free check helper used by the cross-language verifier.
 * <p>
 * It prints a clear {@code [PASS]} / {@code [FAIL]} line per scenario, keeps a
 * running pass/fail tally and offers a numeric-aware {@link #deepEquals(Tree, Tree)}
 * so structured {@link Tree} payloads can be compared after a JSON round trip
 * (where, for example, an {@code int} may come back as a {@code long}).
 * <p>
 * It is intentionally small so the verifier code reads like a documentation
 * sample rather than a test-framework exercise.
 */
public final class CheckRunner {

	private int passed;
	private int failed;

	/** Records a passing check and prints a {@code [PASS]} line. */
	public void pass(String name) {
		passed++;
		System.out.println("[PASS] " + name);
	}

	/** Records a failing check and prints a {@code [FAIL]} line with details. */
	public void fail(String name, String detail) {
		failed++;
		System.out.println("[FAIL] " + name + " — " + detail);
	}

	/** Passes or fails {@code name} depending on {@code condition}. */
	public void check(String name, boolean condition, String detailIfFail) {
		if (condition) {
			pass(name);
		} else {
			fail(name, detailIfFail);
		}
	}

	/** Convenience: assert two values are equal (numeric-aware for numbers). */
	public void checkEquals(String name, Object expected, Object actual) {
		boolean eq = scalarEquals(expected, actual);
		check(name, eq, "expected " + expected + ", got " + actual);
	}

	public int getPassed() {
		return passed;
	}

	public int getFailed() {
		return failed;
	}

	/**
	 * Deeply compares two {@link Tree} values after a JSON round trip:
	 * maps are compared key-by-key, lists element-by-element (in order) and
	 * scalars by value. Numbers are compared by numeric value so that, e.g.,
	 * {@code 42} sent as an {@code int} still equals {@code 42} returned as a
	 * {@code long}.
	 */
	public boolean deepEquals(Tree a, Tree b) {
		if (a == null || b == null) {
			return a == b;
		}
		boolean an = a.isNull();
		boolean bn = b.isNull();
		if (an || bn) {
			return an && bn;
		}
		boolean am = a.isMap();
		boolean bm = b.isMap();
		boolean al = a.isList();
		boolean bl = b.isList();
		if (am != bm || al != bl) {
			return false;
		}
		if (am) {
			if (a.size() != b.size()) {
				return false;
			}
			for (Tree childA : a) {
				Tree childB = b.get(childA.getName());
				if (childB == null || !deepEquals(childA, childB)) {
					return false;
				}
			}
			return true;
		}
		if (al) {
			if (a.size() != b.size()) {
				return false;
			}
			java.util.Iterator<Tree> ia = a.iterator();
			java.util.Iterator<Tree> ib = b.iterator();
			while (ia.hasNext() && ib.hasNext()) {
				if (!deepEquals(ia.next(), ib.next())) {
					return false;
				}
			}
			return true;
		}
		// Scalar leaf
		return scalarEquals(a.asObject(), b.asObject());
	}

	/** Value equality that treats all numbers numerically. */
	private static boolean scalarEquals(Object x, Object y) {
		if (x == null || y == null) {
			return x == y;
		}
		if (x instanceof Number && y instanceof Number) {
			return ((Number) x).doubleValue() == ((Number) y).doubleValue();
		}
		if (x instanceof Boolean || y instanceof Boolean) {
			return x.equals(y);
		}
		return x.toString().equals(y.toString());
	}

}
