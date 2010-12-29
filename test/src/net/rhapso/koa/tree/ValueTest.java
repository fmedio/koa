package net.rhapso.koa.tree;

import junit.framework.TestCase;

public class ValueTest extends TestCase {
    public void testAsLong() throws Exception {
        Value value = new Value(new LongValue(42).asBytes());
        assertEquals(42l, value.asLong());
    }
}
