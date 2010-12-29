package net.rhapso.koa.bag;

import net.rhapso.koa.BaseTreeTestCase;
import net.rhapso.koa.tree.Value;

import java.util.Iterator;

public class MappedMultiValuesTest extends BaseTreeTestCase {
    public void testMappedMultiValues() throws Exception {
        MappedMultiValues mappedMultiValues = MappedMultiValues.initialize(makeAddressable());
        MultiValueRef multiValueRef = mappedMultiValues.create();
        mappedMultiValues.append(value("foo"), multiValueRef);
        mappedMultiValues.append(value("poop"), multiValueRef);
        mappedMultiValues.append(value("hello"), multiValueRef);
        assertEquals(3, mappedMultiValues.count(multiValueRef));
        Iterator<Value> values = mappedMultiValues.get(multiValueRef);
        assertEquals(value("foo"), values.next());
        assertEquals(value("poop"), values.next());
        assertEquals(value("hello"), values.next());
        assertFalse(values.hasNext());
    }
}
