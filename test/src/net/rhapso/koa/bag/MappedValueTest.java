package net.rhapso.koa.bag;

import net.rhapso.koa.BaseTreeTestCase;
import net.rhapso.koa.tree.Value;

public class MappedValueTest extends BaseTreeTestCase {
    private MappedValue mappedValue;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        mappedValue = new MappedValue(makeAddressable());
        mappedValue.write(new MappedValueRef(0), new Value("foo"));
    }

    public void testReadWrite() throws Exception {
        assertEquals(value("foo"), mappedValue.read(new MappedValueRef(0)));
    }

    public void testGetSetNext() throws Exception {
        MappedValueRef ref = new MappedValueRef(0);
        assertEquals(0l, mappedValue.getNext(ref).asLong());
        mappedValue.setNext(ref, new MappedValueRef(randomLong));
        assertEquals(randomLong, mappedValue.getNext(ref).asLong());
    }

    public void testStorageSize() throws Exception {
        assertEquals(17, mappedValue.storageSize(new Value("hello")).asLong());
    }
}
