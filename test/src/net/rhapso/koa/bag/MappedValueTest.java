/*
 * The MIT License
 *
 * Copyright (c) 2010 Fabrice Medio
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

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
