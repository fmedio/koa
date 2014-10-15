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
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class MappedMultiValuesTest extends BaseTreeTestCase {

    @Test
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
