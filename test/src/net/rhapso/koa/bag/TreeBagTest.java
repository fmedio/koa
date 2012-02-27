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
import net.rhapso.koa.tree.Key;
import net.rhapso.koa.tree.Value;

import java.util.Iterator;

public class TreeBagTest extends BaseTreeTestCase {
    private TreeBag tree;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        tree = TreeBag.initialize(memoryTree(), makeAddressable());
        tree.put(key("foo"), value("one"));
        tree.put(key("foo"), value("two"));
        tree.put(key("bar"), value("one"));
    }

    public void testTruncate() throws Exception {
        tree.truncate();
    }

    public void testInsert() throws Exception {
        assertEquals(value("one"), tree.get(key("foo")));
        Iterator<Value> values = tree.getValues(key("foo"));
        assertEquals(value("one"), values.next());
        assertEquals(value("two"), values.next());
        assertFalse(values.hasNext());

        Iterator<Value> empty = tree.getValues(key("not here"));
        assertFalse(empty.hasNext());
    }

    public void testCursor() throws Exception {
        Iterator<Key> iterator = tree.cursorAtOrAfter(new Key(new byte[0]));
        assertEquals(key("bar"), iterator.next());
        assertEquals(key("foo"), iterator.next());
        assertFalse(iterator.hasNext());
    }
}
