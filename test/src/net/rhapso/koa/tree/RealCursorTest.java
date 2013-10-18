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

package net.rhapso.koa.tree;

import baggage.BaseTestCase;

import static org.mockito.Mockito.*;

public class RealCursorTest extends BaseTestCase {
    private LeafNode leafNode;
    private NodeFactory nodeFactory;

    public void testCursor() throws Exception {
        LeafNode next = mock(LeafNode.class);
        when(leafNode.size()).thenReturn(3);
        when(next.size()).thenReturn(3);
        when(leafNode.getNextLeafNode()).thenReturn(new NodeRef(randomLong));
        when(nodeFactory.read(new NodeRef(randomLong))).thenReturn(next);
        RealCursor cursor = new RealCursor(nodeFactory, leafNode, new KeyOffset(0));

        cursor.next();
        cursor.next();
        cursor.next();

        verify(leafNode, times(1)).get(0);
        verify(leafNode, times(1)).get(1);
        verify(leafNode, times(1)).get(2);

        cursor.next();

        verify(leafNode, times(1)).getNextLeafNode();
        verify(nodeFactory, times(1)).read(new NodeRef(randomLong));
        verify(next, times(1)).get(0);
    }

    public void testHasNext() throws Exception {
        Key key = mock(Key.class);

        when(leafNode.size()).thenReturn(3);
        when(leafNode.get(anyInt())).thenReturn(key);
        when(leafNode.getNextLeafNode()).thenReturn(NodeRef.NULL);

        RealCursor cursor = new RealCursor(nodeFactory, leafNode, new KeyOffset(0));
        assertTrue(cursor.hasNext());
        cursor.next();
        assertTrue(cursor.hasNext());
        cursor.next();
        assertTrue(cursor.hasNext());
        cursor.next();
        assertFalse(cursor.hasNext());
    }

    @Override
    protected void setUp() throws Exception {
        leafNode = mock(LeafNode.class);
        nodeFactory = mock(NodeFactory.class);
    }
}
