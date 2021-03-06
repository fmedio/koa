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

import net.rhapso.koa.BaseTreeTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class LeafNodeTest extends BaseTreeTestCase {
    private LeafNode leafNode;
    private NodeFactory nodeFactory;

    @Test
    public void testReference() throws Exception {
        for (int key : new int[]{6, 2, 4}) {
            leafNode.put(key(key), value(key));
        }

        KeyRef keyRef = leafNode.referenceOf(key(2));
        assertEquals(2, leafNode.key(keyRef).bytes()[0]);
        assertNull(leafNode.referenceOf(key(42)));
    }

    @Test
    public void testCursorAtOrAfter() throws Exception {
        for (String key : new String[]{"b", "bc", "bbb"}) {
            leafNode.put(key(key), value(key));
        }

        assertEquals("b bbb bc", readCursor(leafNode.cursorAtOrAfter(key("a"))));
        assertEquals("b bbb bc", readCursor(leafNode.cursorAtOrAfter(key("b"))));
        assertEquals("bc", readCursor(leafNode.cursorAtOrAfter(key("bc"))));
        assertEquals("", readCursor(leafNode.cursorAtOrAfter(key("cc"))));
    }

    @Test
    public void testCursorAt() throws Exception {
        for (String key : new String[]{"b", "bc", "bbb"}) {
            leafNode.put(key(key), value(key));
        }

        Iterator iterator = leafNode.cursorAt(key(0));
        assertFalse(iterator.hasNext());

        assertEquals("b bbb bc", readCursor(leafNode.cursorAt(key("b"))));
        assertEquals("", readCursor(leafNode.cursorAt(key("c"))));
    }

    @Test
    public void testPut() throws Exception {
        for (int key : new int[]{6, 2, 4}) {
            leafNode.put(key(key), value(key));
        }

        assertEquals("[2] [4] [6]", leafNode.keys().toString());
        assertEquals("[2] [4] [6]", leafNode.values().toString(nodeFactory));
        assertEquals(value(2), leafNode.get(key(2)));
        assertEquals(value(4), leafNode.get(key(4)));
        assertEquals(value(6), leafNode.get(key(6)));
        assertNull(leafNode.get(key(42)));
    }

    @Test
    public void testUpdate() throws Exception {
        boolean didUpdate = leafNode.put(key(1), value(1)).didUpdate;
        assertEquals(value(1), leafNode.get(key(1)));
        assertFalse(didUpdate);

        didUpdate = leafNode.put(key(1), value(2)).didUpdate;
        assertEquals(value(2), leafNode.get(key(1)));
        assertTrue(didUpdate);
    }

    @Test
    public void testSplit() throws Exception {
        LeafNodeSplitter splitter = mock(LeafNodeSplitter.class);

        for (int key : new int[]{6, 2, 4}) {
            leafNode.put(key(key), value(key), splitter);
        }

        verifyZeroInteractions(splitter);
        leafNode.put(key(1), value(1), splitter);
        verify(splitter, times(1)).splitInto(eq(nodeFactory), eq(leafNode), any(LeafNode.class));
    }

    @Test
    public void testLeafNodeIsRootNode() throws Exception {
        leafNode = nodeFactory.newLeafNode();
        InsertionResult result = leafNode.put(key(1), value(1));
        assertEquals(leafNode.getNodeRef(), result.newRoot);
    }

    @Test
    public void testLiveParent() throws Exception {
        NodeRef parent = new NodeRef(42);
        leafNode = nodeFactory.newLeafNode();
        leafNode.setParent(parent);
        InsertionResult result = leafNode.put(key(1), value(1));
        assertEquals(parent, result.newRoot);
    }

    @Before
    public void setUp() throws Exception {
        nodeFactory = NodeFactory.initialize(makeAddressable(), new Order(3));
        leafNode = nodeFactory.newLeafNode();
    }

}
