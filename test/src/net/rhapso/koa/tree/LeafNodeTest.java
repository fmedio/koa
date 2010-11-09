/*
 * The MIT License
 *
 * Copyright (c) 2010 Fabrice Medio <fmedio@gmail.com>
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
import net.rhapso.koa.storage.BlockSize;
import net.rhapso.koa.storage.MemoryAddressable;

import static org.mockito.Mockito.*;

public class LeafNodeTest extends BaseTreeTestCase {
    private LeafNode leafNode;
    private NodeFactory nodeFactory;

    public void testReference() throws Exception {
        for (int key : new int[]{6, 2, 4}) {
            leafNode.put(key(key), value(key));
        }

        KeyRef keyRef = leafNode.referenceOf(key(2));
        assertEquals(2, leafNode.key(keyRef).bytes()[0]);
        assertNull(leafNode.referenceOf(key(42)));
    }

    public void testCursorAtOrAfter() throws Exception {
        for (String key : new String[]{"b", "bc", "bbb"}) {
            leafNode.put(key(key), value(key));
        }

        assertEquals("b bbb bc", readCursor(leafNode.cursorAtOrAfter(key("a"))));
        assertEquals("b bbb bc", readCursor(leafNode.cursorAtOrAfter(key("b"))));
        assertEquals("bc", readCursor(leafNode.cursorAtOrAfter(key("bc"))));
        assertEquals("", readCursor(leafNode.cursorAtOrAfter(key("cc"))));
    }

    public void testCursorAt() throws Exception {
        for (String key : new String[]{"b", "bc", "bbb"}) {
            leafNode.put(key(key), value(key));
        }

        Cursor cursor = leafNode.cursorAt(key(0));
        assertTrue(cursor == Cursor.NULL);

        assertEquals("b bbb bc", readCursor(leafNode.cursorAt(key("b"))));
        assertEquals("", readCursor(leafNode.cursorAt(key("c"))));
    }

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

    public void testSplit() throws Exception {
        LeafNodeSplitter splitter = mock(LeafNodeSplitter.class);

        for (int key : new int[]{6, 2, 4}) {
            leafNode.put(key(key), value(key), splitter);
        }

        verifyZeroInteractions(splitter);
        leafNode.put(key(1), value(1), splitter);
        verify(splitter, times(1)).splitInto(eq(nodeFactory), eq(leafNode), any(LeafNode.class));
    }

    public void testLeafNodeIsRootNode() throws Exception {
        leafNode = nodeFactory.newLeafNode();
        NodeRef result = leafNode.put(key(1), value(1));
        assertEquals(leafNode.getNodeRef(), result);
    }

    public void testLiveParent() throws Exception {
        NodeRef parent = new NodeRef(randomLong);
        leafNode = nodeFactory.newLeafNode();
        leafNode.setParent(parent);
        NodeRef result = leafNode.put(key(1), value(1));
        assertEquals(parent, result);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        nodeFactory = NodeFactory.initialize(new MemoryAddressable(1000), BlockSize.DEFAULT, new Order(3));
        leafNode = nodeFactory.newLeafNode();
    }


}
