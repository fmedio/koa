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
import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.Offset;
import net.rhapso.koa.storage.StorageSize;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class NodeFactoryTest extends BaseTreeTestCase {
    private Addressable addressable;
    private NodeFactory nodeFactory;
    private TreeControl treeControl;
    private Order order;
    private long randomLong = 42421343;

    @Test
    public void testInitialize() {
        Addressable addressable = makeAddressable();
        NodeFactory.initialize(addressable, new Order(2));
        NodeFactory factory = new NodeFactory(addressable);
        Tree tree = new Koa(factory, factory.getTreeControl());
        for (String string : new String[]{"a", "bb", "aaa", "fffff", "ab", "bbbb", "f", "ba"}) {
            tree.put(key(string), value(string));
        }
        String result = readCursor(tree.cursorAtOrAfter(key("a")));
        assertEquals("a aaa ab ba bb bbbb f fffff", result);
    }

    @Test
    public void testWriteKey() throws Exception {
        Addressable addressable = makeAddressable();
        TreeControl treeControl = TreeControl.initialize(addressable, new Order(3));
        nodeFactory = new NodeFactory(addressable, treeControl);
        KeyRef left = nodeFactory.append(new Key(new byte[]{1, 2, 3}));
        KeyRef right = nodeFactory.append(new Key(new byte[]{4, 5, 6, 7}));
        assertArrayEquals(new byte[]{1, 2, 3}, nodeFactory.readKey(left).bytes());
        assertArrayEquals(new byte[]{4, 5, 6, 7}, nodeFactory.readKey(right).bytes());
    }

    @Test
    public void testReadInnerNode() throws Exception {
        when(addressable.read(anyLong())).thenReturn((int) Scribe.NodeType.innerNode.asByte());

        Node node = nodeFactory.read(new Offset(randomLong));

        verify(addressable, times(1)).read(anyLong());
        assertTrue(node instanceof InnerNode);
    }

    @Test
    public void testReadLeafNode() throws Exception {
        when(addressable.read(anyLong())).thenReturn((int) Scribe.NodeType.leafNode.asByte());

        Node node = nodeFactory.read(new Offset(randomLong));

        verify(addressable, times(1)).read(anyLong());
        assertTrue(node instanceof LeafNode);
    }

    @Before
    public void setUp() throws Exception {
        addressable = mock(Addressable.class);
        treeControl = mock(TreeControl.class);
        order = new Order(3);
        when(treeControl.getOrder()).thenReturn(order);
        when(treeControl.allocate(any(StorageSize.class))).thenReturn(new Offset(randomLong));
        nodeFactory = new NodeFactory(addressable, treeControl);
    }
}
