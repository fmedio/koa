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
import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.Offset;
import net.rhapso.koa.storage.StorageSize;

import static org.mockito.Mockito.*;

public class NodeFactoryTest extends BaseTreeTestCase {
    private Addressable addressable;
    private NodeFactory nodeFactory;
    private TreeControl treeControl;
    private Order order;

    public void testInitialize() {
        Addressable addressable = makeAddressable();
        NodeFactory.initialize(addressable, new Order(2));
        NodeFactory factory = new NodeFactory(addressable);
        Tree tree = new LocalTree(factory, factory.getTreeControl());
        for (String string : new String[]{"a", "bb", "aaa", "fffff", "ab", "bbbb", "f", "ba"}) {
            tree.put(key(string), value(string));
        }
        String result = readCursor(tree.cursorAtOrAfter(key("a")));
        assertEquals("a aaa ab ba bb bbbb f fffff", result);
    }

    public void testWriteKey() throws Exception {
        Addressable addressable = makeAddressable();
        TreeControl treeControl = TreeControl.initialize(addressable, new Order(3));
        nodeFactory = new NodeFactory(addressable, treeControl);
        KeyRef left = nodeFactory.append(new Key(new byte[]{1, 2, 3}));
        KeyRef right = nodeFactory.append(new Key(new byte[]{4, 5, 6, 7}));
        assertEquals(new byte[]{1, 2, 3}, nodeFactory.readKey(left).bytes());
        assertEquals(new byte[]{4, 5, 6, 7}, nodeFactory.readKey(right).bytes());
    }

    public void testReadInnerNode() throws Exception {
        when(addressable.read()).thenReturn((int) Scribe.NodeType.innerNode.asByte());

        Node node = nodeFactory.read(new Offset(randomLong));

        verify(addressable, times(1)).read();
        assertTrue(node instanceof InnerNode);
    }

    public void testReadLeafNode() throws Exception {
        when(addressable.read()).thenReturn((int) Scribe.NodeType.leafNode.asByte());

        Node node = nodeFactory.read(new Offset(randomLong));

        verify(addressable, times(1)).read();
        assertTrue(node instanceof LeafNode);
    }

    public void testNewInnerNode() throws Exception {
        InnerNode innerNode = nodeFactory.newInnerNode(new NodeRef(randomLong));
        verify(addressable, times(1)).write(Scribe.NodeType.innerNode.asByte());
        verify(treeControl, times(1)).allocate(new InnerNodeScribe(order).storageSize());
        verify(addressable, atLeastOnce()).write(any(byte[].class));
    }

    public void testNewLeafNode() throws Exception {
        when(addressable.read()).thenReturn((int) Scribe.NodeType.leafNode.asByte());
        LeafNode leafNode = nodeFactory.newLeafNode();
        leafNode.setParent(new NodeRef(randomLong));
        verify(addressable, times(1)).write(Scribe.NodeType.leafNode.asByte());
        verify(treeControl, times(1)).allocate(new LeafNodeScribe(order).storageSize());
        verify(addressable, atLeastOnce()).write(any(byte[].class));
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        addressable = mock(Addressable.class);
        treeControl = mock(TreeControl.class);
        order = new Order(3);
        when(treeControl.getOrder()).thenReturn(order);
        when(treeControl.allocate(any(StorageSize.class))).thenReturn(new Offset(randomLong));
        nodeFactory = new NodeFactory(addressable, treeControl);
    }
}
