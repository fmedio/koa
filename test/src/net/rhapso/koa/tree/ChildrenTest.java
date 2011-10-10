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

import com.google.common.base.Joiner;
import net.rhapso.koa.BaseTreeTestCase;
import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.Offset;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;

public class ChildrenTest extends BaseTreeTestCase {
    private NodeFactory nodeFactory;
    private Order order;

    public void testInitialize() throws Exception {
        Addressable addressable = mock(Addressable.class);
        NodeFactory nodeFactory = mock(NodeFactory.class);

        Children children = Children.initialize(nodeFactory, addressable, new Offset(randomLong), order);
        verify(addressable, times(1)).write(randomLong, new byte[Children.storageSize(order).intValue()]);
    }

    public void testStorageSize() throws Exception {
        assertEquals(52, Children.storageSize(new Order(4)).intValue());
    }

    public void testSplitHasSideEffect() throws Exception {
        Children children = new Children(nodeFactory, makeAddressable(), new Offset(0), order);
        for (int i = 0; i < 5; i++) {
            children.add(i, makeNode(i + 1));
        }
        Children destination = new Children(nodeFactory, makeAddressable(), new Offset(0), order);
        children.splitInto(destination);
        assertEquals("3 4 5", destination.toString());
        assertEquals("1 2", children.toString());
    }

    public void testInsert() throws Exception {
        Children children = new Children(nodeFactory, makeAddressable(), new Offset(0), order);
        children.add(makeNode(2));
        children.add(makeNode(4));
        assertEquals("0 2 4", children.add(0, makeNode(0)).toString());
        assertEquals("0 2 3 4", children.add(2, makeNode(3)).toString());
        assertEquals("0 2 3 4 5", children.add(4, makeNode(5)).toString());
    }

    public void testIterable() throws Exception {
        Children children = new Children(nodeFactory, makeAddressable(), new Offset(0), order);
        for (int i = 0; i < 5; i++) {
            children.add(i, makeNode(i + 1));
        }

        assertEquals("1 2 3 4 5", Joiner.on(" ").join(children));
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        nodeFactory = mock(NodeFactory.class);
        order = new Order(42);
        when(nodeFactory.getOrder()).thenReturn(order);
        when(nodeFactory.read(any(NodeRef.class))).thenAnswer(new Answer<Node>() {
            @Override
            public Node answer(InvocationOnMock invocationOnMock) throws Throwable {
                NodeRef arg = (NodeRef) invocationOnMock.getArguments()[0];
                return makeNode((int) arg.asLong());
            }
        });
    }

    private Node makeNode(int display) {
        Node node = mock(Node.class);
        when(node.toString()).thenReturn(Integer.toString(display));
        NodeRef nodeRef = new NodeRef(display);
        when(node.getNodeRef()).thenReturn(nodeRef);
        return node;
    }
}
