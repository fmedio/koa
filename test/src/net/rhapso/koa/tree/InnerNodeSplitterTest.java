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

import static org.junit.Assert.assertEquals;

public class InnerNodeSplitterTest extends BaseTreeTestCase {

    @Test
    public void testSplit() throws Exception {
        InnerNode source = nodeFactory.newInnerNode(NodeRef.NULL);
        InnerNode destination = nodeFactory.newInnerNode(NodeRef.NULL);

        source.insertChild(nodeFactory.newInnerNode(source.getParent()));
        for (int key : new int[]{10, 2, 4, 8, 6}) {
            InnerNode child = nodeFactory.newInnerNode(source.getParent());
            source.add(nodeFactory.append(key(key)), child);
        }

        InnerNode result = new InnerNodeSplitter().splitInto(nodeFactory, source, destination);

        assertEquals("[2] [4]", source.keys().toString());
        assertEquals("[8] [10]", destination.keys().toString());
        assertEquals(source.getParent(), destination.getParent());

        Node parent = nodeFactory.read(source.getParent());
        assertEquals("[6]", parent.keys().toString());

        assertEquals(3, source.children().size());
        assertEquals(3, destination.children().size());

        assertEquals(parent.getNodeRef(), result.getNodeRef());
    }

    @Before
    public void setUp() throws Exception {
        nodeFactory = NodeFactory.initialize(makeAddressable(), new Order(5));
    }

    private NodeFactory nodeFactory;
}
