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

public class LeafNodeSplitterTest extends BaseTreeTestCase {
    private NodeFactory nodeFactory;

    public void testSplit() throws Exception {
        LeafNode source = nodeFactory.newLeafNode();
        source.setNextLeafNode(new NodeRef(randomLong));
        LeafNode destination = nodeFactory.newLeafNode();

        for (int key : new int[]{10, 2, 6, 8, 4}) {
            source.put(key(key), value(key));
        }

        new LeafNodeSplitter().splitInto(nodeFactory, source, destination);
        assertEquals("[2] [4] [6]", source.keys().toString());
        assertEquals("[2] [4] [6]", source.values().toString(nodeFactory));
        assertEquals("[8] [10]", destination.keys().toString());
        assertEquals("[8] [10]", destination.values().toString(nodeFactory));
        assertEquals(destination.getNodeRef(), source.getNextLeafNode());
        assertEquals(new NodeRef(randomLong), destination.getNextLeafNode());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        nodeFactory = NodeFactory.initialize(new MemoryAddressable(1000), new BlockSize(500), new Order(5));
    }
}