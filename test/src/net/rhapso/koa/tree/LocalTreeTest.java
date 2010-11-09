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

import static org.mockito.Mockito.*;

public class LocalTreeTest extends BaseTreeTestCase {
    private NodeFactory nodeFactory;
    private TreeControl treeControl;
    private NodeRef nodeRef;
    private Node root;
    private LocalTree tree;
    private Key key;
    private Value value;


    public void testPut() throws Exception {
        NodeRef newRoot = mock(NodeRef.class);

        when(treeControl.getRootNode()).thenReturn(nodeRef);
        when(nodeFactory.read(nodeRef)).thenReturn(root);
        when(root.put(key, value)).thenReturn(newRoot);

        tree.put(key, value);

        verify(root, times(1)).put(key, value);
        verify(treeControl, times(1)).setRootNode(newRoot);
        verify(treeControl, times(1)).incrementCount();
    }

    public void testContains() throws Exception {
        when(treeControl.getRootNode()).thenReturn(nodeRef);
        when(nodeFactory.read(nodeRef)).thenReturn(root);
        when(root.contains(key)).thenReturn(true);

        boolean result = tree.contains(key);

        verify(root, times(1)).contains(key);
        assertEquals(true, result);
    }

    public void testCreateRootIfNeeded() throws Exception {
        LeafNode leaf = mock(LeafNode.class);

        when(treeControl.getRootNode()).thenReturn(NodeRef.NULL);
        when(nodeFactory.newLeafNode()).thenReturn(leaf);

        Node node = tree.obtainRoot();

        assertSame(leaf, node);
        verify(treeControl, times(1)).setRootNode(leaf.getNodeRef());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        nodeFactory = mock(NodeFactory.class);
        treeControl = mock(TreeControl.class);
        nodeRef = mock(NodeRef.class);
        root = mock(Node.class);
        key = mock(Key.class);
        value = new Value(randomLong);

        tree = new LocalTree(nodeFactory, treeControl);
    }
}
