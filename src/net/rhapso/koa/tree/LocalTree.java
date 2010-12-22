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

import net.rhapso.koa.StorageFactory;
import net.rhapso.koa.storage.Addressable;

import java.util.Iterator;

public class LocalTree implements Tree {
    private final NodeFactory nodeFactory;
    private final TreeControl treeControl;


    public static LocalTree open(StoreName storeName, StorageFactory storageFactory) {
        if (storageFactory.exists(storeName)) {
            NodeFactory factory = new NodeFactory(storageFactory.openAddressable(storeName));
            return new LocalTree(factory, factory.getTreeControl());
        } else {
            Addressable addressable = storageFactory.openAddressable(storeName);
            NodeFactory nodeFactory = NodeFactory.initialize(addressable, storageFactory.getOrder());
            return new LocalTree(nodeFactory, nodeFactory.getTreeControl());
        }
    }

    public LocalTree(NodeFactory nodeFactory, TreeControl treeControl) {
        this.nodeFactory = nodeFactory;
        this.treeControl = treeControl;
    }

    @Override
    public boolean put(Key key, Value value) {
        NodeRef newRoot = obtainRoot().put(key, value);
        treeControl.incrementCount();
        treeControl.setRootNode(newRoot);
        return true;
    }

    @Override
    public Batch createBatch() {
        return new SequentialBatch(this);
    }

    @Override
    public Value get(Key key) {
        return obtainRoot().get(key);
    }

    @Override
    public Iterator cursorAt(Key key) {
        return obtainRoot().cursorAt(key);
    }

    @Override
    public Iterator cursorAtOrAfter(Key key) {
        return obtainRoot().cursorAtOrAfter(key);
    }

    @Override
    public boolean contains(Key key) {
        return obtainRoot().contains(key);
    }

    Node obtainRoot() {
        NodeRef nodeRef = treeControl.getRootNode();

        if (nodeRef.isNull()) {
            LeafNode root = nodeFactory.newLeafNode();
            treeControl.setRootNode(root.getNodeRef());
            return root;
        } else {
            return nodeFactory.read(nodeRef);
        }
    }

    @Override
    public long count() {
        return treeControl.count();
    }

    @Override
    public KeyRef referenceOf(Key key) {
        return obtainRoot().referenceOf(key);
    }

    @Override
    public Key key(KeyRef ref) {
        return obtainRoot().key(ref);
    }
}
