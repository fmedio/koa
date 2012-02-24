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

import net.rhapso.koa.StorageFactory;
import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.block.Cache;

import java.util.Iterator;

public class Koa implements Tree {
    private final NodeFactory nodeFactory;
    private final TreeControl treeControl;

    public static Koa open(StoreName storeName, StorageFactory storageFactory, Cache cache) {
        if (storageFactory.exists(storeName)) {
            Addressable addressable = storageFactory.openAddressable(storeName, cache);
            NodeFactory factory = new NodeFactory(addressable);
            return new Koa(factory, factory.getTreeControl());
        } else {
            Addressable addressable = storageFactory.openAddressable(storeName, cache);
            NodeFactory nodeFactory = NodeFactory.initialize(addressable, storageFactory.getOrder());
            return new Koa(nodeFactory, nodeFactory.getTreeControl());
        }
    }

    public Koa(NodeFactory nodeFactory, TreeControl treeControl) {
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

    // TODO: Use plain byte[] instead of typed representations
    @Override
    public Value get(Key key) {
        return obtainRoot().get(key);
    }

    @Override
    public Iterator<Key> cursorAt(Key key) {
        return obtainRoot().cursorAt(key);
    }

    @Override
    public Iterator<Key> cursorAtOrAfter(Key key) {
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
