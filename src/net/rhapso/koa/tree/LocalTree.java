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

import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.BlockAddressable;
import net.rhapso.koa.storage.BlockSize;
import net.rhapso.koa.storage.FileAddressable;

import java.io.File;

public class LocalTree implements Tree {
    private final NodeFactory nodeFactory;
    private final TreeControl treeControl;

    public static LocalTree openOrCreate(File file) throws Exception {
        if (!file.exists()) {
            file.createNewFile();
            return initialize(new FileAddressable(file));
        } else {
            FileAddressable addressable = new FileAddressable(file);
            return open(addressable);
        }
    }

    public static LocalTree open(Addressable addressable) {
        TreeControl treeControl = new TreeControl(addressable);
        BlockSize blockSize = treeControl.getBlockSize();
        NodeFactory nodeFactory = new NodeFactory(new BlockAddressable(addressable, blockSize, 10000));
        return new LocalTree(nodeFactory, treeControl);
    }

    public static LocalTree initialize(Addressable adressable) {
        Order order = new Order(10);
        BlockSize blockSize = new BlockSize(4096 * 16);
        BlockAddressable blockAddressable = new BlockAddressable(adressable, blockSize, 10000);
        NodeFactory nodeFactory = NodeFactory.initialize(blockAddressable, blockSize, order);
        return new LocalTree(nodeFactory, nodeFactory.getTreeControl());
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
    public Cursor cursorAt(Key key) {
        return obtainRoot().cursorAt(key);
    }

    @Override
    public Cursor cursorAtOrAfter(Key key) {
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

    @Override
    public void flush() {
        nodeFactory.flush();
    }
}
