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

import com.google.common.collect.Lists;
import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.Offset;
import net.rhapso.koa.storage.StorageSize;

public class LeafNodeScribe extends Scribe {

    public LeafNodeScribe(Order order) {
        super(order,
                Lists.newArrayList(
                        new StorageSize(1),
                        StoredLong.storageSize(),
                        StoredLong.storageSize(),
                        KeySet.storageSize(order),
                        StoredLong.storageSize(),
                        Values.storageSize(order)));
    }

    @Override
    public Node read(NodeFactory nodeFactory, Addressable addressable, Offset offset) {
        StoredLong parent = new StoredLong(addressable, fieldOffset(offset, 0));
        StoredLong ref = new StoredLong(addressable, fieldOffset(offset, 1));
        KeySet keySet = new KeySet(nodeFactory, addressable, fieldOffset(offset, 2), getOrder());
        StoredLong next = new StoredLong(addressable, fieldOffset(offset, 3));
        Values values = new Values(addressable, fieldOffset(offset, 4), getOrder());
        return new LeafNode(nodeFactory, keySet, values, parent, ref, next);
    }

    @Override
    public Node create(Offset from, NodeFactory nodeFactory, Addressable addressable, NodeRef parent) {
        StoredLong parentStore = new StoredLong(addressable, fieldOffset(from, 0));
        parentStore.write(parent.asLong());
        StoredLong referenceStore = new StoredLong(addressable, fieldOffset(from, 1));
        referenceStore.write(from.asNodeRef().asLong());
        KeySet keySet = KeySet.initialize(nodeFactory, addressable, fieldOffset(from, 2), getOrder());
        StoredLong next = new StoredLong(addressable, fieldOffset(from, 3));
        next.write(NodeRef.NULL.asLong());
        Values values = Values.initialize(addressable, fieldOffset(from, 4), getOrder());
        return new LeafNode(nodeFactory, keySet, values, parentStore, referenceStore, next);
    }

    @Override
    public NodeType nodeType() {
        return NodeType.leafNode;
    }
}
