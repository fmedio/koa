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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.Offset;
import net.rhapso.koa.storage.StorageSize;

import java.util.List;

public enum NodeType {
    innerNode {
        @Override
        public List<StorageSize> nodeOffsets(Order order) {
            return Lists.newArrayList(
                    new StorageSize(1),
                    StoredLong.storageSize(),
                    StoredLong.storageSize(),
                    KeySet.storageSize(order),
                    Children.storageSize(order));
        }

        @Override
        public Node read(NodeFactory nodeFactory, Addressable addressable, Offset offset, Order order) {
            List<Offset> offsets = accumulateOffsets(this, offset, order);
            StoredLong parent = new StoredLong(addressable, offsets.get(0));
            StoredLong ref = new StoredLong(addressable, offsets.get(1));
            KeySet keySet = new KeySet(nodeFactory, addressable, offsets.get(2), order);
            Children children = new Children(nodeFactory, addressable, offsets.get(3), order);
            return new InnerNode(nodeFactory, keySet, parent, ref, children);
        }

        @Override
        public Node create(Offset from, Order order, NodeFactory nodeFactory, Addressable addressable, NodeRef parent) {
            List<Offset> offsets = accumulateOffsets(this, from, order);
            StoredLong parentStore = new StoredLong(addressable, offsets.get(0));
            parentStore.write(parent.asLong());
            StoredLong referenceStore = new StoredLong(addressable, offsets.get(1));
            referenceStore.write(from.asNodeRef().asLong());
            KeySet keySet = KeySet.initialize(nodeFactory, addressable, offsets.get(2), order);
            Children children = Children.initialize(nodeFactory, addressable, offsets.get(3), order);
            return new InnerNode(nodeFactory, keySet, parentStore, referenceStore, children);
        }
    },

    leafNode {
        @Override
        public List<StorageSize> nodeOffsets(Order order) {
            return Lists.newArrayList(
                    new StorageSize(1),
                    StoredLong.storageSize(),
                    StoredLong.storageSize(),
                    KeySet.storageSize(order),
                    StoredLong.storageSize(),
                    Values.storageSize(order));
        }

        @Override
        public Node read(NodeFactory nodeFactory, Addressable addressable, Offset offset, Order order) {
            List<Offset> offsets = accumulateOffsets(this, offset, order);
            StoredLong parent = new StoredLong(addressable, offsets.get(0));
            StoredLong ref = new StoredLong(addressable, offsets.get(1));
            KeySet keySet = new KeySet(nodeFactory, addressable, offsets.get(2), order);
            StoredLong next = new StoredLong(addressable, offsets.get(3));
            Values values = new Values(addressable, offsets.get(4), order);
            return new LeafNode(nodeFactory, keySet, values, parent, ref, next);
        }

        @Override
        public Node create(Offset from, Order order, NodeFactory nodeFactory, Addressable addressable, NodeRef parent) {
            List<Offset> offsets = accumulateOffsets(this, from, order);
            StoredLong parentStore = new StoredLong(addressable, offsets.get(0));
            parentStore.write(parent.asLong());
            StoredLong referenceStore = new StoredLong(addressable, offsets.get(1));
            referenceStore.write(from.asNodeRef().asLong());
            KeySet keySet = KeySet.initialize(nodeFactory, addressable, offsets.get(2), order);
            StoredLong next = new StoredLong(addressable, offsets.get(3));
            next.write(NodeRef.NULL.asLong());
            Values values = Values.initialize(addressable, offsets.get(4), order);
            return new LeafNode(nodeFactory, keySet, values, parentStore, referenceStore, next);
        }
    };

    public byte asByte() {
        return (byte) ordinal();
    }

    public static NodeType fromByte(byte b) {
        return NodeType.values()[b];
    }


    private static List<Offset> accumulateOffsets(NodeType nodeType, final Offset from, Order order) {
        return Lists.transform(nodeType.nodeOffsets(order), new Function<StorageSize, Offset>() {
            long acc = from.asLong();

            @Override
            public Offset apply(StorageSize storageSize) {
                acc += storageSize.asLong();
                return new Offset(acc);
            }
        });
    }

    public StorageSize storageSize(Order order) {
        long acc = 0;
        for (StorageSize storageSize : nodeOffsets(order)) {
            acc += storageSize.asLong();
        }

        return new StorageSize(acc);
    }

    public abstract Node create(Offset from, Order order, NodeFactory nodeFactory, Addressable addressable, NodeRef parent);

    public abstract Node read(NodeFactory nodeFactory, Addressable addressable, Offset offset, Order order);

    abstract List<StorageSize> nodeOffsets(Order order);
}
