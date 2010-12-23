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
import net.rhapso.koa.storage.Offset;
import net.rhapso.koa.storage.StorageSize;

import java.util.ArrayList;

public abstract class Scribe {
    private Order order;
    private ArrayList<StorageSize> nodeOffsets;
    private StorageSize storageSize;

    public static enum NodeType {
        leafNode {
            @Override
            protected Scribe pick(LeafNodeScribe leafNodeScribe, InnerNodeScribe innerNodeScribe) {
                return leafNodeScribe;
            }
        },
        innerNode {
            @Override
            protected Scribe pick(LeafNodeScribe leafNodeScribe, InnerNodeScribe innerNodeScribe) {
                return innerNodeScribe;
            }
        };

        protected abstract Scribe pick(LeafNodeScribe leafNodeScribe, InnerNodeScribe innerNodeScribe);

        public byte asByte() {
            return (byte) ordinal();
        }

        public static Scribe choose(byte ordinal, LeafNodeScribe leafNodeScribe, InnerNodeScribe innerNodeScribe) {
            return values()[ordinal].pick(leafNodeScribe, innerNodeScribe);
        }
    }

    protected Scribe(Order order, ArrayList<StorageSize> nodeOffsets) {
        this.order = order;
        this.nodeOffsets = nodeOffsets;
        long totalStorageSize = 0;
        for (StorageSize ss : nodeOffsets) {
            totalStorageSize += ss.asLong();
        }

        storageSize = new StorageSize(totalStorageSize);
    }

    protected Offset fieldOffset(Offset from, int field) {
        long accumulator = from.asLong();
        for (int i = 0; i <= field; i++) {
            accumulator += nodeOffsets.get(i).asLong();
        }

        return new Offset(accumulator);
    }

    public StorageSize storageSize() {
        return storageSize;
    }

    public Order getOrder() {
        return order;
    }

    public abstract NodeType nodeType();

    public abstract Node create(Offset from, NodeFactory nodeFactory, Addressable addressable, NodeRef parent);

    public abstract Node read(NodeFactory nodeFactory, Addressable addressable, Offset offset);
}
