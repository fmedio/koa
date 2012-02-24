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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import net.rhapso.koa.storage.*;

import java.util.Iterator;
import java.util.List;

public class Children extends StoredArray<NodeRef> implements Iterable<Node> {
    private final NodeFactory nodeFactory;

    public Children(NodeFactory nodeFactory, Addressable addressable, Offset offset, Order order) {
        super(new NodeRefIO(), addressable, maxSize(order), offset);
        this.nodeFactory = nodeFactory;
    }

    public static Children initialize(NodeFactory nodeFactory, Addressable addressable, Offset offset, Order order) {
        addressable.write(offset.asLong(), new byte[storageSize(order).intValue()]);
        return new Children(nodeFactory, addressable, offset, order);
    }

    public static StorageSize storageSize(Order order) {
        return StoredArray.storageSize(new NodeRefIO(), maxSize(order));
    }

    private static MaxSize maxSize(Order order) {
        return new MaxSize(order.asInt() + 2);
    }

    public void splitInto(Children destination) {
        int half = (int) Math.floor((double) size() / 2d);
        int originalSize = size();
        for (int i = half; i < originalSize; i++) {
            destination.add(remove(half));
        }
    }

    public Node resolve(int index) {
        NodeRef nodeRef = get(index);
        return nodeFactory.read(nodeRef);
    }

    public void add(Node tee) {
        add(size(), tee);
    }

    public Children add(int position, Node tee) {
        add(position, tee.getNodeRef());
        return this;
    }

    @Override
    public String toString() {
        List<String> strings = map(new Function<NodeRef, String>() {
            @Override
            public String apply(NodeRef nodeRef) {
                return nodeRef.toString();
            }
        });

        return Joiner.on(" ").join(strings);
    }

    @Override
    public Iterator<Node> iterator() {
        return new Iterator<Node>() {
            int currentOffset = 0;

            @Override
            public boolean hasNext() {
                return currentOffset < size();
            }

            @Override
            public Node next() {
                return nodeFactory.read(get(currentOffset++));
            }

            @Override
            public void remove() {

            }
        };
    }
}
