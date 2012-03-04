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

import net.rhapso.koa.storage.*;
import net.rhapso.koa.storage.block.BlockSize;

public class TreeControl {
    private static final int LENGTH = 8;
    private final StoredArray<Long> storedArray;
    private final Addressable addressable;
    private final Order order;

    public static TreeControl initialize(Addressable addressable, Order order) {
        StoredArray<Long> storedArray = StoredArray.initialize(new LongIO(), addressable, new MaxSize(LENGTH), new Offset(0));
        storedArray.add((long) order.asInt());
        storedArray.add(0l);
        storedArray.add(addressable.getBlockSize().asLong());
        storedArray.add(storageSize().asLong());
        storedArray.add(NodeRef.NULL.asLong());
        storedArray.add(0l);
        return new TreeControl(addressable);
    }

    public static StorageSize storageSize() {
        return StoredArray.storageSize(new LongIO(), new MaxSize(LENGTH));
    }

    public TreeControl(Addressable addressable) {
        this.addressable = addressable;
        storedArray = new StoredArray<Long>(new LongIO(), addressable, new MaxSize(8), new Offset(0));
        order = new Order(storedArray.get(0).intValue());
    }

    public void setRootNode(NodeRef nodeRef) {
        storedArray.set(4, nodeRef.asLong());
    }

    public NodeRef getRootNode() {
        return new NodeRef(storedArray.get(4));
    }

    public Offset allocate(StorageSize storageSize) {
        long currentInsertionPoint = storedArray.get(3);
        Offset offset = addressable.nextInsertionLocation(new Offset(currentInsertionPoint), storageSize.asLong());
        storedArray.set(3, offset.plus(storageSize.asLong()).asLong());
        return offset;
    }

    public Order getOrder() {
        return order;
    }

    public long count() {
        return storedArray.get(5);
    }

    public void incrementCount() {
        long count = storedArray.get(5);
        storedArray.set(5, count + 1);
    }

    public void flush() {
        addressable.flush();
    }

    public boolean clear() {
        initialize(addressable, order);
        return true;
    }
}
