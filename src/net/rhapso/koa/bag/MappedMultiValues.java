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

package net.rhapso.koa.bag;

import net.rhapso.koa.storage.*;
import net.rhapso.koa.tree.Order;
import net.rhapso.koa.tree.TreeControl;
import net.rhapso.koa.tree.Value;

import java.util.Iterator;

public class MappedMultiValues {
    private TreeControl treeControl;
    private Addressable addressable;
    private MappedValue mappedValue;

    public MappedMultiValues(Addressable addressable) {
        this.addressable = addressable;
        treeControl = new TreeControl(addressable);
        mappedValue = new MappedValue(addressable);
    }

    public static MappedMultiValues initialize(Addressable addressable) {
        TreeControl.initialize(addressable, new Order(2));
        return new MappedMultiValues(addressable);
    }

    public MultiValueRef create() {
        LongIO longIo = new LongIO();
        MaxSize maxSize = new MaxSize(3);
        Offset offset = treeControl.allocate(StoredArray.storageSize(longIo, maxSize));
        StoredArray<Long> storedArray = StoredArray.initialize(longIo, addressable, maxSize, offset);
        storedArray.add(0l);
        storedArray.add(1, 0l);
        storedArray.add(2, 0l);
        return new MultiValueRef(offset.asLong());
    }

    public void append(Value value, MultiValueRef ref) {
        StoredArray<Long> descriptor = readDescriptor(ref);
        Offset offset = treeControl.allocate(mappedValue.storageSize(value));
        MappedValueRef mappedValueRef = new MappedValueRef(offset.asLong());
        mappedValue.write(mappedValueRef, value);

        descriptor.set(0, descriptor.get(0) + 1);

        if (descriptor.get(0) == 1) {
            descriptor.set(1, mappedValueRef.asLong()); // first
            descriptor.set(2, mappedValueRef.asLong()); // last
        } else {
            MappedValueRef previousLast = new MappedValueRef(descriptor.get(2));
            mappedValue.setNext(previousLast, mappedValueRef);
            descriptor.set(2, mappedValueRef.asLong());
        }
    }

    private StoredArray<Long> readDescriptor(MultiValueRef ref) {
        return new StoredArray<Long>(new LongIO(), addressable, new MaxSize(3), new Offset(ref.asLong()));
    }

    public long count(MultiValueRef ref) {
        return readDescriptor(ref).get(0);
    }

    public Iterator<Value> get(final MultiValueRef ref) {
        return new Iterator<Value>() {
            StoredArray<Long> descriptor = readDescriptor(ref);
            MappedValueRef next = new MappedValueRef(descriptor.get(1));

            public boolean hasNext() {
                return next.asLong() != 0l;
            }

            @Override
            public Value next() {
                Value value = mappedValue.read(next);
                next = mappedValue.getNext(next);
                return value;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public void flush() {
        treeControl.flush();
    }
}
