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

package net.rhapso.koa.storage;

import com.google.common.base.Function;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;

public class StoredArray<T> {
    private final IO<T> io;
    private final Addressable addressable;
    private final MaxSize maxSize;
    private final Offset offset;

    public static <T> StoredArray<T> initialize(IO<T> io, Addressable addressable, MaxSize maxSize, Offset offset) {
        byte[] bytes = new byte[storageSize(io, maxSize).intValue()];
        addressable.write(offset.asLong(), bytes);
        return new StoredArray<T>(io, addressable, maxSize, offset);
    }

    public StoredArray(IO<T> io, Addressable addressable, MaxSize maxSize, Offset offset) {
        this.io = io;
        this.addressable = addressable;
        this.maxSize = maxSize;
        this.offset = offset;
    }

    public T get(int index) {
        return io.read(addressable, offsetOfElement(index));
    }

    public <R> List<R> map(Function<T, R> function) {
        int currentSize = size();
        ArrayList<R> list = new ArrayList<R>(currentSize);
        for (int i = 0; i < currentSize; i++) {
            T tee = get(i);
            list.add(function.apply(tee));
        }
        return list;
    }

    private Offset offsetOfElement(int i) {
        return new Offset(io.storageSize().times(i).plus(4).plus(offset).asLong());
    }

    public int size() {
        return addressable.readInt(offset.asLong());
    }

    private void resize(int howMany) {
        int currentSize = size();
        addressable.writeInt(offset.asLong(), currentSize + howMany);
    }

    public void add(T tee) {
        add(size(), tee);
    }

    public void add(int position, T tee) {
        int currentSize = size();

        if (position > currentSize || currentSize == maxSize.asInt()) {
            throw new ArrayIndexOutOfBoundsException();
        }

        for (int i = size(); i > position; i--) {
            T t = io.read(addressable, offsetOfElement(i - 1));
            io.write(addressable, offsetOfElement(i), t);
        }

        io.write(addressable, offsetOfElement(position), tee);
        resize(1);
    }

    public void set(int position, T tee) {
        if (position >= size()) {
            throw new ArrayIndexOutOfBoundsException();
        }
        io.write(addressable, offsetOfElement(position), tee);
    }

    public static <T> StorageSize storageSize(IO<T> io, MaxSize maxSize) {
        return io.storageSize().times(maxSize.asInt()).plus(4);
    }

    public T remove(int index) {
        int currentSize = size();

        if (index + 1 > currentSize) {
            throw new ArrayIndexOutOfBoundsException();
        }


        T returnValue = get(index);

        for (int i = index; i < currentSize - 1; i++) {
            T tee = get(i + 1);
            io.write(addressable, offsetOfElement(i), tee);
        }

        resize(-1);
        return returnValue;
    }

    @Override
    public String toString() {
        List<String> result = map(new Function<T, String>() {
            @Override
            public String apply(T t) {
                return t.toString();
            }
        });

        return Joiner.on(" ").join(result);
    }

    public T removeLast() {
        return this.remove(this.size() - 1);
    }
}
