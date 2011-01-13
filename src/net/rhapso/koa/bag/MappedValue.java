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

import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.StorageSize;
import net.rhapso.koa.tree.Value;

public class MappedValue {
    private Addressable addressable;

    public MappedValue(Addressable addressable) {
        this.addressable = addressable;
    }

    public void write(MappedValueRef ref, Value value) {
        addressable.seek(ref.asLong());
        byte[] bytes = value.getBytes();
        addressable.writeLong(0l);
        addressable.writeInt(bytes.length);
        addressable.write(bytes);
    }

    public Value read(MappedValueRef ref) {
        addressable.seek(ref.asLong());
        addressable.readLong();
        byte[] bytes = new byte[addressable.readInt()];
        addressable.read(bytes);
        return new Value(bytes);
    }


    public MappedValueRef getNext(MappedValueRef ref) {
        addressable.seek(ref.asLong());
        return new MappedValueRef(addressable.readLong());
    }

    public void setNext(MappedValueRef ref, MappedValueRef next) {
        addressable.seek(ref.asLong());
        addressable.writeLong(next.asLong());
    }

    public StorageSize storageSize(Value value) {
        return new StorageSize(8 + 4 + value.getBytes().length);
    }
}
