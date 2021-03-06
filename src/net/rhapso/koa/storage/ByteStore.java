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

import net.rhapso.koa.StorageFactory;
import net.rhapso.koa.storage.block.Cache;
import net.rhapso.koa.tree.StoreName;

public class ByteStore {
    private Addressable storage;

    public ByteStore(Addressable addressable) {
        this.storage = addressable;
    }

    public static ByteStore open(StorageFactory storageFactory, StoreName storageName, Cache cache) {
        if (!storageFactory.exists(storageName)) {
            Addressable addressable = storageFactory.openAddressable(storageName, cache);
            return initialize(addressable);
        }

        return new ByteStore(storageFactory.openAddressable(storageName, cache));
    }

    public static ByteStore initialize(Addressable addressable) {
        addressable.writeLong(0, 8);
        return new ByteStore(addressable);
    }

    public Offset put(byte[] bytes) {
        long insertionPoint = storage.readLong(0);
        Offset writeOffset = storage.nextInsertionLocation(new Offset(insertionPoint), bytes.length);
        final long position = writeOffset.asLong();
        storage.writeInt(position, bytes.length);
        storage.write(position + 4, bytes);

        storage.writeLong(0, position + 4 + bytes.length);
        return writeOffset;
    }

    public byte[] get(Offset offset) {
        final long position = offset.asLong();
        int size = storage.readInt(position);
        byte[] result = new byte[size];
        storage.read(position + 4, result);
        return result;
    }
}
