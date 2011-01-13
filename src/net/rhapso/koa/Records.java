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

package net.rhapso.koa;

import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.IO;
import net.rhapso.koa.storage.Offset;

// Fixed-length records for sequential access

// Direct i/o, unsafe
public class Records<T> {
    private Addressable storage;
    private IO<T> io;

    public Records(Addressable storage, IO<T> io) {
        this.storage = storage;
        this.io = io;
    }

    public void put(long index, T value) {
        Offset offset = seekToAppropriatePosition(index);
        io.write(storage, offset, value);
    }

    public T get(long index) {
        Offset offset = seekToAppropriatePosition(index);
        return io.read(storage, offset);
    }

    private Offset seekToAppropriatePosition(long index) {
        long storageSize = io.storageSize().asLong();
        Offset candidateOffset = new Offset(index * storageSize);
        Offset offset = storage.nextInsertionLocation(candidateOffset, storageSize);
        storage.seek(offset.asLong());
        if (offset.asLong() > storage.length()) {
            for (long i = 0; i < io.storageSize().asLong(); i++) {
                storage.write(0);
            }
            storage.seek(offset.asLong());
        }
        return offset;
    }
}
