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

import net.rhapso.koa.storage.block.Block;
import net.rhapso.koa.storage.block.BlockId;
import net.rhapso.koa.storage.block.BlockSize;
import net.rhapso.koa.storage.block.Cache;

public class Addressable {
    private final Cache cache;
    private final Storage storage;
    private final BlockSize blockSize;

    public boolean flush() {
        cache.flush();
        return true;
    }

    public Addressable(final Storage storage, Cache cache) {
        this.storage = storage;
        this.blockSize = cache.getBlockSize();
        this.cache = cache;
    }


    public void read(long position, byte[] b) {
        obtainBlock(position).read(blockOffset(position), b);
    }


    private Block obtainBlock(long position) {
        return cache.obtainBlock(storage, new BlockId(position / blockSize.asLong()));
    }

    public void write(long position, byte[] b) {
        obtainBlock(position).put(blockOffset(position), b);
    }

    public int readInt(long position) {
        return obtainBlock(position).readInt(blockOffset(position));
    }

    public void writeInt(long position, int v) {
        obtainBlock(position).putInt(blockOffset(position), v);
    }

    public long readLong(long position) {
        return obtainBlock(position).readLong(blockOffset(position));
    }

    public double readDouble(long position) {
        return obtainBlock(position).readDouble(blockOffset(position));
    }

    public void writeDouble(long position, double d) {
        obtainBlock(position).putDouble(blockOffset(position), d);
    }

    public void writeLong(long position, long v) {
        obtainBlock(position).putLong(blockOffset(position), v);
    }

    public int read(long position) {
        return obtainBlock(position).read(blockOffset(position));
    }

    public void write(long position, int aByte) {
        obtainBlock(position).put(blockOffset(position), (byte) aByte);
    }

    private int blockOffset(long position) {
        return (int) (position % blockSize.asLong());
    }

    public long length() {
        return storage.length();
    }

    public Offset nextInsertionLocation(Offset currentOffset, long length) {
        if (length > blockSize.asInt()) {
            throw new IllegalArgumentException("Requested length exceeds block size");
        }

        long relativeLocation = currentOffset.asLong() % blockSize.asLong();

        if (relativeLocation + length > blockSize.asLong()) {
            return currentOffset.plus(blockSize.asLong() - relativeLocation);
        }

        return currentOffset;
    }

    public void close() {
        storage.close();
    }

    public BlockSize getBlockSize() {
        return blockSize;
    }
}
