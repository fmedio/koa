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

package net.rhapso.koa.storage;

import net.rhapso.koa.storage.block.Block;
import net.rhapso.koa.storage.block.BlockId;
import net.rhapso.koa.storage.block.BlockSize;
import net.rhapso.koa.storage.block.CacheProvider;

public class Addressable {
    private final CacheProvider cacheProvider;
    private final StorageProvider storageProvider;
    private final BlockSize blockSize;
    private long position;

    public void flush() {
        cacheProvider.flush();
    }


    public Addressable(final StorageProvider storageProvider, final BlockSize blockSize, CacheProvider cacheProvider) {
        this.storageProvider = storageProvider;
        this.blockSize = blockSize;
        this.cacheProvider = cacheProvider;
        this.position = 0;
    }


    public void seek(long pos) {
        this.position = pos;
    }

    public void read(byte[] b) {
        obtainBlock().read(currentBlockOffset(), b);
        position += b.length;
    }


    private Block obtainBlock() {
        return cacheProvider.obtainBlock(storageProvider, currentBlockId());
    }

    public void write(byte[] b) {
        obtainBlock().put(currentBlockOffset(), b);
        position += b.length;
    }

    public int readInt() {
        int value = obtainBlock().readInt(currentBlockOffset());
        position += 4;
        return value;
    }

    public void writeInt(int v) {
        obtainBlock().putInt(currentBlockOffset(), v);
        position += 4;
    }

    public long readLong() {
        long result = obtainBlock().readLong(currentBlockOffset());
        position += 8;
        return result;
    }

    public double readDouble() {
        double result = obtainBlock().readDouble(currentBlockOffset());
        position += 8;
        return result;
    }

    public void writeDouble(double d) {
        obtainBlock().putDouble(currentBlockOffset(), d);
        position += 8;
    }

    public void writeLong(long v) {
        obtainBlock().putLong(currentBlockOffset(), v);
        position += 8;
    }

    public int read() {
        byte b = obtainBlock().read(currentBlockOffset());
        position++;
        return b;
    }

    public void write(int aByte) {
        obtainBlock().put(currentBlockOffset(), (byte) aByte);
        position++;
    }

    private int currentBlockOffset() {
        return (int) (position % blockSize.asLong());
    }

    private BlockId currentBlockId() {
        return new BlockId(position / blockSize.asLong());
    }

    public long length() {
        return storageProvider.length();
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
        storageProvider.close();
    }

    public Offset getPosition() {
        return new Offset(position);
    }

    public BlockSize getBlockSize() {
        return blockSize;
    }
}
