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

import java.util.LinkedHashMap;
import java.util.Map;

public class BlockAddressable implements Addressable {
    private final Addressable underlying;
    private final BlockSize blockSize;
    private long position;
    private final Map<BlockId, Block> blocks;

    public BlockAddressable(final Addressable underlying, final BlockSize blockSize, final int cachedBlocks) {
        this.underlying = underlying;
        this.blockSize = blockSize;
        this.position = 0;
        this.blocks = new LinkedHashMap<BlockId, Block>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<BlockId, Block> eldest) {
                if (blocks.size() > cachedBlocks) {
                    flush(eldest);
                    return true;
                }
                return false;
            }
        };
    }

    @Override
    public void seek(long pos) {
        this.position = pos;
    }

    @Override
    public void read(byte[] b) {
        obtainBlock().read(currentBlockOffset(), b);
        position += b.length;
    }

    @Override
    public void write(byte[] b) {
        obtainBlock().put(currentBlockOffset(), b);
        position += b.length;
    }

    @Override
    public int readInt() {
        int value = obtainBlock().readInt(currentBlockOffset());
        position += 4;
        return value;
    }

    @Override
    public void writeInt(int v) {
        obtainBlock().putInt(currentBlockOffset(), v);
        position += 4;
    }


    private Block obtainBlock() {
        BlockId blockId = currentBlockId();
        Block block = blocks.get(blockId);
        if (block == null) {
            long blockOffset = blockId.asLong() * blockSize.asLong();
            byte[] bytes = new byte[blockSize.asInt()];
            if (blockOffset >= underlying.length()) {
                underlying.seek(blockOffset);
                underlying.write(new byte[blockSize.asInt()]);
            } else {
                underlying.seek(blockOffset);
                underlying.read(bytes);
            }
            block = new Block(bytes, false);
        }
        blocks.put(blockId, block);
        return block;
    }

    @Override
    public long readLong() {
        long result = obtainBlock().readLong(currentBlockOffset());
        position += 8;
        return result;
    }

    @Override
    public double readDouble() {
        double result = obtainBlock().readDouble(currentBlockOffset());
        position += 8;
        return result;
    }

    @Override
    public void writeDouble(double d) {
        obtainBlock().putDouble(currentBlockOffset(), d);
        position += 8;
    }

    @Override
    public void writeLong(long v) {
        obtainBlock().putLong(currentBlockOffset(), v);
        position += 8;
    }

    @Override
    public int read() {
        byte b = obtainBlock().read(currentBlockOffset());
        position++;
        return b;
    }

    @Override
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

    @Override
    public long length() {
        return underlying.length();
    }

    @Override
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

    @Override
    public void flush() {
        for (Map.Entry<BlockId, Block> entry : blocks.entrySet()) {
            flush(entry);
        }
        underlying.flush();
    }

    private void flush(Map.Entry<BlockId, Block> mapEntry) {
        if (mapEntry.getValue().isDirty()) {
            underlying.seek(mapEntry.getKey().asLong() * blockSize.asLong());
            underlying.write(mapEntry.getValue().bytes());
            mapEntry.getValue().markClean();
        }
    }

    @Override
    public void close() {
        underlying.close();
    }

    public Offset getPosition() {
        return new Offset(position);
    }
}
