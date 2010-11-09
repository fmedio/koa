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

import java.nio.ByteBuffer;

public class MemoryAddressable implements Addressable {
    private final ByteBuffer byteBuffer;
    private int pos;

    public MemoryAddressable(int bytes) {
        byteBuffer = ByteBuffer.allocate(bytes);
        this.pos = 0;
    }

    @Override
    public void seek(long pos) {
        this.pos = (int) pos;
    }

    public void read(byte[] b) {
        for (int i = 0; i < b.length; i++) {
            b[i] = byteBuffer.get(pos++);
        }
    }

    @Override
    public int readInt() {
        int result = byteBuffer.getInt(pos);
        pos += 4;
        return result;
    }

    @Override
    public long readLong() {
        long result = byteBuffer.getLong(pos);
        pos += 8;
        return result;
    }

    @Override
    public double readDouble() {
        double result = byteBuffer.getDouble(pos);
        pos += 8;
        return result;
    }

    @Override
    public void writeDouble(double d) {
        byteBuffer.putDouble(pos, d);
        pos += 8;
    }

    @Override
    public void write(byte[] b) {
        for (byte theByte : b) {
            byteBuffer.put(pos++, theByte);
        }
    }

    @Override
    public void writeInt(int v) {
        byteBuffer.putInt(pos, v);
        pos += 4;
    }

    @Override
    public void writeLong(long v) {
        byteBuffer.putLong(pos, v);
        pos += 8;
    }

    @Override
    public int read() {
        return byteBuffer.get(pos);
    }

    @Override
    public void write(int aByte) {
        byteBuffer.put(pos++, (byte) aByte);
    }

    @Override
    public long length() {
        return byteBuffer.capacity();
    }

    @Override
    public Offset nextInsertionLocation(Offset currentOffset, long length) {
        return currentOffset;
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
}
