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

package net.rhapso.koa.storage.block;

import java.nio.ByteBuffer;

public class Block {
    private final ByteBuffer byteBuffer;
    private boolean isDirty;

    public Block(byte[] bytes, boolean isDirty) {
        byteBuffer = ByteBuffer.wrap(bytes);
        this.isDirty = isDirty;
    }

    public int readInt(int offset) {
        return byteBuffer.getInt(offset);
    }

    public void putInt(int offset, int value) {
        isDirty = true;
        byteBuffer.putInt(offset, value);
    }

    public void put(int offset, byte[] bytes) {
        isDirty = true;
        byteBuffer.position(offset);
        byteBuffer.put(bytes);
    }

    public void read(int offset, byte[] bytes) {
        byteBuffer.position(offset);
        byteBuffer.get(bytes);
    }


    public void putLong(int offset, long value) {
        isDirty = true;
        byteBuffer.putLong(offset, value);
    }

    public long readLong(int offset) {
        return byteBuffer.getLong(offset);
    }

    public void putDouble(int offset, double value) {
        isDirty = true;
        byteBuffer.putDouble(offset, value);
    }

    public double readDouble(int offset) {
        return byteBuffer.getDouble(offset);
    }

    public byte read(int offset) {
        return byteBuffer.get(offset);
    }

    public void put(int offset, byte b) {
        isDirty = true;
        byteBuffer.put(offset, b);
    }

    public byte[] bytes() {
        return byteBuffer.array();
    }

    public boolean isDirty() {
        return isDirty;
    }

    public void markClean() {
        isDirty = false;
    }
}
