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

package net.rhapso.koa.tree;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class Value extends Bytes implements Serializable {
    public Value(String contents) {
        super(contents);
    }

    public Value(long l) {
        super(new LongValue(l).asBytes());
    }

    public Value(byte b) {
        super(new byte[]{b});
    }

    public Value(byte[] bytes) {
        super(bytes);
    }

    public int asInt() {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes());
        return byteBuffer.getInt();
    }

    public byte asByte() {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes());
        return byteBuffer.get();
    }

    public long asLong() {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes());
        return byteBuffer.getLong();
    }
}
