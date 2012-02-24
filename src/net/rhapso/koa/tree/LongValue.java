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

package net.rhapso.koa.tree;

import java.io.Serializable;

public class LongValue implements Serializable {
    private final long value;

    public LongValue(long value) {
        this.value = value;
    }

    public long asLong() {
        return value;
    }

    public LongValue plus(long l) {
        return new LongValue(value + l);
    }

    public LongValue times(long l) {
        return new LongValue(value * l);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LongValue longValue = (LongValue) o;

        if (value != longValue.value) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + value;
    }

    public byte[] asBytes() {
        byte[] bytes = new byte[8];

        for (int i = 0; i < bytes.length; i++) {
            bytes[7 - i] = (byte) (value >>> (i * 8));
        }

        return bytes;
    }

    public static LongValue fromBytes(byte[] bytes) {
        long result = 0l;

        for (int i = 0; i < 8; i++) {
            result <<= 8;
            result ^= bytes[i] & 0xffl;
        }

        return new LongValue(result);
    }
}
