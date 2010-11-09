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

public class Key extends Bytes implements Comparable<Key>, Serializable {
    public Key(byte[] bytes) {
        super(bytes);
    }

    public Key(String s) {
        super(s.getBytes());
    }

    public Key(ByteBuffer byteBuffer) {
        this(byteBuffer.array());
    }

    @Override
    public int compareTo(Key o) {
        byte[] mine = bytes();
        byte[] his = o.bytes();

        int minSize = Math.min(mine.length, his.length);

        for (int i = 0; i < minSize; i++) {
            if (mine[i] != his[i]) {
                return mine[i] - his[i];
            }
        }
        return mine.length - his.length;
    }
}
