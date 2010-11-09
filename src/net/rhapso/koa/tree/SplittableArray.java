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

import net.rhapso.koa.storage.*;

public class SplittableArray<T extends LongValue> extends StoredArray<T> {

    public SplittableArray(IO<T> io, Addressable addressable, MaxSize maxSize, Offset offset) {
        super(io, addressable, maxSize, offset);
    }

    public void splitInto(StoredArray<T> destination) {
        int originalSize = size();
        int half = (int) Math.ceil((double) originalSize / 2d);
        for (int i = half; i < originalSize; i++) {
            destination.add(this.remove(half));
        }
    }

    protected static <T> StorageSize storageSize(Order order, IO<T> io) {
        return io.storageSize()
                .times(order.asInt() + 1)
                .plus(4);
    }
}
