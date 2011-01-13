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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.MaxSize;
import net.rhapso.koa.storage.Offset;
import net.rhapso.koa.storage.StorageSize;

import javax.annotation.Nullable;
import java.util.List;

public class Values extends SplittableArray<ValueRef> {

    public Values(Addressable addressable, Offset offset, Order order) {
        super(new ValueRefIO(), addressable, new MaxSize(order.asInt() + 1), offset);
    }

    public static Values initialize(Addressable addressable, Offset offset, Order order) {
        initialize(new ValueRefIO(), addressable, new MaxSize(order.asInt() + 1), offset);
        return new Values(addressable, offset, order);
    }

    public static StorageSize storageSize(Order order) {
        return storageSize(order, new ValueRefIO());
    }

    public String toString(final NodeFactory nodeFactory) {
        List<String> values = this.map(new Function<ValueRef, String>() {
            @Override
            public String apply(@Nullable ValueRef valueRef) {
                Value value = nodeFactory.readValue(valueRef);
                return new Bytes(value.bytes()).toString();
            }
        });

        return Joiner.on(" ").join(values);
    }
}