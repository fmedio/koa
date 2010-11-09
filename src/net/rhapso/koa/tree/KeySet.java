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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import net.rhapso.koa.storage.*;

import javax.annotation.Nullable;
import java.util.List;

public class KeySet extends SplittableArray<KeyRef> {
    private final NodeFactory nodeFactory;

    public KeySet(NodeFactory nodeFactory, Addressable addressable, Offset offset, Order order) {
        super(new KeyRefIO(), addressable, new MaxSize(order.asInt() + 1), offset);
        this.nodeFactory = nodeFactory;
    }

    public static KeySet initialize(NodeFactory nodeFactory, Addressable addressable, Offset offset, Order order) {
        StoredArray.initialize(new KeyRefIO(), addressable, new MaxSize(order.asInt() + 1), offset);
        return new KeySet(nodeFactory, addressable, offset, order);
    }

    public static StorageSize storageSize(Order order) {
        return storageSize(order, new KeyRefIO());
    }

    public int insertionPoint(KeyRef keyRef, boolean failOnDuplicateKey) {
        return insertionPoint(nodeFactory.readKey(keyRef), failOnDuplicateKey);
    }

    public int insertionPoint(Key candidate, boolean failOnDuplicateKey) {
        int i = 0;
        for (; i < this.size(); i++) {
            int result = candidate.compareTo(nodeFactory.readKey(this.get(i)));
            if (result < 0) {
                break;
            }

            if (result == 0) {
                if (failOnDuplicateKey) {
                    throw new DuplicateKeyException(candidate);
                } else {
                    return i + 1;
                }
            }
        }

        return i;
    }

    public boolean contains(Key key) {
        return offsetOf(key) != -1;
    }

    public int offsetOf(Key key) {
        for (int i = 0; i < this.size(); i++) {
            if (key.compareTo(nodeFactory.readKey(this.get(i))) == 0) {
                return i;
            }
        }
        return -1;
    }


    public String toString() {
        List<String> strings = map(new Function<KeyRef, String>() {
            @Override
            public String apply(@Nullable KeyRef t) {
                byte[] bytes = nodeFactory.readBytes(t.asLong());
                return new Bytes(bytes).toString();
            }
        });

        return Joiner.on(" ").join(strings);
    }
}
