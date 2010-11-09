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

package net.rhapso.koa;

import clutter.BaseTestCase;
import clutter.Functional;
import clutter.GenericFunction;
import com.google.common.base.Joiner;
import net.rhapso.koa.tree.Cursor;
import net.rhapso.koa.tree.Key;
import net.rhapso.koa.tree.Value;

import java.util.LinkedList;
import java.util.List;

public abstract class BaseTreeTestCase extends BaseTestCase {
    protected Key key(byte[] bytes) {
        return new Key(bytes);
    }

    protected Key key(int singleByte) {
        return key(new byte[]{(byte) singleByte});
    }

    protected Key key(String string) {
        return key(string.getBytes());
    }

    protected Value value(int singleByte) {
        return new Value((byte) singleByte);
    }

    protected Value value(String string) {
        return new Value(string.getBytes());
    }

    protected String toString(Cursor cursor) {
        List<String> list = new LinkedList<String>();
        while (cursor.hasNext()) {
            list.add(cursor.next().toString());
        }

        return Joiner.on(", ").join(list);
    }

    protected String[] stringify(Key[] keys) {
        String[] sorted = Functional.transform(keys, new GenericFunction<Key, String>() {
            @Override
            public String apply(Key key) {
                return new String(key.bytes());
            }

            @Override
            public String[] array(int size) {
                return new String[size];
            }
        });
        return sorted;
    }

    protected String readCursor(Cursor<Key> cursor) {
        List<String> list = new LinkedList<String>();
        while (cursor.hasNext()) {
            list.add(new String(cursor.next().bytes()));
        }
        return Joiner.on(" ").join(list);
    }
}
