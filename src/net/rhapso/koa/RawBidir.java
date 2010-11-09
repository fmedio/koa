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

import net.rhapso.koa.tree.*;

import java.nio.ByteBuffer;

public class RawBidir implements Bidir {
    private final Tree byId;
    private final Tree byContents;

    public RawBidir(Tree byId, Tree byContents) {
        this.byId = byId;
        this.byContents = byContents;
    }

    @Override
    public long upsert(ByteBuffer byteBuffer) {
        Key key = new Key(byteBuffer.array());
        if (!byContents.contains(key)) {
            LongValue id = new LongValue(byContents.count());
            byContents.put(key, new Value(id.asBytes()));
            KeyRef keyRef = byContents.referenceOf(key);
            byId.put(new Key(id.asBytes()), new Value(keyRef.asBytes()));
            return id.asLong();
        } else {
            return LongValue.fromBytes(byContents.get(key).bytes()).asLong();
        }
    }

    @Override
    public Long get(ByteBuffer byteBuffer) {
        Value id = byContents.get(new Key(byteBuffer.array()));
        if (id == null) {
            return null;
        } else {
            return LongValue.fromBytes(id.bytes()).asLong();
        }
    }

    @Override
    public ByteBuffer resolve(long id) {
        Value value = byId.get(new Key(new LongValue(id).asBytes()));
        if (value == null) {
            return null;
        }

        KeyRef keyRef = KeyRef.fromBytes(value.getBytes());
        Key key = byContents.key(keyRef);
        return ByteBuffer.wrap(key.bytes());
    }

    @Override
    public Cursor<Long> cursorAtOrAfter(ByteBuffer byteBuffer) {
        final Cursor<Key> keyCursor = byContents.cursorAtOrAfter(new Key(byteBuffer));
        return new Cursor<Long>() {
            @Override
            public Long next() {

                Value value = byContents.get(keyCursor.next());
                return LongValue.fromBytes(value.bytes()).asLong();
            }

            @Override
            public boolean hasNext() {
                return keyCursor.hasNext();
            }
        };
    }
}
