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
import net.rhapso.koa.tree.Cursor;
import net.rhapso.koa.tree.MemoryTree;

import java.nio.ByteBuffer;

public class RawBidirTest extends BaseTestCase {
    public void testPutGet() throws Exception {
        Bidir bidir = new RawBidir(new MemoryTree(), new MemoryTree());
        assertEquals(0l, bidir.upsert(buffer("foo")));
        assertEquals(1l, bidir.upsert(buffer("bar")));
        assertEquals(0l, bidir.upsert(buffer("foo")));
        assertEquals(buffer("foo"), bidir.resolve(0l));
        assertEquals(buffer("bar"), bidir.resolve(1l));
        assertEquals(null, bidir.resolve(42l));
        assertEquals(0l, (long) bidir.get(buffer("foo")));
        assertNull(bidir.get(buffer("poop")));
    }

    public void testCursorAtOrAfter() {
        Bidir bidir = new RawBidir(new MemoryTree(), new MemoryTree());
        for (ByteBuffer buffer : new ByteBuffer[]{buffer("fum"), buffer("fee"), buffer("fo")}) {
            bidir.upsert(buffer);
        }

        Cursor<Long> longCursor = bidir.cursorAtOrAfter(buffer("f"));
        assertEquals(1l, (long) longCursor.next());
        assertEquals(2l, (long) longCursor.next());
        assertEquals(0l, (long) longCursor.next());
        assertFalse(longCursor.hasNext());
    }

    private ByteBuffer buffer(String s) {
        return ByteBuffer.wrap(s.getBytes());
    }
}
