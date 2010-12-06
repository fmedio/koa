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

import net.rhapso.koa.tree.Tree;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class BidirCache implements Bidir {
    private static final int CAPACITY = 50000;

    private Bidir bidir;
    private LinkedHashMap<ByteBuffer, Long> byContents;
    private LinkedHashMap<Long, ByteBuffer> byId;

    public BidirCache(Tree byId, Tree byContents) {
        this(new RawBidir(byId, byContents));
    }

    public BidirCache(Bidir bidir) {
        this.bidir = bidir;
        this.byContents = new LinkedHashMap<ByteBuffer, Long>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<ByteBuffer, Long> eldest) {
                return size() > CAPACITY;
            }
        };

        byId = new LinkedHashMap<Long, ByteBuffer>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, ByteBuffer> eldest) {
                return size() > CAPACITY;
            }
        };
    }

    @Override
    public long upsert(ByteBuffer byteBuffer) {
        Long result = byContents.get(byteBuffer);
        if (result != null) {
            return result;
        }

        result = bidir.upsert(byteBuffer);
        byContents.put(byteBuffer, result);
        byId.put(result, byteBuffer);
        return result;
    }

    @Override
    public Long get(ByteBuffer byteBuffer) {
        Long result = byContents.get(byteBuffer);
        if (result != null) {
            return result;
        }

        result = bidir.get(byteBuffer);
        if (result == null) {
            return null;
        }

        byContents.put(byteBuffer, result);
        byId.put(result, byteBuffer);
        return result;
    }

    @Override
    public ByteBuffer resolve(long id) {
        ByteBuffer result = byId.get(id);
        if (result != null) {
            return result;
        }

        result = bidir.resolve(id);
        if (result == null) {
            return result;
        }

        byContents.put(result, id);
        byId.put(id, result);
        return result;
    }

    @Override
    public Iterator<Long> cursorAtOrAfter(ByteBuffer byteBuffer) {
        return bidir.cursorAtOrAfter(byteBuffer);
    }
}
