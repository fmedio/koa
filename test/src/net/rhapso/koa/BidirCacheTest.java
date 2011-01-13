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

package net.rhapso.koa;

import java.nio.ByteBuffer;

import static org.mockito.Mockito.*;

public class BidirCacheTest extends BaseTreeTestCase {
    private ByteBuffer bytes;
    private Bidir underlying;
    private BidirCache cache;

    public void testUpsert() throws Exception {
        when(underlying.upsert(any(ByteBuffer.class))).thenReturn(randomLong);
        assertEquals(randomLong, (long) cache.upsert(bytes));
        assertEquals(randomLong, (long) cache.upsert(bytes));
        verify(underlying, times(1)).upsert(bytes);
    }

    public void testGet() throws Exception {
        when(underlying.get(bytes)).thenReturn(randomLong);
        assertEquals(randomLong, (long) cache.get(bytes));
        assertEquals(randomLong, (long) cache.get(bytes));
        verify(underlying, times(1)).get(bytes);
    }

    public void testResolve() throws Exception {
        when(underlying.resolve(randomLong)).thenReturn(bytes);
        assertEquals(bytes, cache.resolve(randomLong));
        assertEquals(bytes, cache.resolve(randomLong));
        verify(underlying, times(1)).resolve(randomLong);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        bytes = ByteBuffer.wrap(new byte[]{2});
        underlying = mock(Bidir.class);
        cache = new BidirCache(underlying);
    }

    public void testByteBufferEquality() throws Exception {
        assertEquals(bytes, ByteBuffer.wrap(new byte[]{2}));
    }
}
