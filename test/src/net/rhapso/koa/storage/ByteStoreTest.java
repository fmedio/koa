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

package net.rhapso.koa.storage;

import net.rhapso.koa.BaseTreeTestCase;
import net.rhapso.koa.storage.block.BlockSize;
import net.rhapso.koa.storage.block.LRUCache;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ByteStoreTest extends BaseTreeTestCase {
    @Test
    public void testStore() throws Exception {
        ByteStore byteStore = ByteStore.initialize(makeAddressable());
        Offset first = byteStore.put(new byte[42]);
        Offset second = byteStore.put(new byte[43]);
        Offset third = byteStore.put(new byte[44]);
        assertEquals(42, byteStore.get(first).length);
        assertEquals(43, byteStore.get(second).length);
        assertEquals(44, byteStore.get(third).length);
    }

    @Test
    public void testWeirdBufferOverflow() throws Exception {
        Addressable addressable = new Addressable(new MemoryStorage(1024 * 500), new LRUCache(100, new BlockSize(8)));
        ByteStore byteStore = ByteStore.initialize(addressable);
        byteStore.put(new byte[3]);
        byteStore.put(new byte[3]);
        byteStore.put(new byte[3]);
    }
}
