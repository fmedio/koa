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

package net.rhapso.koa.storage.block;

import junit.framework.TestCase;
import net.rhapso.koa.storage.MemoryStorage;

public class LRUCacheTest extends TestCase {
    private Cache cache;
    private MemoryStorage storage;

    public void testBasicOperation() throws Exception {
        final Block block = cache.obtainBlock(storage, new BlockId(0));
        assertTrue(isZero(storage.bytes()));
        block.put(0, (byte) 1);
        assertTrue(isZero(storage.bytes()));
        cache.flush();
        assertFalse(isZero(storage.bytes()));
    }

    public void testOldBlocksAreEvicted() throws Exception {
        assertTrue(isZero(storage.bytes()));
        cache.obtainBlock(storage, new BlockId(0)).put(0, (byte) 1);
        cache.obtainBlock(storage, new BlockId(1)).put(0, (byte) 1);
        assertTrue(isZero(storage.bytes()));
        cache.obtainBlock(storage, new BlockId(2)).put(0, (byte) 1);
        assertFalse(isZero(storage.bytes()));
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        cache = new LRUCache(2, new BlockSize(128));
        storage = new MemoryStorage(4 * 128);
    }

    private boolean isZero(byte[] bytes) {
        for (byte aByte : bytes) {
            if (aByte != 0) {
                return false;
            }
        }

        return true;
    }
}
