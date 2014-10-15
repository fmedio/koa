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
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class AddressableTest extends BaseTreeTestCase {
    private Storage storage;

    @Test
    public void testFlushCache() throws Exception {
        final BlockSize blockSize = new BlockSize(4);
        Addressable addressable = new Addressable(storage, new LRUCache(1, blockSize));
        addressable.writeInt(0, 1);
        verify(storage, times(0)).write(new byte[]{0, 0, 0, 1});
        addressable.writeInt(4, 2);
        verify(storage, times(1)).write(new byte[]{0, 0, 0, 1});
    }

    @Test
    public void testCommit() throws Exception {
        final BlockSize blockSize = new BlockSize(4);
        Addressable addressable = new Addressable(storage, new LRUCache(Integer.MAX_VALUE, blockSize));
        addressable.writeInt(0, 42);
        addressable.flush();
        byte[] expectedResult = fillBuffer(42);
        verify(storage, times(1)).write(expectedResult);
    }

    @Test
    public void testNextInsertLocation() throws Exception {
        final BlockSize blockSize = new BlockSize(42);
        final Addressable addressable = new Addressable(storage, new LRUCache(Integer.MAX_VALUE, blockSize));
        assertEquals(0l, addressable.nextInsertionLocation(new Offset(0), 42).asLong());
        assertEquals(42l, addressable.nextInsertionLocation(new Offset(1), 42).asLong());
        assertEquals(42l, addressable.nextInsertionLocation(new Offset(41), 12).asLong());
        assertFailure(IllegalArgumentException.class, () -> addressable.nextInsertionLocation(new Offset(0), 43));
    }

    @Test
    public void testOnlyFlushDirtyBlocks() {
        final BlockSize blockSize = new BlockSize(2);
        Addressable addressable = new Addressable(storage, new LRUCache(2, blockSize));
        addressable.write(0, new byte[2]);
        addressable.write(2, new byte[2]);
        addressable.flush();
        reset(storage);
        addressable.write(0, new byte[2]);
        addressable.flush();
        verify(storage, times(1)).seek(0);
        verify(storage, times(1)).write(new byte[2]);
    }

    @Before
    public void setUp() throws Exception {
        storage = mock(Storage.class);
    }

    private byte[] fillBuffer(int value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(value);
        return byteBuffer.array();
    }
}
