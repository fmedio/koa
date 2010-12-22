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

package net.rhapso.koa.storage;

import clutter.BaseTestCase;
import clutter.Fallible;
import junit.framework.Assert;
import net.rhapso.koa.storage.block.BlockSize;
import net.rhapso.koa.storage.block.LRUCacheProvider;

import java.nio.ByteBuffer;

import static org.mockito.Mockito.*;

public class AddressableTest extends BaseTestCase {
    private StorageProvider storageProvider;

    public void testFlushCache() throws Exception {
        final BlockSize blockSize = new BlockSize(4);
        Addressable addressable = new Addressable(storageProvider, blockSize, new LRUCacheProvider(1, blockSize));
        addressable.writeInt(1);
        verify(storageProvider, times(0)).write(new byte[]{0, 0, 0, 1});
        addressable.writeInt(2);
        verify(storageProvider, times(1)).write(new byte[]{0, 0, 0, 1});
    }

    public void testCommit() throws Exception {
        final BlockSize blockSize = new BlockSize(4);
        Addressable addressable = new Addressable(storageProvider, blockSize, new LRUCacheProvider(Integer.MAX_VALUE, blockSize));
        addressable.writeInt(randomInt);
        addressable.flush();
        byte[] expectedResult = fillBuffer(randomInt);
        verify(storageProvider, times(1)).write(expectedResult);
        Assert.assertEquals(new Offset(4l), addressable.getPosition());
    }

    public void testNextInsertLocation() throws Exception {
        final BlockSize blockSize = new BlockSize(42);
        final Addressable addressable = new Addressable(storageProvider, blockSize, new LRUCacheProvider(Integer.MAX_VALUE, blockSize));
        assertEquals(0l, addressable.nextInsertionLocation(new Offset(0), 42).asLong());
        assertEquals(42l, addressable.nextInsertionLocation(new Offset(1), 42).asLong());
        assertEquals(42l, addressable.nextInsertionLocation(new Offset(41), 12).asLong());
        assertFailure(IllegalArgumentException.class, new Fallible() {
            @Override
            public void execute() throws Exception {
                addressable.nextInsertionLocation(new Offset(0), 43);
            }
        });
    }

    public void testOnlyFlushDirtyBlocks() {
        final BlockSize blockSize = new BlockSize(2);
        Addressable addressable = new Addressable(storageProvider, blockSize, new LRUCacheProvider(2, blockSize));
        addressable.write(new byte[2]);
        addressable.write(new byte[2]);
        addressable.flush();
        reset(storageProvider);
        addressable.seek(0);
        addressable.write(new byte[2]);
        addressable.flush();
        verify(storageProvider, times(1)).seek(0);
        verify(storageProvider, times(1)).write(new byte[2]);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        storageProvider = mock(StorageProvider.class);
    }

    private byte[] fillBuffer(int value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(value);
        return byteBuffer.array();
    }
}
