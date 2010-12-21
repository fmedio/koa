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

import clutter.BaseTestCase;
import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.MemoryAddressable;
import net.rhapso.koa.storage.Offset;
import net.rhapso.koa.storage.StorageSize;
import net.rhapso.koa.storage.block.BlockSize;

import static org.mockito.Mockito.*;

public class TreeControlTest extends BaseTestCase {
    public void testInitialize() throws Exception {
        Addressable addressable = mock(Addressable.class);
        TreeControl treeControl = TreeControl.initialize(addressable, new BlockSize(randomInt), new Order(3));
        verify(addressable, times(1)).writeLong(3l);
        verify(addressable, times(2)).writeLong(0l);
        verify(addressable, times(1)).writeLong(eq(TreeControl.storageSize().asLong()));
        verify(addressable, times(1)).writeLong(-1l);
        verify(addressable, times(1)).writeLong(randomInt);
    }

    public void testRead() throws Exception {
        Addressable addressable = new MemoryAddressable(1000);
        TreeControl treeControl = TreeControl.initialize(addressable, new BlockSize(randomInt), new Order(3));

        Offset offset = treeControl.allocate(new StorageSize(randomLong));
        assertEquals(TreeControl.storageSize().asLong(), offset.asLong());
        offset = treeControl.allocate(new StorageSize(42));
        assertEquals(TreeControl.storageSize().plus(randomLong).asLong(), offset.asLong());

        treeControl.setRootNode(new NodeRef(randomLong));
        assertEquals(new NodeRef(randomLong), treeControl.getRootNode());
    }

    public void testAllocate() throws Exception {
        Addressable addressable = mock(Addressable.class);

        when(addressable.readInt()).thenReturn(randomInt);
        when(addressable.nextInsertionLocation(any(Offset.class), anyLong())).thenReturn(new Offset(randomLong));

        TreeControl treeControl = TreeControl.initialize(addressable, new BlockSize(1024), new Order(3));
        treeControl.allocate(new StorageSize(42));

        verify(addressable, times(1)).nextInsertionLocation(new Offset(0), 42l);
    }
}
