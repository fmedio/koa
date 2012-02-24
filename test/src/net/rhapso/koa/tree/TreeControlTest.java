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

package net.rhapso.koa.tree;

import net.rhapso.koa.BaseTreeTestCase;
import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.Offset;
import net.rhapso.koa.storage.StorageSize;
import net.rhapso.koa.storage.block.BlockSize;

import static org.mockito.Mockito.*;

public class TreeControlTest extends BaseTreeTestCase {
    private Addressable mockAddressable;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        mockAddressable = mock(Addressable.class);
        when(mockAddressable.getBlockSize()).thenReturn(new BlockSize(randomInt));
    }

    public void testRead() throws Exception {
        Addressable addressable = makeAddressable();
        TreeControl treeControl = TreeControl.initialize(addressable, new Order(3));

        Offset offset = treeControl.allocate(new StorageSize(100));
        assertEquals(TreeControl.storageSize().asLong(), offset.asLong());
        offset = treeControl.allocate(new StorageSize(42));
        assertEquals(TreeControl.storageSize().plus(100).asLong(), offset.asLong());

        treeControl.setRootNode(new NodeRef(100));
        assertEquals(new NodeRef(100), treeControl.getRootNode());
    }

    public void testAllocate() throws Exception {
        when(mockAddressable.readInt(anyInt())).thenReturn(randomInt);
        when(mockAddressable.nextInsertionLocation(any(Offset.class), anyLong())).thenReturn(new Offset(randomLong));

        TreeControl treeControl = TreeControl.initialize(mockAddressable, new Order(3));
        treeControl.allocate(new StorageSize(42));

        verify(mockAddressable, times(1)).nextInsertionLocation(new Offset(0), 42l);
    }
    
    public void testFlush() {
        when(mockAddressable.flush()).thenReturn(true);
        TreeControl treeControl = TreeControl.initialize(mockAddressable, new Order(3));
        treeControl.flush();
        verify(mockAddressable, times(1)).flush();
    }
}
