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
import net.rhapso.koa.storage.*;

import static org.mockito.Mockito.*;

public class RecordsTest extends BaseTestCase {
    public void testReadWrite() {
        Addressable addressable = new MemoryAddressable(1024);
        Records<Integer> intRecords = new Records<Integer>(addressable, new IntIO());
        intRecords.put(42, randomInt);
        intRecords.put(84, 2 * randomInt);
        assertEquals(randomInt, (int) intRecords.get(42));
        assertEquals(randomInt * 2, (int) intRecords.get(84));
    }

    public void testReadPastCurrentBoundary() {
        Addressable addressable = mock(Addressable.class);
        IntIO io = mock(IntIO.class);
        when(addressable.length()).thenReturn(0l);
        when(addressable.nextInsertionLocation(any(Offset.class), anyLong())).thenReturn(new Offset(4));
        when(io.storageSize()).thenReturn(new StorageSize(4));

        Records<Integer> intRecords = new Records<Integer>(addressable, io);
        intRecords.get(1);
        verify(addressable, times(1)).length();
        verify(addressable, times(2)).seek(4);
        verify(addressable, times(4)).write(0);
    }
}
