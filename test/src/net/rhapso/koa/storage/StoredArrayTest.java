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
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class StoredArrayTest extends BaseTreeTestCase {
    private Addressable addressable;
    private StoredArray<Integer> array;

    @Test
    public void testInitialize() throws Exception {
        LongIO longIo = new LongIO();
        MaxSize maxSize = new MaxSize(42);
        Addressable addressable = mock(Addressable.class);

        StoredArray<Long> result = StoredArray.initialize(longIo, addressable, maxSize, new Offset(0));

        int storageSize = StoredArray.storageSize(longIo, maxSize).intValue();
        verify(addressable, times(1)).write(0, new byte[storageSize]);
    }

    @Test
    public void testInsertHastBounds() throws Exception {
        assertEquals(0, array.size());
        assertFailure(ArrayIndexOutOfBoundsException.class, () -> array.add(1, 0));
        array.add(0, 0);
        assertEquals(1, array.size());
        array.add(0, 1);
        assertEquals(2, array.size());
        array.add(0, 2);
        assertEquals(3, array.size());
        array.add(0, 3);
        assertEquals(4, array.size());

        assertFailure(ArrayIndexOutOfBoundsException.class, () -> array.add(0, 42));
    }

    @Test
    public void testRemoveHasBounds() throws Exception {
        assertEquals(0, array.size());
        assertFailure(ArrayIndexOutOfBoundsException.class, () -> array.remove(0));
        array.add(0, 0);
        array.add(1, 1);
        array.add(2, 2);
        assertEquals(1, (int) array.remove(1));
        assertEquals(2, array.size());
        assertEquals("0 2", array.toString());
    }

    @Test
    public void testRemoveFromBounds() throws Exception {
        array = new StoredArray<Integer>(new IntIO(), addressable, new MaxSize(10), new Offset(0));
        for (int i = 0; i < 10; i++) {
            array.add(i, i);
        }
        assertEquals("0 1 2 3 4 5 6 7 8 9", array.toString());
        assertEquals(0, (int) array.remove(0));
        assertEquals(9, (int) array.remove(8));
        assertEquals("1 2 3 4 5 6 7 8", array.toString());
    }

    @Test
    public void testMap() throws Exception {
        array.add(0, 0);
        array.add(1, 1);
        array.add(2, 3);
        array.add(2, 2);
        assertEquals("0 1 2 3", array.toString());
    }

    @Test
    public void testSet() throws Exception {
        array.add(0);
        array.add(1);
        array.add(2);
        array.set(0, 42);
        assertEquals("42 1 2", array.toString());
        assertFailure(ArrayIndexOutOfBoundsException.class, () -> array.set(3, 42));
    }

    @Before
    public void setUp() throws Exception {
        addressable = makeAddressable();
        array = new StoredArray<Integer>(new IntIO(), addressable, new MaxSize(4), new Offset(0));
    }
}
