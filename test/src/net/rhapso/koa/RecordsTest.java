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

import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.IntIO;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RecordsTest extends BaseTreeTestCase {
    @Test
    public void testReadWrite() {
        int randomInt = 324324;
        Addressable addressable = makeAddressable();
        Records<Integer> intRecords = new Records<Integer>(addressable, new IntIO());
        intRecords.put(42, randomInt);
        intRecords.put(84, 2 * randomInt);
        assertEquals(randomInt, (int) intRecords.get(42));
        assertEquals(randomInt * 2, (int) intRecords.get(84));
        assertEquals(0, (int) intRecords.get(100));
    }
}
