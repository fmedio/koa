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

package net.rhapso.koa.storage.block;

import clutter.BaseTestCase;

public class BlockTest extends BaseTestCase {
    public void testReadWriteInt() throws Exception {
        Block block = new Block(new byte[]{0, 0, 0, 0}, false);
        block.put(3, (byte) 42);
        assertTrue(block.isDirty());
        assertEquals(42, block.readInt(0));
        block.putInt(0, randomInt);
        assertEquals(randomInt, block.readInt(0));
    }

    public void testReadWriteBytes() throws Exception {
        Block block = new Block(new byte[8], false);
        block.put(0, new byte[]{0, 2, 3, 4, 5, 6, 7});
        assertTrue(block.isDirty());
        byte[] part = new byte[4];
        block.read(2, part);
        assertEquals(new byte[]{3, 4, 5, 6}, part);
    }
}
