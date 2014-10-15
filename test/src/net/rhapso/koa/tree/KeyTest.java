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

import com.google.common.base.Joiner;
import net.rhapso.koa.BaseTreeTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class KeyTest extends BaseTreeTestCase {
    @Test
    public void testSort() {
        List<Key> keys = new ArrayList<Key>();
        for (String string : new String[]{"ffff", "b", "bc", "bbb", "dd", "de", "deffff"}) {
            keys.add(new Key(string.getBytes()));
        }

        Key[] sortedKeys = keys.toArray(new Key[0]);
        Arrays.sort(sortedKeys);
        String[] sorted = stringify(sortedKeys);

        assertEquals("b bbb bc dd de deffff ffff", Joiner.on(" ").join(sorted));
    }

    @Test
    public void testEmptyKey() throws Exception {
        assertTrue(new Key(new byte[]{}).compareTo(new Key(new byte[]{0})) < 0);
    }

    @Test
    public void testLargerKeys() throws Exception {
        byte[] bytes = makeRandomBytes(100);
        assertEquals(new Key(bytes).hashCode(), new Key(bytes).hashCode());
    }

    @Test
    public void testCompare() throws Exception {
        assertTrue(new Key(new byte[]{1}).compareTo(new Key(new byte[]{2})) < 0);
        assertTrue(new Key(new byte[]{-10}).compareTo(new Key(new byte[]{-1})) < 0);
        assertTrue(new Key(new byte[]{1}).compareTo(new Key(new byte[]{1})) == 0);
        assertTrue(new Key(new byte[]{1}).compareTo(new Key(new byte[]{0})) > 0);
        assertTrue(new Key(new byte[]{1, 1}).compareTo(new Key(new byte[]{1, 1})) == 0);
    }

    @Test
    public void testEqualsAndHashcode() throws Exception {
        assertEquals(new Key(new byte[]{0}), new Key(new byte[]{0}));
        assertEquals(new Key(new byte[]{0}).hashCode(), new Key(new byte[]{0}).hashCode());
        assertNotEquals(new Key(new byte[]{0}), new Key(new byte[]{1}));
        assertNotEquals(new Key(new byte[]{0}).hashCode(), new Key(new byte[]{1}).hashCode());
    }

    @Test
    public void testToString() throws Exception {
        assertEquals("[1,2,3]", new Key(new byte[]{1, 2, 3}).toString());
    }
}
