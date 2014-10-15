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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import net.rhapso.koa.BaseTreeTestCase;
import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.Offset;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class KeySetTest extends BaseTreeTestCase {
    private KeySet keySet;
    private NodeFactory nodeFactory;
    private Addressable addressable;
    private Order order;


    @Test
    public void testStorageSize() throws Exception {
        assertEquals(44, KeySet.storageSize(new Order(4)).intValue());
    }

    @Test
    public void testOffsetOf() throws Exception {
        registerKeys(Lists.newArrayList(
                key(2),
                key(4),
                key(6)
        ));

        assertEquals(-1, keySet.offsetOf(key(1)));
        assertEquals(0, keySet.offsetOf(key(2)));
        assertEquals(1, keySet.offsetOf(key(4)));
        assertEquals(2, keySet.offsetOf(key(6)));
        assertEquals(-1, keySet.offsetOf(key(8)));
    }

    @Test
    public void testInsertionPoint() throws Exception {
        registerKeys(Lists.newArrayList(
                key(2),
                key(4),
                key(6)
        ));

        assertEquals(0, keySet.insertionPoint(key(1)));
        assertEquals(1, keySet.insertionPoint(key(3)));
        assertEquals(2, keySet.insertionPoint(key(5)));
        assertEquals(3, keySet.insertionPoint(key(7)));
    }

    @Test
    public void testContains() throws Exception {
        List<Key> keys = Lists.newArrayList(
                key(makeRandomBytes(3)),
                key(makeRandomBytes(2)),
                key(makeRandomBytes(5))
        );

        registerKeys(keys);

        assertTrue(keySet.contains(keys.get(0)));
        assertTrue(keySet.contains(keys.get(1)));
        assertTrue(keySet.contains(keys.get(2)));
        assertFalse(keySet.contains(key(makeRandomBytes(42))));
    }

    @Test
    public void testSplitInto() throws Exception {
        registerKeys(Lists.newArrayList(
                key(1),
                key(2),
                key(3),
                key(4),
                key(5)
        ));
        KeySet destination = KeySet.initialize(nodeFactory, addressable, new Offset(300), order);
        keySet.splitInto(destination);
        assertEquals("[4] [5]", destination.toString());
    }


    private void registerKeys(List<Key> keys) {
        List<KeyRef> refs = Lists.transform(keys, new Function<Key, KeyRef>() {
            @Override
            public KeyRef apply(Key key) {
                return nodeFactory.append(key);
            }
        });

        for (KeyRef ref : refs) {
            keySet.add(ref);
        }
    }

    @Before
    public void setUp() throws Exception {
        addressable = makeAddressable();
        order = new Order(4);
        nodeFactory = NodeFactory.initialize(addressable, order);
        keySet = new KeySet(nodeFactory, addressable, new Offset(200), order);
    }
}
