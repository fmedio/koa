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

import com.google.common.base.Joiner;
import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.MemoryStorage;
import net.rhapso.koa.storage.MemoryStorageFactory;
import net.rhapso.koa.storage.block.BlockSize;
import net.rhapso.koa.storage.block.LRUCache;
import net.rhapso.koa.tree.*;

import java.security.SecureRandom;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.fail;

public abstract class BaseTreeTestCase {
    public static final SecureRandom RANDOM = new SecureRandom();

   protected long randomInt = RANDOM.nextInt();
   protected long randomLong = RANDOM.nextInt();


    protected Koa memoryTree() {
        return Koa.open(new StoreName("memory"), new MemoryStorageFactory(), new LRUCache(100, BlockSize.DEFAULT));
    }

    public void assertFailure(Class<? extends Throwable> expected, Runnable runnable) {
        try {
            runnable.run();
        } catch (Throwable throwable) {
            if (throwable.getClass().equals(expected)) {
                return;
            } else {
                fail("Got a " + throwable.getClass().getName() + ", should have gotten a " + expected.getName());
            }
        }

        fail("Should have gotten a " + expected.getName());
    }

    protected Key key(byte[] bytes) {
        return new Key(bytes);
    }

    protected Key key(int singleByte) {
        return key(new byte[]{(byte) singleByte});
    }

    protected Key key(String string) {
        return key(string.getBytes());
    }

    protected Value value(int singleByte) {
        return new Value((byte) singleByte);
    }

    protected Value value(String string) {
        return new Value(string.getBytes());
    }

    protected String toString(Iterator iterator) {
        List<String> list = new LinkedList<String>();
        while (iterator.hasNext()) {
            list.add(iterator.next().toString());
        }

        return Joiner.on(", ").join(list);
    }

    public byte[] makeRandomBytes(int length) {
        byte[] bytes = new byte[length];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    protected String[] stringify(Bytes[] bytes) {
        String[] strings = new String[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            Bytes theBytes = bytes[i];
            strings[i] = new String(theBytes.bytes());
        }
        return strings;
    }

    protected String readCursor(Iterator<Key> iterator) {
        List<String> list = new LinkedList<String>();
        while (iterator.hasNext()) {
            list.add(new String(iterator.next().bytes()));
        }
        return Joiner.on(" ").join(list);
    }

    protected Addressable makeAddressable() {
        return new Addressable(new MemoryStorage(1024 * 500), new LRUCache(100, BlockSize.DEFAULT));
    }
}
