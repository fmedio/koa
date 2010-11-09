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

import net.rhapso.koa.tree.*;

public class MemoryTree implements Tree {
    private Tree tree;

    public MemoryTree() {
        tree = LocalTree.open(new StoreName("memory"), new MemoryAddressableFactory());
    }

    @Override
    public boolean put(Key key, Value value) {
        return tree.put(key, value);
    }

    @Override
    public Value get(Key key) {
        return tree.get(key);
    }

    @Override
    public boolean contains(Key key) {
        return tree.contains(key);
    }

    @Override
    public long count() {
        return tree.count();
    }

    @Override
    public Cursor<Key> cursorAt(Key key) {
        return tree.cursorAt(key);
    }

    @Override
    public Cursor<Key> cursorAtOrAfter(Key key) {
        return tree.cursorAtOrAfter(key);
    }

    @Override
    public KeyRef referenceOf(Key key) {
        return tree.referenceOf(key);
    }

    @Override
    public Key key(KeyRef ref) {
        return tree.key(ref);
    }

    @Override
    public Batch createBatch() {
        return tree.createBatch();
    }

    @Override
    public void flush() {
        tree.flush();
    }
}
