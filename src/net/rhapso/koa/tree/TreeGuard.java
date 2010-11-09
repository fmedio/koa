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

public class TreeGuard implements Tree {
    private final Tree tree;

    public TreeGuard(Tree tree) {
        this.tree = tree;
    }

    @Override
    public synchronized boolean put(Key key, Value value) {
        return tree.put(key, value);
    }

    @Override
    public synchronized Value get(Key key) {
        return tree.get(key);
    }

    @Override
    public synchronized boolean contains(Key key) {
        return tree.contains(key);
    }

    @Override
    public synchronized long count() {
        return tree.count();
    }

    @Override
    public synchronized Cursor cursorAt(final Key key) {
        return new CursorGuard(tree.cursorAt(key));
    }

    @Override
    public synchronized Cursor cursorAtOrAfter(Key key) {
        return new CursorGuard(tree.cursorAtOrAfter(key));
    }

    @Override
    public synchronized KeyRef referenceOf(Key key) {
        return tree.referenceOf(key);
    }

    @Override
    public synchronized Key key(KeyRef ref) {
        return tree.key(ref);
    }

    @Override
    public synchronized void flush() {
        tree.flush();
    }

    @Override
    public Batch createBatch() {
        return new SequentialBatch(this);
    }

    private class CursorGuard implements Cursor<Key> {
        private final Cursor<Key> cursor;

        public CursorGuard(Cursor<Key> cursor) {
            this.cursor = cursor;
        }

        @Override
        public Key next() {
            synchronized (TreeGuard.this) {
                return cursor.next();
            }
        }

        @Override
        public boolean hasNext() {
            synchronized (TreeGuard.this) {
                return cursor.hasNext();
            }
        }
    }
}
