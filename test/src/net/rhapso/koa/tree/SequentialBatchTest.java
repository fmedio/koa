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

import clutter.IntegrationTest;
import net.rhapso.koa.BaseTreeTestCase;

import static org.mockito.Mockito.*;

@IntegrationTest
public class SequentialBatchTest extends BaseTreeTestCase {
    public void testThreadSafety() throws Exception {
        Tree tree = new BlockingTree();
        final SequentialBatch batchInsert = new SequentialBatch(tree);
        attempt(new AddAndFlushAttempt(batchInsert));
        expectEvent("barrier crossed");
        attempt(new AddAndFlushAttempt(batchInsert));
        assertNoEventsSoFar();
    }

    public void testInsert() throws Exception {
        Tree tree = mock(Tree.class);
        SequentialBatch insert = new SequentialBatch(tree);
        insert.add(key(0), value(0));
        insert.add(key(1), value(1));
        verifyZeroInteractions(tree);
        insert.flush();
        verify(tree, times(1)).put(key(0), value(0));
        verify(tree, times(1)).put(key(1), value(1));
        insert.flush();
        verifyNoMoreInteractions(tree);
    }

    private class BlockingTree extends MemoryTree {
        private BlockingTree() {
            super();
        }

        @Override
        public boolean put(Key key, Value value) {
            addEvent("barrier crossed");
            block();
            return true;
        }
    }

    private class AddAndFlushAttempt implements Runnable {
        private final SequentialBatch batchInsert;

        public AddAndFlushAttempt(SequentialBatch batchInsert) {
            this.batchInsert = batchInsert;
        }

        @Override
        public void run() {
            batchInsert.add(key(0), value(0));
            batchInsert.flush();
        }
    }
}
