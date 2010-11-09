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

import java.util.concurrent.LinkedBlockingQueue;

@IntegrationTest
public class TreeGuardTest extends BaseTreeTestCase {
    private LinkedBlockingQueue<String> events;

    public void testLocks() throws Exception {
        final Tree tree = new BlockingGetTree();
        final TreeGuard guard = new TreeGuard(tree);

        attempt(new GetAttempt(guard));
        assertEquals("barrier crossed", events.take());
        attempt(new GetAttempt(guard));

        assertEquals(0, events.size());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        events = new LinkedBlockingQueue<String>();
    }

    private class BlockingGetTree extends MemoryTree {
        private BlockingGetTree() {
            super();
        }

        @Override
        public Value get(Key key) {
            events.add("barrier crossed");
            block();
            return new Value(42);
        }
    }

    private class GetAttempt implements Runnable {
        private final TreeGuard guard;

        public GetAttempt(TreeGuard guard) {
            this.guard = guard;
        }

        @Override
        public void run() {
            guard.get(key(randomByte));
        }
    }
}
