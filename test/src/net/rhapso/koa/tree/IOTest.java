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

import clutter.BaseTestCase;
import clutter.PerformanceTest;
import net.rhapso.koa.LocalAddressableFactory;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.Iterator;

@PerformanceTest
public class IOTest extends BaseTestCase {

    private File data;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        data = new File("perftest");
        FileUtils.deleteQuietly(data);
        data.mkdir();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        FileUtils.deleteQuietly(data);
    }

    public void testVeryLargeRun() throws Exception {
        runLocal(2000000, 5000);
    }

    public void testLargeRun() throws Exception {
        runLocal(50000, 500);
    }

    public void testSmallRun() throws Exception {
        runLocal(5000, 500);
    }

    private void runLocal(int howMany, int samplingRate) {
        LocalAddressableFactory factory = new LocalAddressableFactory(data);
        Tree tree = LocalTree.open(new StoreName("test"), factory);
        doRun(howMany, samplingRate, tree);
    }

    private void doRun(int howMany, int samplingRate, Tree tree) {
        long insertionElapsed = 0;
        long retrievalElapsed = 0;

        tree.put(new Key(new byte[1]), new Value(0));
        System.out.println("keyCount, writeThroughput, readThroughput");
        for (int i = 0; i < howMany; i++) {
            byte[] bytes = makeRandomBytes(100);
            Key key = new Key(bytes);
            long insertionThen = System.nanoTime();
            tree.put(key, new Value(0));
            insertionElapsed += System.nanoTime() - insertionThen;


            long retrievalThen = System.nanoTime();
            boolean containsKey = tree.contains(key);
            retrievalElapsed += System.nanoTime() - retrievalThen;
            if (!containsKey) {
                fail();
            }

            if (i != 0 && i % samplingRate == 0) {
                long avgInsertionTime = insertionElapsed / samplingRate;
                long avgRetrievalTime = retrievalElapsed / samplingRate;

                double writeThroughput = 1000000000d / (double) avgInsertionTime;
                double readThroughput = 1000000000d / (double) avgRetrievalTime;

                System.out.println(i + "," + writeThroughput + "," + readThroughput);
                insertionElapsed = 0;
                retrievalElapsed = 0;
            }
        }
        tree.flush();
        Iterator iterator = tree.cursorAt(new Key(new byte[1]));
        long then = System.nanoTime();
        while (iterator.hasNext()) {
            iterator.next();
        }
        long elapsed = System.nanoTime() - then;
        System.out.println("Cursor can read " + (((double) howMany / (double) elapsed) * 1000000000d) + " keys/sec");
    }

}