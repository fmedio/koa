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

package net.rhapso.koa.storage;

import clutter.BaseTestCase;
import clutter.Fallible;
import clutter.IntegrationTest;
import org.apache.commons.io.FileUtils;

import java.io.File;

@IntegrationTest
public class FileAddressableTest extends BaseTestCase {
    private File file;

    public void testExtentAllocationOnWrite() throws Exception {
        final FileAddressable fileAddressable = new FileAddressable(file);
        assertEquals(0, fileAddressable.getCurrentLength());
        fileAddressable.seek(100);
        assertFailure(RuntimeException.class, new Fallible() {
            @Override
            public void execute() throws Exception {
                fileAddressable.read(new byte[8]);
            }
        });
        fileAddressable.write(new byte[1]);
        assertEquals(101, fileAddressable.getCurrentLength());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        file = new File("./addressable");
        FileUtils.deleteQuietly(file);
        file.createNewFile();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        FileUtils.deleteQuietly(file);
    }
}
