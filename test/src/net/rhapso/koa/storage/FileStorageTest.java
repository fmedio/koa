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

package net.rhapso.koa.storage;

import net.rhapso.koa.BaseTreeTestCase;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class FileStorageTest extends BaseTreeTestCase {
    private File file;

    @Test
    public void testExtentAllocationOnWrite() throws Exception {
        final FileStorage fileStorage = new FileStorage(file);
        assertEquals(0, fileStorage.getCurrentLength());
        fileStorage.seek(100);
        assertFailure(RuntimeException.class, () -> fileStorage.read(new byte[8]));
        fileStorage.write(new byte[1]);
        assertEquals(101, fileStorage.getCurrentLength());
    }

    @Before
    public void setUp() throws Exception {
        file = new File("./addressable");
        FileUtils.deleteQuietly(file);
        file.createNewFile();
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteQuietly(file);
    }
}
