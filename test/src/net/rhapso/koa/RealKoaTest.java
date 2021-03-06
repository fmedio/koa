package net.rhapso.koa;

import net.rhapso.koa.storage.FileStorageFactory;
import net.rhapso.koa.storage.block.BlockSize;
import net.rhapso.koa.storage.block.LRUCache;
import net.rhapso.koa.tree.Key;
import net.rhapso.koa.tree.Koa;
import net.rhapso.koa.tree.StoreName;
import net.rhapso.koa.tree.Value;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RealKoaTest extends BaseTreeTestCase {
    private File directory;
    
    @Before
    public void setUp() throws Exception {
        directory = new File("foo");
        FileUtils.deleteQuietly(directory);
        directory.mkdir();
    }

    @Test
    public void testDoIt() {
        Koa tree = Koa.open(new StoreName("foo"), new FileStorageFactory(directory), new LRUCache(2048, new BlockSize(4096)));
        Key left = new Key("foo".getBytes());
        tree.put(left, new Value("bar".getBytes()));
        Key right = new Key("fooo".getBytes());
        tree.put(right, new Value("pooop".getBytes()));
        assertTrue(tree.contains(left));
        assertTrue(tree.contains(right));
        assertEquals("bar", new String(tree.get(left).getBytes()));
        assertEquals("pooop", new String(tree.get(right).getBytes()));
    }

    @Test
    public void testCrudePerformance() {
        Koa tree = Koa.open(new StoreName("foo"), new FileStorageFactory(directory), new LRUCache(10000, new BlockSize(4096)));
        long then = System.currentTimeMillis();
        for (int i = 0; i < 3000; i++) {
            tree.put(new Key(Integer.toString(i).getBytes()), new Value(Integer.toString(i).getBytes()));
        }
        long elapsed = System.currentTimeMillis() - then;
        System.out.println("Elapsed: " + elapsed);
        tree.flush();
        System.out.println("Done flushing");

        then = System.currentTimeMillis();
        for (int i = 0; i < 3000; i++) {
            Value value = tree.get(new Key(Integer.toString(i).getBytes()));
            assertNotNull(value);
        }
        elapsed = System.currentTimeMillis() - then;
        System.out.println("Elapsed: " + elapsed);
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteQuietly(directory);
    }
}
