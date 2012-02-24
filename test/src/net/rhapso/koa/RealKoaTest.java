package net.rhapso.koa;

import clutter.IntegrationTest;
import junit.framework.TestCase;
import net.rhapso.koa.storage.FileStorageFactory;
import net.rhapso.koa.storage.block.BlockSize;
import net.rhapso.koa.storage.block.LRUCache;
import net.rhapso.koa.tree.*;
import org.apache.commons.io.FileUtils;

import java.io.File;

@IntegrationTest
public class RealKoaTest extends TestCase {    
    private File directory;
    
    @Override
    protected void setUp() throws Exception {
        directory = new File("foo");
        FileUtils.deleteQuietly(directory);
        directory.mkdir();
    }

    public void testDoIt() {
        Koa tree = Koa.open(new StoreName("foo"), new FileStorageFactory(directory), new LRUCache(2048, new BlockSize(4096)));
        Key left = new Key("foo".getBytes());
        tree.put(left, new Value("bar".getBytes()));
        Key right = new Key("fooo".getBytes());
        tree.put(right, new Value("bar".getBytes()));
        assertTrue(tree.contains(left));
        assertTrue(tree.contains(right));
    }

    @Override
    protected void tearDown() throws Exception {
        FileUtils.deleteQuietly(directory);
    }
}
