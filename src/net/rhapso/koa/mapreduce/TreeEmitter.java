package net.rhapso.koa.mapreduce;


import net.rhapso.koa.StorageFactory;
import net.rhapso.koa.bag.TreeBag;
import net.rhapso.koa.storage.block.BlockSize;
import net.rhapso.koa.storage.block.LRUCache;
import net.rhapso.koa.tree.Key;
import net.rhapso.koa.tree.StoreName;
import net.rhapso.koa.tree.Value;

import java.io.Serializable;

public class TreeEmitter<K extends Serializable, V extends Serializable> implements Emitter<K, V> {
    private TreeBag multiTree;

    public TreeEmitter(StorageFactory storageFactory, int taskId) {
        this.multiTree = TreeBag.open(new StoreName("task" + taskId), storageFactory, new LRUCache(10000, new BlockSize(4096)));
    }

    @Override
    public void emit(K key, V value) {
        try {
            multiTree.put(new Key((Serializable) key), new Value((Serializable) value));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public TreeBag getMultiTree() {
        return multiTree;
    }
}
