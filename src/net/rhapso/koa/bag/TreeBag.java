package net.rhapso.koa.bag;

import clutter.Iterators;
import net.rhapso.koa.StorageFactory;
import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.block.BlockSize;
import net.rhapso.koa.storage.block.Cache;
import net.rhapso.koa.storage.block.LRUCache;
import net.rhapso.koa.tree.*;

import java.util.Iterator;

/**
 * Tree implementation that supports multiple values for each key
 */
public class TreeBag implements Tree {
    private Tree keys;
    private MappedMultiValues mappedMultiValues;

    public TreeBag(Tree keys, Addressable values) {
        this.keys = keys;
        mappedMultiValues = new MappedMultiValues(values);
    }

    public static TreeBag initialize(Tree keys, Addressable addressable) {
        MappedMultiValues.initialize(addressable);
        return new TreeBag(keys, addressable);
    }

    public static TreeBag open(StoreName storeName, StorageFactory storageFactory, Cache keyCache) {
        if (storageFactory.exists(storeName)) {
            Tree keys = LocalTree.open(storeName, storageFactory, keyCache);
            Addressable addressable = storageFactory.openAddressable(storeName.append("_data"), new LRUCache(100, new BlockSize(4096)));
            return new TreeBag(keys, addressable);
        } else {
            Tree keys = LocalTree.open(storeName, storageFactory, keyCache);
            Addressable addressable = storageFactory.openAddressable(storeName.append("_data"), new LRUCache(100, new BlockSize(4096)));
            return TreeBag.initialize(keys, addressable);
        }
    }

    @Override
    public boolean put(Key key, Value value) {
        Value existing = keys.get(key);
        MultiValueRef multiValueRef;
        if (existing == null) {
            multiValueRef = mappedMultiValues.create();
            keys.put(key, new Value(multiValueRef.asBytes()));
        } else {
            multiValueRef = new MultiValueRef(existing.asLong());
        }

        mappedMultiValues.append(value, multiValueRef);
        return true;
    }

    @Override
    public Value get(Key key) {
        Value ref = keys.get(key);
        if (ref == null) {
            return null;
        }

        return mappedMultiValues.get(new MultiValueRef(ref.asLong())).next();
    }

    @Override
    public boolean contains(Key key) {
        return keys.contains(key);
    }

    @Override
    public long count() {
        return keys.count();
    }

    @Override
    public Iterator<Key> cursorAt(Key key) {
        return keys.cursorAt(key);
    }

    @Override
    public Iterator<Key> cursorAtOrAfter(Key key) {
        return keys.cursorAtOrAfter(key);
    }

    @Override
    public KeyRef referenceOf(Key key) {
        return keys.referenceOf(key);
    }

    @Override
    public Key key(KeyRef ref) {
        return keys.key(ref);
    }

    public Iterator<Value> getValues(Key key) {
        Value value = keys.get(key);
        if (value == null) {
            return Iterators.NULL;
        }

        MultiValueRef multiValueRef = new MultiValueRef(value.asLong());
        return mappedMultiValues.get(multiValueRef);
    }
}
