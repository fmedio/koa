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
