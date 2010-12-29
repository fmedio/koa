package net.rhapso.koa.bag;

import clutter.Iterators;
import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.tree.Key;
import net.rhapso.koa.tree.KeyRef;
import net.rhapso.koa.tree.Tree;
import net.rhapso.koa.tree.Value;

import java.util.Iterator;

/**
 * Tree implementation that supports multiple values for
 */
public class MultiTree implements Tree {
    private Tree keys;
    private Addressable addressable;
    private MappedMultiValues mappedMultiValues;

    public MultiTree(Tree keys, Addressable values) {
        this.keys = keys;
        this.addressable = values;
        mappedMultiValues = new MappedMultiValues(addressable);
    }

    public static MultiTree initialize(Tree keys, Addressable addressable) {
        MappedMultiValues.initialize(addressable);
        return new MultiTree(keys, addressable);
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
