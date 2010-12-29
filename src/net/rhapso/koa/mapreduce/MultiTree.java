package net.rhapso.koa.mapreduce;

import net.rhapso.koa.tree.Key;
import net.rhapso.koa.tree.KeyRef;
import net.rhapso.koa.tree.Tree;
import net.rhapso.koa.tree.Value;

import java.util.Iterator;

/**
 * Tree implementation that supports duplicate keys
 */
public class MultiTree implements Tree {
    @Override
    public boolean put(Key key, Value value) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Value get(Key key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean contains(Key key) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long count() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterator<Key> cursorAt(Key key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterator<Key> cursorAtOrAfter(Key key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public KeyRef referenceOf(Key key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Key key(KeyRef ref) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

}
