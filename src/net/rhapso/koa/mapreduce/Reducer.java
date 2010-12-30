package net.rhapso.koa.mapreduce;

import java.io.Serializable;
import java.util.Iterator;

public interface Reducer<K extends Serializable, V extends Serializable, O extends Serializable> {
    public void reduce(K key, Iterator<V> values, Output<K, O> output);
}
