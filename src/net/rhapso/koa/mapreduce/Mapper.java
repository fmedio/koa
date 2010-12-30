package net.rhapso.koa.mapreduce;

import java.io.Serializable;

public interface Mapper<I, K extends Serializable & Comparable<K>, V extends Serializable> {
    public void map(I input, Emitter<K, V> emitter);
}
