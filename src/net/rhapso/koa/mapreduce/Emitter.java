package net.rhapso.koa.mapreduce;

import java.io.Serializable;

public interface Emitter<K extends Serializable, V extends Serializable> {
    public void emit(K key, V value);
}
