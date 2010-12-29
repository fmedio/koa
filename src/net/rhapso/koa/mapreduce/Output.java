package net.rhapso.koa.mapreduce;

import java.io.Serializable;

public interface Output<K extends Serializable, V extends Serializable> {
    public void output(K key, V value);
}
