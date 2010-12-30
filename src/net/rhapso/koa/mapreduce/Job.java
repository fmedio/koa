package net.rhapso.koa.mapreduce;

import clutter.Source;

import java.io.Serializable;

public interface Job<InputType extends Serializable,
        Key extends Serializable & Comparable<Key>,
        IntermediaryValue extends Serializable & Comparable<IntermediaryValue>,
        OutputValue extends Serializable> {
    public Source<InputType> getInput();

    public Mapper<InputType, Key, IntermediaryValue> makeMapper();

    public Reducer<Key, IntermediaryValue, OutputValue> makeReducer();

    public Output<Key, OutputValue> makeOutput();
}
