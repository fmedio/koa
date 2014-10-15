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

package net.rhapso.koa.tree;


import com.google.common.collect.Iterators;

import java.util.Iterator;

public class LeafNode extends Node {
    private final StoredLong next;
    private final Values values;

    public LeafNode(NodeFactory nodeFactory, KeySet keySet, Values values, StoredLong parent, StoredLong ref, StoredLong next) {
        super(nodeFactory, keySet, parent, ref);
        this.values = values;
        this.next = next;
    }

    @Override
    public InsertionResult put(Key key, Value value) {
        return put(key, value, new LeafNodeSplitter());
    }

    public InsertionResult put(Key key, Value value, LeafNodeSplitter splitter) {
        ValueRef valueRef = getNodeFactory().append(value);

        boolean didUpdate = false;


        int i = keys().insertionPoint(key);
        if (i < 0) {
            i = -i -1;
            values().set(i, valueRef);
            didUpdate = true;
        } else {
            KeyRef keyRef = getNodeFactory().append(key);

            keys().add(i, keyRef);
            values().add(i, valueRef);

            int order = getNodeFactory().getOrder().asInt();
            int keyCount = keys().size();
            if (keyCount > order) {
                splitter.splitInto(getNodeFactory(), this, getNodeFactory().newLeafNode());
            }

            if (!getParent().isNull()) {
                return new InsertionResult(getParent(), false);
            }
        }

        return new InsertionResult(getNodeRef(), didUpdate);
    }


    @Override
    public boolean contains(Key key) {
        return keys().contains(key);
    }

    void setNextLeafNode(NodeRef nodeRef) {
        next.write(nodeRef.asLong());
    }

    public NodeRef getNextLeafNode() {
        return new NodeRef(next.read());
    }

    Key get(int i) {
        KeyRef keyRef = keys().get(i);
        return getNodeFactory().readKey(keyRef);
    }

    int size() {
        return keys().size();
    }

    public Values values() {
        return values;
    }

    public Iterator<Key> cursorAt(Key key) {
        int offset = keys().offsetOf(key);
        if (offset == -1) {
            return Iterators.concat();
        } else {
            return new RealCursor(getNodeFactory(), this, new KeyOffset(offset));
        }
    }

    @Override
    public Iterator<Key> cursorAtOrAfter(Key key) {
        int keyOffset = keys().offsetOf(key);

        if (keyOffset != -1) {
            return new RealCursor(getNodeFactory(), this, new KeyOffset(keyOffset));
        }

        int offset = keys().insertionPoint(key);
        if (offset < 0) {
            offset = -offset;
        }
        if (offset == keys().size()) {
            NodeRef nodeRef = getNextLeafNode();

            if (NodeRef.NULL.equals(nodeRef)) {
                return Iterators.concat();
            }

            LeafNode leafNode = (LeafNode) getNodeFactory().read(nodeRef);
            return new RealCursor(
                    getNodeFactory(),
                    leafNode,
                    new KeyOffset(0));
        } else {
            return new RealCursor(getNodeFactory(), this, new KeyOffset(offset));
        }
    }

    public Value get(Key key) {
        int offset = keys().offsetOf(key);
        if (offset == -1) {
            return null;
        } else {
            return getNodeFactory().readValue(values().get(offset));
        }
    }

    @Override
    public KeyRef referenceOf(Key key) {
        int offset = keys().offsetOf(key);
        if (offset == -1) {
            return null;
        } else {
            return keys().get(offset);
        }
    }
}
