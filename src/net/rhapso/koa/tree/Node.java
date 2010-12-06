/*
 * The MIT License
 *
 * Copyright (c) 2010 Fabrice Medio <fmedio@gmail.com>
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

import java.util.Iterator;

public abstract class Node {
    private final KeySet keySet;
    private final NodeFactory nodeFactory;
    private final StoredLong parent;
    private final StoredLong reference;

    public Node(NodeFactory nodeFactory, KeySet keySet, StoredLong parent, StoredLong reference) {
        this.nodeFactory = nodeFactory;
        this.keySet = keySet;
        this.parent = parent;
        this.reference = reference;
    }

    protected NodeRef getParent() {
        return new NodeRef(parent.read());
    }

    KeySet keys() {
        return keySet;
    }

    public NodeRef getNodeRef() {
        return new NodeRef(reference.read());
    }

    public NodeFactory getNodeFactory() {
        return nodeFactory;
    }

    public void setParent(NodeRef parentRef) {
        parent.write(parentRef.asLong());
    }

    public InnerNode getParentNode() {
        return (InnerNode) nodeFactory.read(getParent());
    }

    public abstract NodeRef put(Key key, Value value);

    public abstract boolean contains(Key key);

    public abstract Iterator<Key> cursorAt(Key key);

    public abstract Value get(Key key);

    public abstract Iterator<Key> cursorAtOrAfter(Key key);

    public abstract KeyRef referenceOf(Key key);

    public Key key(KeyRef keyRef) {
        return getNodeFactory().readKey(keyRef);
    }
}
