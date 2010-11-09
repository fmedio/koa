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

public class InnerNode extends Node {
    private final Children children;

    public InnerNode(NodeFactory nodeFactory, KeySet keySet, StoredLong parent, StoredLong ref, Children children) {
        super(nodeFactory, keySet, parent, ref);
        this.children = children;
    }

    InnerNode add(KeyRef keyRef, Node child) {
        int insertionPoint = keys().insertionPoint(keyRef, true);
        keys().add(insertionPoint, keyRef);
        children.add(insertionPoint + 1, child);

        if (keys().size() > getNodeFactory().getOrder().asInt()) {
            return getNodeFactory().split(this);
        }
        return this;
    }

    void insertChild(Node node) {
        children.add(children.size(), node);
    }

    @Override
    public NodeRef put(Key key, Value value) {
        int i = keys().insertionPoint(key, true);
        Node node = children.resolve(i);
        try {
            node.put(key, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (getParent().isNull()) {
            return this.getNodeRef();
        }

        return getParent();
    }

    @Override
    public boolean contains(Key key) {
        Node node = findNodeFor(key);
        return node.contains(key);
    }

    @Override
    public Value get(Key key) {
        Node node = findNodeFor(key);
        return node.get(key);
    }

    @Override
    public KeyRef referenceOf(Key key) {
        Node node = findNodeFor(key);
        return node.referenceOf(key);
    }

    @Override
    public Cursor<Key> cursorAt(Key key) {
        return findNodeFor(key).cursorAt(key);
    }

    @Override
    public Cursor cursorAtOrAfter(Key key) {
        return findNodeFor(key).cursorAt(key);
    }

    private Node findNodeFor(Key key) {
        int i = keys().insertionPoint(key, false);
        return children.resolve(i);
    }

    public Children children() {
        return children;
    }
}