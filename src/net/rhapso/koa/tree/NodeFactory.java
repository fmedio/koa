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

import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.ByteIO;
import net.rhapso.koa.storage.Offset;
import net.rhapso.koa.storage.StorageSize;

public class NodeFactory {
    private final Addressable addressable;
    private final TreeControl treeControl;
    private final LeafNodeScribe leafNodeScribe;
    private final InnerNodeScribe innerNodeScribe;
    private final Order order;

    public NodeFactory(Addressable addressable) {
        this(addressable, new TreeControl(addressable));
    }

    public static NodeFactory initialize(Addressable addressable, Order order) {
        TreeControl treeControl = TreeControl.initialize(addressable, order);
        return new NodeFactory(addressable, treeControl);
    }

    public NodeFactory(Addressable addressable, TreeControl treeControl) {
        this.treeControl = treeControl;
        this.order = treeControl.getOrder();
        this.addressable = addressable;
        leafNodeScribe = new LeafNodeScribe(order);
        innerNodeScribe = new InnerNodeScribe(order);
    }

    Node read(Offset offset) {
        byte typeMarker = new ByteIO().read(addressable, offset);
        Scribe scribe = Scribe.NodeType.choose(typeMarker, leafNodeScribe, innerNodeScribe);
        return scribe.read(this, addressable, offset);
    }

    private Node createNode(NodeRef parent, Scribe scribe) {
        Offset from = treeControl.allocate(scribe.storageSize());
        new ByteIO().write(addressable, from, scribe.nodeType().asByte());
        return scribe.create(from, this, addressable, parent);
    }

    public KeyRef append(Key key) {
        byte[] buf = key.bytes();
        return new KeyRef(write(buf).asLong());
    }

    public ValueRef append(Value value) {
        long offset = write(value.bytes()).asLong();
        return new ValueRef(offset);
    }

    private Offset write(byte[] buf) {
        Offset offset = treeControl.allocate(new StorageSize(4 + buf.length));
        addressable.writeInt(offset.asLong(), buf.length);
        addressable.write(offset.asLong() + 4, buf);
        return offset;
    }

    public Value readValue(ValueRef valueRef) {
        return new Value(readBytes(valueRef.asLong()));
    }

    public Key readKey(KeyRef keyRef) {
        return new Key(readBytes(keyRef.asLong()));
    }

    byte[] readBytes(long position) {
        byte[] bytes = new byte[addressable.readInt(position)];
        addressable.read(position + 4, bytes);
        return bytes;
    }

    Node read(NodeRef nodeRef) {
        return read(nodeRef.getOffset());
    }

    InnerNode split(InnerNode innerNode) {
        return new InnerNodeSplitter().splitInto(this, innerNode, newInnerNode(NodeRef.NULL));
    }

    public Order getOrder() {
        return order;
    }

    LeafNode newLeafNode() {
        return (LeafNode) createNode(NodeRef.NULL, leafNodeScribe);
    }

    InnerNode newInnerNode(NodeRef parent) {
        return (InnerNode) createNode(parent, innerNodeScribe);
    }

    public TreeControl getTreeControl() {
        return treeControl;
    }

}
