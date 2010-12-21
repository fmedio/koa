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

import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.ByteIO;
import net.rhapso.koa.storage.Offset;
import net.rhapso.koa.storage.StorageSize;
import net.rhapso.koa.storage.block.BlockSize;

public class NodeFactory {
    private final Addressable addressable;
    private final TreeControl treeControl;
    private final Order order;

    public NodeFactory(Addressable addressable) {
        this(addressable, new TreeControl(addressable));
    }

    public static NodeFactory initialize(Addressable addressable, BlockSize blockSize, Order order) {
        TreeControl treeControl = TreeControl.initialize(addressable, blockSize, order);
        return new NodeFactory(addressable, treeControl);
    }

    public NodeFactory(Addressable addressable, TreeControl treeControl) {
        this.treeControl = treeControl;
        this.order = treeControl.getOrder();
        this.addressable = addressable;
    }

    Node read(Offset offset) {
        byte typeMarker = new ByteIO().read(addressable, offset);
        NodeType nodeType = NodeType.fromByte(typeMarker);
        return nodeType.read(this, addressable, offset, order);
    }

    private Node createNode(NodeRef parent, NodeType nodeType) {
        Offset from = treeControl.allocate(nodeType.storageSize(order));
        new ByteIO().write(addressable, from, nodeType.asByte());
        return nodeType.create(from, order, this, addressable, parent);
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
        addressable.seek(offset.asLong());
        addressable.writeInt(buf.length);
        addressable.write(buf);
        return offset;
    }

    public Value readValue(ValueRef valueRef) {
        return new Value(readBytes(valueRef.asLong()));
    }

    public Key readKey(KeyRef keyRef) {
        return new Key(readBytes(keyRef.asLong()));
    }

    byte[] readBytes(long position) {
        addressable.seek(position);
        byte[] bytes = new byte[addressable.readInt()];
        addressable.read(bytes);
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
        return (LeafNode) createNode(NodeRef.NULL, NodeType.leafNode);
    }

    InnerNode newInnerNode(NodeRef parent) {
        return (InnerNode) createNode(parent, NodeType.innerNode);
    }

    public TreeControl getTreeControl() {
        return treeControl;
    }

}
