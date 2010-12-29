package net.rhapso.koa.bag;

import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.StorageSize;
import net.rhapso.koa.tree.Value;

public class MappedValue {
    private Addressable addressable;

    public MappedValue(Addressable addressable) {
        this.addressable = addressable;
    }

    public void write(MappedValueRef ref, Value value) {
        addressable.seek(ref.asLong());
        byte[] bytes = value.getBytes();
        addressable.writeLong(0l);
        addressable.writeInt(bytes.length);
        addressable.write(bytes);
    }

    public Value read(MappedValueRef ref) {
        addressable.seek(ref.asLong());
        addressable.readLong();
        byte[] bytes = new byte[addressable.readInt()];
        addressable.read(bytes);
        return new Value(bytes);
    }


    public MappedValueRef getNext(MappedValueRef ref) {
        addressable.seek(ref.asLong());
        return new MappedValueRef(addressable.readLong());
    }

    public void setNext(MappedValueRef ref, MappedValueRef next) {
        addressable.seek(ref.asLong());
        addressable.writeLong(next.asLong());
    }

    public StorageSize storageSize(Value value) {
        return new StorageSize(8 + 4 + value.getBytes().length);
    }
}
