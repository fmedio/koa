package net.rhapso.koa.storage.block;

import net.rhapso.koa.storage.Addressable;

public interface CacheProvider {
    public void flush();

    public Block obtainBlock(Addressable addressable, BlockId blockId);
}
