package net.rhapso.koa.storage.block;

import net.rhapso.koa.storage.Storage;

public interface Cache {
    public void flush();

    public Block obtainBlock(Storage storage, BlockId blockId);

    public BlockSize getBlockSize();

    public CacheStatistics resetStatistics();
}
