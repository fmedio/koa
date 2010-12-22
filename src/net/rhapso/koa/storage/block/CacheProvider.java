package net.rhapso.koa.storage.block;

import net.rhapso.koa.storage.StorageProvider;

public interface CacheProvider {
    public void flush();

    public Block obtainBlock(StorageProvider storageProvider, BlockId blockId);
}
