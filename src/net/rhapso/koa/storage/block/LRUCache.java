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

package net.rhapso.koa.storage.block;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import net.rhapso.koa.storage.Storage;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

public class LRUCache implements Cache {
    private volatile long cacheHits;
    private volatile long cacheMisses;
    private volatile long stores;

    private final int maxCachedBlocks;
    private final BlockSize blockSize;
    private ConcurrentMap<CacheKey, FutureTask<Block>> blocks;

    public LRUCache(int maxCachedBlocks, BlockSize blockSize) {
        this.maxCachedBlocks = maxCachedBlocks;
        this.blockSize = blockSize;
        blocks = new ConcurrentLinkedHashMap.Builder<CacheKey, FutureTask<Block>>()
                .maximumWeightedCapacity(maxCachedBlocks)
                .listener(new Flusher())
                .build();
    }

    @Override
    public BlockSize getBlockSize() {
        return blockSize;
    }

    @Override
    public synchronized void flush() {
        for (Map.Entry<CacheKey, FutureTask<Block>> entry : blocks.entrySet()) {
            new Flusher().onEviction(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public Block obtainBlock(Storage storage, BlockId blockId) {
        try {
            return doObtainBlock(storage, blockId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Block doObtainBlock(Storage storage, BlockId blockId) throws InterruptedException, ExecutionException {
        final CacheKey key = new CacheKey(storage, blockId);
        FutureTask<Block> future = blocks.putIfAbsent(key, new FutureTask<Block>(new BlockReader(blockId, storage)));

        if (future == null) {
            future = blocks.get(key);
            cacheMisses++;
        } else {
            cacheHits++;
        }
        future.run();
        return future.get();
    }

    @Override
    public CacheStatistics resetStatistics() {
        CacheStatistics cacheStatistics = new CacheStatistics(cacheHits, cacheMisses, maxCachedBlocks, blocks.size(), stores);
        cacheHits = 0;
        cacheMisses = 0;
        stores = 0;
        return cacheStatistics;
    }

    private class Flusher implements EvictionListener<CacheKey, FutureTask<Block>> {
        @Override
        public void onEviction(CacheKey cacheKey, FutureTask<Block> future) {
            try {
                final Storage storage = cacheKey.getStorage();
                final Block block = future.get();

                if (block.isDirty()) {
                    stores++;
                    storage.seek(cacheKey.getBlockId().asLong() * blockSize.asLong());
                    storage.write(block.bytes());
                    block.markClean();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class BlockReader implements Callable<Block> {
        private BlockId blockId;
        private Storage storage;

        private BlockReader(BlockId blockId, Storage storage) {
            this.blockId = blockId;
            this.storage = storage;
        }

        @Override
        public Block call() throws Exception {
            long blockOffset = blockId.asLong() * blockSize.asLong();
            byte[] bytes = new byte[blockSize.asInt()];
            if (blockOffset >= storage.length()) {
                storage.seek(blockOffset);
                storage.write(new byte[blockSize.asInt()]);
            } else {
                storage.seek(blockOffset);
                storage.read(bytes);
            }
            return new Block(bytes, false);
        }
    }
}
