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

package net.rhapso.koa.storage.block;

import net.rhapso.koa.storage.Storage;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache extends LinkedHashMap<CacheKey, Block> implements Cache {
    private long cacheHits;
    private long cacheMisses;
    private long stores;

    private final int maxCachedBlocks;
    private final BlockSize blockSize;

    public LRUCache(int maxCachedBlocks, BlockSize blockSize) {
        this.maxCachedBlocks = maxCachedBlocks;
        this.blockSize = blockSize;
    }

    @Override
    public BlockSize getBlockSize() {
        return blockSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<CacheKey, Block> eldest) {
        if (size() > maxCachedBlocks) {
            CacheKey cacheKey = eldest.getKey();
            flush(cacheKey.getAddressable(), cacheKey.getBlockId(), eldest.getValue());
            return true;
        }
        return false;
    }

    @Override
    public void flush() {
        for (Map.Entry<CacheKey, Block> entry : entrySet()) {
            CacheKey cacheKey = entry.getKey();
            flush(cacheKey.getAddressable(), cacheKey.getBlockId(), entry.getValue());
        }
    }

    private void flush(Storage storage, BlockId blockId, Block block) {
        if (block.isDirty()) {
            stores++;
            storage.seek(blockId.asLong() * blockSize.asLong());
            storage.write(block.bytes());
            block.markClean();
        }
    }


    @Override
    public Block obtainBlock(Storage storage, BlockId blockId) {
        Block block = get(new CacheKey(storage, blockId));
        if (block == null) {
            long blockOffset = blockId.asLong() * blockSize.asLong();
            byte[] bytes = new byte[blockSize.asInt()];
            if (blockOffset >= storage.length()) {
                storage.seek(blockOffset);
                storage.write(new byte[blockSize.asInt()]);
            } else {
                storage.seek(blockOffset);
                storage.read(bytes);
            }
            block = new Block(bytes, false);
            put(new CacheKey(storage, blockId), block);
            cacheMisses++;
        } else {
            cacheHits++;
        }
        return block;
    }

    @Override
    public CacheStatistics resetStatistics() {
        CacheStatistics cacheStatistics = new CacheStatistics(cacheHits, cacheMisses, maxCachedBlocks, size(), stores);
        cacheHits = 0;
        cacheMisses = 0;
        stores = 0;
        return cacheStatistics;
    }
}
