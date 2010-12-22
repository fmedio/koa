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

import net.rhapso.koa.storage.StorageProvider;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCacheProvider extends LinkedHashMap<CacheKey, Block> implements CacheProvider {
    private final int cachedBlocks;
    private final BlockSize blockSize;

    public LRUCacheProvider(int cachedBlocks, BlockSize blockSize) {
        this.cachedBlocks = cachedBlocks;
        this.blockSize = blockSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<CacheKey, Block> eldest) {
        if (size() > cachedBlocks) {
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

    private void flush(StorageProvider storageProvider, BlockId blockId, Block block) {
        if (block.isDirty()) {
            storageProvider.seek(blockId.asLong() * blockSize.asLong());
            storageProvider.write(block.bytes());
            block.markClean();
        }
    }


    @Override
    public Block obtainBlock(StorageProvider storageProvider, BlockId blockId) {
        Block block = get(new CacheKey(storageProvider, blockId));
        if (block == null) {
            long blockOffset = blockId.asLong() * blockSize.asLong();
            byte[] bytes = new byte[blockSize.asInt()];
            if (blockOffset >= storageProvider.length()) {
                storageProvider.seek(blockOffset);
                storageProvider.write(new byte[blockSize.asInt()]);
            } else {
                storageProvider.seek(blockOffset);
                storageProvider.read(bytes);
            }
            block = new Block(bytes, false);
        }
        put(new CacheKey(storageProvider, blockId), block);
        return block;
    }
}
