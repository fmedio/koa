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

public class CacheKey {
    private Storage storage;
    private BlockId blockId;

    public CacheKey(Storage storage, BlockId blockId) {
        this.storage = storage;
        this.blockId = blockId;
    }

    public Storage getAddressable() {
        return storage;
    }

    public BlockId getBlockId() {
        return blockId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CacheKey cacheKey = (CacheKey) o;

        if (storage != null ? storage != cacheKey.storage : cacheKey.storage != null)
            return false;
        if (blockId != null ? !blockId.equals(cacheKey.blockId) : cacheKey.blockId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = storage != null ? storage.hashCode() : 0;
        result = 31 * result + (blockId != null ? blockId.hashCode() : 0);
        return result;
    }
}
