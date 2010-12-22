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

package net.rhapso.koa;

import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.MemoryAddressable;
import net.rhapso.koa.storage.block.BlockAddressable;
import net.rhapso.koa.storage.block.BlockSize;
import net.rhapso.koa.storage.block.CacheProvider;
import net.rhapso.koa.storage.block.LRUCacheProvider;
import net.rhapso.koa.tree.StoreName;

public class MemoryAddressableFactory extends AddressableFactory {
    public MemoryAddressableFactory() {
        super(new LRUCacheProvider(1000, BlockSize.DEFAULT));
    }

    @Override
    protected Addressable createAddressable(StoreName storeName, CacheProvider cacheProvider) {
        Addressable addressable = new MemoryAddressable(getBlockSize().asInt() * 100);
        return new BlockAddressable(addressable, getBlockSize(), cacheProvider);
    }

    @Override
    public boolean physicallyExists(StoreName storeName) {
        return addressables.keySet().contains(storeName);
    }
}
