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
import net.rhapso.koa.storage.BlockSize;
import net.rhapso.koa.tree.Order;
import net.rhapso.koa.tree.StoreName;

import java.util.HashMap;
import java.util.Map;

public abstract class AddressableFactory {
    protected Map<StoreName, Addressable> addressables;
    protected Order order;
    protected BlockSize blockSize;

    public AddressableFactory() {
        order = new Order(10);
        addressables = new HashMap<StoreName, Addressable>();
        blockSize = new BlockSize(4096 * 16);
    }

    protected abstract Addressable createAddressable(StoreName storeName);

    public abstract boolean exists(StoreName storeName);

    public BlockSize getBlockSize() {
        return blockSize;
    }

    public Order getOrder() {
        return order;
    }

    public Addressable openAddressable(StoreName storeName) {
        if (addressables.containsKey(storeName)) {
            return addressables.get(storeName);
        }
        Addressable addressable = createAddressable(storeName);
        addressables.put(storeName, addressable);
        return addressable;
    }

    public void flush() {
        for (Addressable addressable : addressables.values()) {
            addressable.flush();
        }
    }
}
