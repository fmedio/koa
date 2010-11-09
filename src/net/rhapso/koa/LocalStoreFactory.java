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

import com.camp4.text.skunk.PostingsIO;
import net.rhapso.koa.storage.Addressable;
import net.rhapso.koa.storage.BlockAddressable;
import net.rhapso.koa.storage.BlockSize;
import net.rhapso.koa.storage.FileAddressable;
import net.rhapso.koa.tree.LocalTree;
import net.rhapso.koa.tree.StoreName;
import net.rhapso.koa.tree.Tree;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class LocalStoreFactory implements StoreFactory {
    private Map<StoreName, Tree> trees;
    private Map<StoreName, PostingsIO> postings;
    private Map<StoreName, Addressable> anonymousAddressables;

    private File dataDir;

    public LocalStoreFactory(File dataDir) {
        this.dataDir = dataDir;
        trees = new HashMap<StoreName, Tree>();
        postings = new HashMap<StoreName, PostingsIO>();
        anonymousAddressables = new HashMap<StoreName, Addressable>();
    }

    @Override
    public Tree openTree(StoreName storeName) {
        if (trees.keySet().contains(storeName)) {
            return trees.get(storeName);
        }

        try {
            LocalTree localTree = LocalTree.openOrCreate(new File(dataDir, storeName.getName()));
            trees.put(storeName, localTree);
            return localTree;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public PostingsIO openPostings(StoreName storeName) {
        if (postings.keySet().contains(storeName)) {
            return postings.get(storeName);
        }

        try {
            File file = new File(dataDir, storeName.getName());
            PostingsIO postingsIO = null;
            if (file.exists()) {
                BlockAddressable blockAddressable = new BlockAddressable(new FileAddressable(file), new BlockSize(4096 * 16), 10000);
                postingsIO = new PostingsIO(blockAddressable);
                postings.put(storeName, postingsIO);
            } else {
                file.createNewFile();
                BlockSize blockSize = new BlockSize(4096 * 16);
                postingsIO = PostingsIO.initialize(new BlockAddressable(new FileAddressable(file), blockSize, 10000), blockSize);
                postings.put(storeName, postingsIO);
            }

            return postingsIO;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Addressable openAddressable(StoreName storeName) {
        if (anonymousAddressables.containsKey(storeName)) {
            return anonymousAddressables.get(storeName);
        }

        File file = new File(dataDir, storeName.getName());
        Addressable addressable = new BlockAddressable(new FileAddressable(file), new BlockSize(4096 * 16), 10000);
        anonymousAddressables.put(storeName, addressable);
        return addressable;
    }

    @Override
    public void flush() {
        for (Tree tree : trees.values()) {
            tree.flush();
        }

        for (PostingsIO postingsIO : postings.values()) {
            postingsIO.flush();
        }

        for (Addressable addressable : anonymousAddressables.values()) {
            addressable.flush();
        }
    }
}
