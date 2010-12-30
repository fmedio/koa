package net.rhapso.koa.mapreduce;

import clutter.CachingIterator;
import clutter.MultiIteratorSource;
import clutter.SourceDeduplicator;
import net.rhapso.koa.StorageFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Executor<I extends Serializable, K extends Serializable & Comparable<K>, IV extends Serializable & Comparable<IV>, OV extends Serializable> {
    private StorageFactory storageFactory;

    public Executor(StorageFactory storageFactory) {
        this.storageFactory = storageFactory;
    }

    public void execute(Job<I, K, IV, OV> job, int mapTasks) {
        List<MapTask<I, K, IV>> tasks = new ArrayList<MapTask<I, K, IV>>(mapTasks);
        for (int i = 0; i < mapTasks; i++) {
            MapTask<I, K, IV> task = new MapTask<I, K, IV>(i, storageFactory, job.makeMapper());
            tasks.add(task);
        }

        CachingIterator<I> input = new CachingIterator<I>(job.getInput());
        int scheduledSoFar = 0;
        while (input.hasNext()) {
            I next = input.next();
            int taskId = scheduledSoFar++ % mapTasks;
            tasks.get(taskId).add(next);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
        for (MapTask<I, K, IV> task : tasks) {
            executorService.submit(task);
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Reducer<K, IV, OV> reducer = job.makeReducer();
        Output<K, OV> output = job.makeOutput();

        Iterator<K> keys = allKeys(tasks);
        while (keys.hasNext()) {
            K key = keys.next();
            Iterator<IV> values = allValues(key, tasks);
            reducer.reduce(key, values, output);
        }
    }

    private Iterator<IV> allValues(K key, List<MapTask<I, K, IV>> tasks) {
        List<Iterator<IV>> iterators = new ArrayList<Iterator<IV>>();
        for (MapTask<I, K, IV> task : tasks) {
            iterators.add(task.values(key));
        }
        MultiIteratorSource<IV> source = new MultiIteratorSource<IV>(iterators);
        return new CachingIterator<IV>(source);
    }

    private Iterator<K> allKeys(List<MapTask<I, K, IV>> tasks) {
        List<Iterator<K>> iterators = new ArrayList<Iterator<K>>(tasks.size());
        for (MapTask<I, K, IV> task : tasks) {
            iterators.add(task.keys());
        }

        MultiIteratorSource<K> allKeys = new MultiIteratorSource<K>(iterators);
        SourceDeduplicator<K> deduplicator = new SourceDeduplicator<K>(allKeys);
        return new CachingIterator<K>(deduplicator);
    }
}
