package net.rhapso.koa.mapreduce;

import clutter.CollectionSource;
import clutter.IntegrationTest;
import clutter.Source;
import junit.framework.TestCase;
import net.rhapso.koa.storage.MemoryStorageFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

@IntegrationTest
public class ExecutorTest extends TestCase {
    public void testExecute() throws Exception {
        TestJob job = new TestJob();
        Executor<String, String, Integer, Integer> executor = new Executor<String, String, Integer, Integer>(new MemoryStorageFactory());
        executor.execute(job, 2);
        Map<String, Integer> result = job.getCounts();
        assertEquals(4, result.size());
        assertEquals(3, (int) result.get("hello"));
        assertEquals(1, (int) result.get("world"));
        assertEquals(1, (int) result.get("panda"));
        assertEquals(1, (int) result.get("42"));
    }

    private static class TestJob implements Job<String, String, Integer, Integer> {
        private Map<String, Integer> counts;

        private TestJob() {
            counts = new HashMap<String, Integer>();
        }

        public Map<String, Integer> getCounts() {
            return counts;
        }

        @Override
        public Source<String> getInput() {
            return new CollectionSource<String>("hello world", "hello panda", "hello 42");
        }

        @Override
        public Mapper<String, String, Integer> makeMapper() {
            return new Mapper<String, String, Integer>() {
                @Override
                public void map(String input, Emitter<String, Integer> emitter) {
                    StringTokenizer tokenizer = new StringTokenizer(input, " ", false);
                    while (tokenizer.hasMoreTokens()) {
                        String token = tokenizer.nextToken();
                        emitter.emit(token, 1);
                    }
                }
            };
        }

        @Override
        public Reducer<String, Integer, Integer> makeReducer() {
            return new Reducer<String, Integer, Integer>() {
                @Override
                public void reduce(String key, Iterator<Integer> values, Output<String, Integer> output) {
                    int sum = 0;
                    while (values.hasNext()) {
                        sum += values.next();
                    }
                    output.output(key, sum);
                }
            };
        }

        @Override
        public Output<String, Integer> makeOutput() {
            return new Output<String, Integer>() {
                @Override
                public void output(String key, Integer value) {
                    counts.put(key, value);
                }
            };
        }
    }
}
