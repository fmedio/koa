package net.rhapso.koa.mapreduce;

import java.text.DecimalFormat;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsoleProgressReporter implements ProgressReporter {
    private static final DecimalFormat FORMAT = new DecimalFormat("###.#");

    private Map<MapTaskId, Double> mapProgress;
    private Map<ReduceTaskId, Double> reduceProgress;

    public ConsoleProgressReporter() {
        mapProgress = new TreeMap<MapTaskId, Double>();
        reduceProgress = new TreeMap<ReduceTaskId, Double>();
    }

    @Override
    public void mapProgress(MapTaskId taskId, long totalWorkUnits, long workUnitsAccomplishedSoFar) {
        double ratio = (double) workUnitsAccomplishedSoFar / (double) totalWorkUnits;
        mapProgress.put(taskId, ratio);
    }

    @Override
    public void reduceProgress(ReduceTaskId taskId, long totalWorkUnits, long workUnitsAccomplishedSoFar) {
        double ratio = (double) workUnitsAccomplishedSoFar / (double) totalWorkUnits;
        reduceProgress.put(taskId, ratio);
    }

    public void startReporting() {
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                System.out.println("----Map tasks: ");
                for (MapTaskId mapTaskId : mapProgress.keySet()) {
                    System.out.println(mapTaskId.toString() + ": " + format(mapProgress.get(mapTaskId)));
                }
                System.out.println("----Reduce tasks: ");
                for (ReduceTaskId reduceTaskId : reduceProgress.keySet()) {
                    System.out.println(reduceTaskId.toString() + ": " + format(reduceProgress.get(reduceTaskId)));
                }
                System.out.println();
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    private String format(Double completion) {
        return FORMAT.format(completion * 100d) + "%";
    }
}
