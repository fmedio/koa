package net.rhapso.koa.mapreduce;

public interface ProgressReporter {
    public void mapProgress(MapTaskId taskId, long totalWorkUnits, long workUnitsAccomplishedSoFar);

    public void reduceProgress(ReduceTaskId taskId, long totalWorkUnits, long workUnitsAccomplishedSoFar);
}
