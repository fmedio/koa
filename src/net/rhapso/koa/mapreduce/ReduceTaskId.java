package net.rhapso.koa.mapreduce;

public class ReduceTaskId extends TaskId implements Comparable<ReduceTaskId> {
    public ReduceTaskId(int id, int totalTasks) {
        super(totalTasks, id);
    }

    @Override
    public int compareTo(ReduceTaskId o) {
        return getId() - o.getId();
    }
}
