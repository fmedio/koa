package net.rhapso.koa.mapreduce;

public class MapTaskId extends TaskId implements Comparable<MapTaskId> {
    MapTaskId(int totalTasks, int id) {
        super(totalTasks, id);
    }

    @Override
    public int compareTo(MapTaskId o) {
        return getId() - o.getId();
    }
}
