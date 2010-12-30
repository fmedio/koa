package net.rhapso.koa.mapreduce;

public abstract class TaskId {
    private int id;
    private int totalTasks;

    TaskId(int totalTasks, int id) {
        this.totalTasks = totalTasks;
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public int getTotalTasks() {
        return totalTasks;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "id=" + id +
                ", totalTasks=" + totalTasks +
                '}';
    }
}
