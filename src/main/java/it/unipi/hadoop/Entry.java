package it.unipi.hadoop;

public class Entry implements Comparable<Entry>{
    private final int priority;
    private final Point point;

    public Entry(int priority, Point point){
        this.priority = priority;
        this.point = point;
    }

    public int getPriority() {
        return priority;
    }

    public Point getPoint() {
        return point;
    }

    @Override
    public int compareTo(Entry that) {
        /* High priority means lower value for priority */
        return Integer.compare(that.priority, this.priority);
    }
}
