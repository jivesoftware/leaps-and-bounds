package com.jivesoftware.os.lab.guts;

import java.io.File;

/**
 *
 * @author jonathan.colt
 */
public class IndexRangeId implements Comparable<IndexRangeId> {

    final long start;
    final long end;
    final long generation;

    public IndexRangeId(long start, long end, long generation) {
        this.start = start;
        this.end = end;
        this.generation = generation;
    }

    public boolean intersects(IndexRangeId range) {
        return (start <= range.start && end >= range.start) || (start <= range.end && end >= range.end);
    }

    @Override
    public int compareTo(IndexRangeId o) {
        int c = Long.compare(start, o.start);
        if (c == 0) {
            c = Long.compare(o.end, end); // reversed
        }
        return c;
    }

    @Override
    public String toString() {
        return "(" + start + " - " + end + " - " + generation + ')';
    }

    public File toFile(File parent) {
        return new File(parent, start + "-" + end + "-" + generation);
    }

    public IndexRangeId join(IndexRangeId id, long generation) {
        return new IndexRangeId(Math.min(start, id.start), Math.max(end, id.end), generation);
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 41 * hash + (int) (this.start ^ (this.start >>> 32));
        hash = 41 * hash + (int) (this.end ^ (this.end >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final IndexRangeId other = (IndexRangeId) obj;
        if (this.start != other.start) {
            return false;
        }
        return this.end == other.end;
    }
}
