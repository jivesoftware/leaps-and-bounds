package com.jivesoftware.os.lab.guts;

/**
 *
 * @author jonathan.colt
 */
public class TieredCompaction {

    public static MergeRange getMergeRange(boolean[] mergingCopy, long[] indexCounts, long[] generations) {

        double bestScore = Double.MAX_VALUE;
        long generation = 0;
        int start = -1;
        int length = 0;

        int smallestRun = 4; // TODO expose
        int largestRun = 4; // TODO expose

        int count = mergingCopy.length;
        for (int i = 0; i < count; i++) {
            long g = generations[i];
            long sum = 0;
            int l = 0;
            int j = i;
            int stop = i + (largestRun + 1);
            while (j < count && j < stop) {
                if (mergingCopy[j] || generations[j] > g) {
                    break;
                }
                sum += indexCounts[j];
                l++;
                j++;

                if (l >= smallestRun) {
                    double score = (sum / (double) l);
                    if (bestScore >= score) {
                        start = i;
                        length = l;
                        bestScore = score;
                        generation = g;
                    }
                    j++;
                }
            }
        }

        return (start == -1) ? null : new MergeRange(generation, start, length);
    }

    public static String range(long[] counts, int offset, int l) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < offset; i++) {
            sb.append(counts[i]);
            sb.append(", ");
        }
        sb.append("[");
        for (int i = offset; i < offset + l; i++) {
            if (i > offset) {
                sb.append(", ");
            }
            sb.append(counts[i]);
        }
        sb.append("]");
        for (int i = offset + l; i < counts.length; i++) {
            sb.append(", ");
            sb.append(counts[i]);
        }
        sb.append(']');
        return sb.toString();
    }
}
