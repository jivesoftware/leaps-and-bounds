package com.jivesoftware.os.lab.guts;

/**
 *
 * @author jonathan.colt
 */
public class TieredCompaction {

    private TieredCompaction() {
    }

    public static MergeRange getMergeRange(int minimumRun,
        boolean[] mergingCopy,
        long[] indexCounts,
        long[] indexSizes,
        long[] generations,
        byte[][] minKeys,
        byte[][] maxKeys) {

        //return crazySause(minimumRun, mergingCopy, indexCounts, generations);
        //return closetToIdeaLog(minimumRun, mergingCopy, indexCounts, generations); // 117sec
        //return smallestDelta(minimumRun, mergingCopy, indexCounts, generations); //87sec
        //return randomSause(minimumRun, mergingCopy, indexCounts, generations); // 80sec
        return hbaseSause(minimumRun, mergingCopy, indexCounts, indexSizes, generations);
    }

    /*

    http://www.ngdata.com/visualizing-hbase-flushes-and-compactions/
    
    The algorithm is basically as follows:

    Run over the set of all store files, from oldest to youngest

    If there are more than 3 (hbase.hstore.compactionThreshold) store
    files left and the current store file is 20% larger then the sum of
    all younger store files, and it is larger than the memstore flush size,
    then we go on to the next, younger, store file and repeat step 2.

    Once one of the conditions in step two is not valid anymore, the store
    files from the current one to the youngest one are the ones that will
    be merged together. If there are less than the compactionThreshold,
    no merge will be performed. There is also a limit which prevents more
    than 10 (hbase.hstore.compaction.max) store files to be merged in one compaction.

     */
    public static MergeRange hbaseSause(int minimumRun, boolean[] mergingCopy, long[] indexCounts, long[] indexSizes, long[] generations) {

        int maxMergedAtOnce = 10;

        for (int i = mergingCopy.length - 1; i > -1; i--) {

            if (mergingCopy[i]) {
                return null;
            }
            long oldSum = indexSizes[i];
            long g = generations[i];

            long youngSum = 0;
            int l = 2;
            int j = i - 1;
            for (; j > -1 && l < maxMergedAtOnce; j--, l++) {
                if (mergingCopy[j]) {
                    return null;
                }
                youngSum += indexSizes[j];
                g = Math.max(g, generations[j]);

                if ((l >= minimumRun || j == 0) && youngSum > (oldSum * 1.20d)) {
                    return new MergeRange(g, j, l, null, null);
                }
            }
        }
        return null;
    }

    public static MergeRange crazySause(int minimumRun, boolean[] mergingCopy, long[] indexCounts, long[] generations) {
        int start = -1;
        int length = 0;
        long g = -1;
        boolean encounteredMerge = false;
        for (int i = 1; i < mergingCopy.length; i++) {
            if (mergingCopy[i]) {
                encounteredMerge = true;
            }

            long headSum = 0;
            for (int j = 0; j < i; j++) {
                headSum += indexCounts[j];
            }

            if (headSum >= indexCounts[i]) {
                if (encounteredMerge) {
                    start = -1;
                    length = 0;
                    break;
                } else {
                    start = 0;
                    length = 1 + i;
                    g = generations[i];
                }
            }
        }
        if (start < 0 && mergingCopy.length > 1 && !encounteredMerge) {
            start = 0;
            length = mergingCopy.length;
        }

        return (start == -1) ? null : new MergeRange(g, start, length, null, null);
    }

    public static MergeRange randomSause(int minimumRun, boolean[] mergingCopy, long[] indexCounts, long[] generations) {

        double bestScore = Double.MAX_VALUE;
        long generation = 0;
        int start = -1;
        int length = 0;

        int smallestRun = minimumRun;
        int largestRun = minimumRun;

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

        return (start == -1) ? null : new MergeRange(generation, start, length, null, null);
    }

    public static MergeRange closetToIdeaLog(int minimumRun, boolean[] mergingCopy, long[] indexCounts, long[] generations) {

        double bestScore = Double.MAX_VALUE;
        long generation = 0;
        int start = -1;
        int length = 0;

        int smallestRun = minimumRun;
        int largestRun = minimumRun;

        int count = mergingCopy.length;
        for (int i = 0; i < count; i++) {
            long g = generations[i];
            long sum = 0;
            int l = 0;
            int j = i;
            int stop = i + (largestRun + 1);
            while (j < count && j < stop) {
                if (mergingCopy[j]) {
                    break;
                }
                sum += indexCounts[j];

                l++;
                j++;

                if (l >= smallestRun) {
                    //System.out.println(log(4000, 2) + " " + Math.pow(2, 12));
                    //System.out.println(log(sum, 2) + " " + Math.ceil(log(sum, 2)) + " " + Math.pow(2, Math.ceil(log(sum, 2))) + " " + sum);
                    double score = Math.pow(2, Math.ceil(log(sum, 2))) - sum;
                    //System.out.println("Score:" + score);
                    if (score < bestScore) {
                        start = i;
                        length = l;
                        bestScore = score;
                        generation = g;
                    }
                    j++;
                }
            }
        }

        return (start == -1) ? null : new MergeRange(generation, start, length, null, null);
    }

    private static double log(long x, int base) {
        return (Math.log(x) / Math.log(base));
    }

    private static final MergeRange smallestDelta(int minimumRun, boolean[] mergingCopy, long[] indexCounts, long[] generations) {
        double bestScore = Double.MAX_VALUE;
        long generation = 0;
        int start = -1;
        int length = 0;

        int smallestRun = minimumRun;
        int largestRun = minimumRun;

        int count = mergingCopy.length;
        for (int i = 0; i < count; i++) {
            long g = generations[i];
            long last = -1;
            long sum = 0;
            int l = 0;
            int j = i;
            int stop = i + (largestRun + 1);
            while (j < count && j < stop) {
                if (mergingCopy[j] || generations[j] > g) {
                    break;
                }
                if (last != -1) {
                    sum += Math.abs((indexCounts[j] * i) - last);
                }
                last = indexCounts[j];
                l++;
                j++;

                if (l >= smallestRun) {
                    double score = sum * g;
                    if (score < bestScore) {
                        start = i;
                        length = l;
                        bestScore = score;
                        generation = g;
                    }
                    j++;
                }
            }
        }

        return (start == -1) ? null : new MergeRange(generation, start, length, null, null);
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
