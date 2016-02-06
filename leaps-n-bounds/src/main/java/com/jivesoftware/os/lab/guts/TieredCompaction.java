package com.jivesoftware.os.lab.guts;

/**
 *
 * @author jonathan.colt
 */
public class TieredCompaction {

    public static MergeRange getMergeRange(int minimumRun, boolean[] mergingCopy, long[] indexCounts, long[] generations, byte[][] minKeys, byte[][] maxKeys) {

        //return crazySause(minimumRun, mergingCopy, indexCounts, generations);
        //return closetToIdeaLog(minimumRun, mergingCopy, indexCounts, generations); // 117sec
        //return smallestDelta(minimumRun, mergingCopy, indexCounts, generations); //87sec
        return randomSause(minimumRun, mergingCopy, indexCounts, generations); // 80sec
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

        return (start == -1) ? null : new MergeRange(g, start, length);
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

        return (start == -1) ? null : new MergeRange(generation, start, length);
    }

    public static MergeRange closetToIdeaLog(int minimumRun, boolean[] mergingCopy, long[] indexCounts, long[] generations) {

        double bestScore = Double.MAX_VALUE;
        long generation = 0;
        int start = -1;
        int length = 0;

        int smallestRun = minimumRun; // TODO expose
        int largestRun = minimumRun; // TODO expose

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

        return (start == -1) ? null : new MergeRange(generation, start, length);
    }

    private static double log(long x, int base) {
        return (Math.log(x) / Math.log(base));
    }

    private static final MergeRange smallestDelta(int minimumRun, boolean[] mergingCopy, long[] indexCounts, long[] generations) {
        double bestScore = Double.MAX_VALUE;
        long generation = 0;
        int start = -1;
        int length = 0;

        int smallestRun = minimumRun; // TODO expose
        int largestRun = minimumRun; // TODO expose

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
