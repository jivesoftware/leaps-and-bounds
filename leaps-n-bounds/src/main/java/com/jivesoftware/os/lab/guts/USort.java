package com.jivesoftware.os.lab.guts;

/**
 *
 */
public class USort {

    public static void mirrorSort(long[] _sort, Object[] _keys) {
        sortLO(_sort, _keys, 0, _sort.length);
    }

    private static void sortLO(long[] x, Object[] keys, int off, int len) {
        // Insertion sort on smallest arrays
        if (len < 7) {
            for (int i = off; i < len + off; i++) {
                for (int j = i; j > off && x[j - 1] > x[j]; j--) {
                    swapLO(x, keys, j, j - 1);
                }
            }
            return;
        }

        // Choose a partition element, v
        int m = off + (len >> 1);       // Small arrays, middle element
        if (len > 7) {
            int l = off;
            int n = off + len - 1;
            if (len > 40) {        // Big arrays, pseudomedian of 9
                int s = len / 8;
                l = med3L(x, l, l + s, l + 2 * s);
                m = med3L(x, m - s, m, m + s);
                n = med3L(x, n - 2 * s, n - s, n);
            }
            m = med3L(x, l, m, n); // Mid-size, med of 3
        }
        double v = x[m];

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = off, b = a, c = off + len - 1, d = c;
        while (true) {
            while (b <= c && x[b] <= v) {
                if (x[b] == v) {
                    swapLO(x, keys, a++, b);
                }
                b++;
            }
            while (c >= b && x[c] >= v) {
                if (x[c] == v) {
                    swapLO(x, keys, c, d--);
                }
                c--;
            }
            if (b > c) {
                break;
            }
            swapLO(x, keys, b++, c--);
        }

        // Swap partition elements back to middle
        int s, n = off + len;
        s = Math.min(a - off, b - a);
        vecswapLO(x, keys, off, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vecswapLO(x, keys, b, n - s, s);

        // Recursively sort non-partition-elements
        if ((s = b - a) > 1) {
            sortLO(x, keys, off, s);
        }
        if ((s = d - c) > 1) {
            sortLO(x, keys, n - s, s);
        }
    }

    /**
     * Swaps x[a] with x[b].
     */
    private static void swapLO(long x[], Object[] keys, int a, int b) {
        long t = x[a];
        x[a] = x[b];
        x[b] = t;

        Object l = keys[a];
        keys[a] = keys[b];
        keys[b] = l;

    }

    /**
     * Swaps x[a .. (a+n-1)] with x[b .. (b+n-1)].
     */
    private static void vecswapLO(long x[], Object[] keys, int a, int b, int n) {
        for (int i = 0; i < n; i++, a++, b++) {
            swapLO(x, keys, a, b);
        }
    }

    private static void sortDO(double[] x, Object[] keys, int off, int len) {
        // Insertion sort on smallest arrays
        if (len < 7) {
            for (int i = off; i < len + off; i++) {
                for (int j = i; j > off && x[j - 1] > x[j]; j--) {
                    swapDO(x, keys, j, j - 1);
                }
            }
            return;
        }

        // Choose a partition element, v
        int m = off + (len >> 1);       // Small arrays, middle element
        if (len > 7) {
            int l = off;
            int n = off + len - 1;
            if (len > 40) {        // Big arrays, pseudomedian of 9
                int s = len / 8;
                l = med3D(x, l, l + s, l + 2 * s);
                m = med3D(x, m - s, m, m + s);
                n = med3D(x, n - 2 * s, n - s, n);
            }
            m = med3D(x, l, m, n); // Mid-size, med of 3
        }
        double v = x[m];

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = off, b = a, c = off + len - 1, d = c;
        while (true) {
            while (b <= c && x[b] <= v) {
                if (x[b] == v) {
                    swapDO(x, keys, a++, b);
                }
                b++;
            }
            while (c >= b && x[c] >= v) {
                if (x[c] == v) {
                    swapDO(x, keys, c, d--);
                }
                c--;
            }
            if (b > c) {
                break;
            }
            swapDO(x, keys, b++, c--);
        }

        // Swap partition elements back to middle
        int s, n = off + len;
        s = Math.min(a - off, b - a);
        vecswapDO(x, keys, off, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vecswapDO(x, keys, b, n - s, s);

        // Recursively sort non-partition-elements
        if ((s = b - a) > 1) {
            sortDO(x, keys, off, s);
        }
        if ((s = d - c) > 1) {
            sortDO(x, keys, n - s, s);
        }
    }

    /**
     * Swaps x[a] with x[b].
     */
    private static void swapDO(double x[], Object[] keys, int a, int b) {
        double t = x[a];
        x[a] = x[b];
        x[b] = t;

        Object l = keys[a];
        keys[a] = keys[b];
        keys[b] = l;

    }

    /**
     * Swaps x[a .. (a+n-1)] with x[b .. (b+n-1)].
     */
    private static void vecswapDO(double x[], Object[] keys, int a, int b, int n) {
        for (int i = 0; i < n; i++, a++, b++) {
            swapDO(x, keys, a, b);
        }
    }

    private static void sortDD(double[] x, double[] keys, int off, int len) {
        // Insertion sort on smallest arrays
        if (len < 7) {
            for (int i = off; i < len + off; i++) {
                for (int j = i; j > off && x[j - 1] > x[j]; j--) {
                    swapDD(x, keys, j, j - 1);
                }
            }
            return;
        }

        // Choose a partition element, v
        int m = off + (len >> 1);       // Small arrays, middle element
        if (len > 7) {
            int l = off;
            int n = off + len - 1;
            if (len > 40) {        // Big arrays, pseudomedian of 9
                int s = len / 8;
                l = med3D(x, l, l + s, l + 2 * s);
                m = med3D(x, m - s, m, m + s);
                n = med3D(x, n - 2 * s, n - s, n);
            }
            m = med3D(x, l, m, n); // Mid-size, med of 3
        }
        double v = x[m];

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = off, b = a, c = off + len - 1, d = c;
        while (true) {
            while (b <= c && x[b] <= v) {
                if (x[b] == v) {
                    swapDD(x, keys, a++, b);
                }
                b++;
            }
            while (c >= b && x[c] >= v) {
                if (x[c] == v) {
                    swapDD(x, keys, c, d--);
                }
                c--;
            }
            if (b > c) {
                break;
            }
            swapDD(x, keys, b++, c--);
        }

        // Swap partition elements back to middle
        int s, n = off + len;
        s = Math.min(a - off, b - a);
        vecswapDD(x, keys, off, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vecswapDD(x, keys, b, n - s, s);

        // Recursively sort non-partition-elements
        if ((s = b - a) > 1) {
            sortDD(x, keys, off, s);
        }
        if ((s = d - c) > 1) {
            sortDD(x, keys, n - s, s);
        }
    }

    /**
     * Swaps x[a] with x[b].
     */
    private static void swapDD(double x[], double[] keys, int a, int b) {
        double t = x[a];
        x[a] = x[b];
        x[b] = t;

        double l = keys[a];
        keys[a] = keys[b];
        keys[b] = l;

    }

    /**
     * Swaps x[a .. (a+n-1)] with x[b .. (b+n-1)].
     */
    private static void vecswapDD(double x[], double[] keys, int a, int b, int n) {
        for (int i = 0; i < n; i++, a++, b++) {
            swapDD(x, keys, a, b);
        }
    }

    private static void sortLL(long[] x, long[] keys, int off, int len) {
        // Insertion sort on smallest arrays
        if (len < 7) {
            for (int i = off; i < len + off; i++) {
                for (int j = i; j > off && x[j - 1] > x[j]; j--) {
                    swapLL(x, keys, j, j - 1);
                }
            }
            return;
        }

        // Choose a partition element, v
        int m = off + (len >> 1);       // Small arrays, middle element
        if (len > 7) {
            int l = off;
            int n = off + len - 1;
            if (len > 40) {        // Big arrays, pseudomedian of 9
                int s = len / 8;
                l = med3L(x, l, l + s, l + 2 * s);
                m = med3L(x, m - s, m, m + s);
                n = med3L(x, n - 2 * s, n - s, n);
            }
            m = med3L(x, l, m, n); // Mid-size, med of 3
        }
        double v = x[m];

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = off, b = a, c = off + len - 1, d = c;
        while (true) {
            while (b <= c && x[b] <= v) {
                if (x[b] == v) {
                    swapLL(x, keys, a++, b);
                }
                b++;
            }
            while (c >= b && x[c] >= v) {
                if (x[c] == v) {
                    swapLL(x, keys, c, d--);
                }
                c--;
            }
            if (b > c) {
                break;
            }
            swapLL(x, keys, b++, c--);
        }

        // Swap partition elements back to middle
        int s, n = off + len;
        s = Math.min(a - off, b - a);
        vecswapLL(x, keys, off, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vecswapLL(x, keys, b, n - s, s);

        // Recursively sort non-partition-elements
        if ((s = b - a) > 1) {
            sortLL(x, keys, off, s);
        }
        if ((s = d - c) > 1) {
            sortLL(x, keys, n - s, s);
        }
    }

    /**
     * Swaps x[a] with x[b].
     */
    private static void swapLL(long x[], long[] keys, int a, int b) {
        long t = x[a];
        x[a] = x[b];
        x[b] = t;

        long l = keys[a];
        keys[a] = keys[b];
        keys[b] = l;

    }

    /**
     * Swaps x[a .. (a+n-1)] with x[b .. (b+n-1)].
     */
    private static void vecswapLL(long x[], long[] keys, int a, int b, int n) {
        for (int i = 0; i < n; i++, a++, b++) {
            swapLL(x, keys, a, b);
        }
    }

    private static void sortLD(long[] x, double[] keys, int off, int len) {
        // Insertion sort on smallest arrays
        if (len < 7) {
            for (int i = off; i < len + off; i++) {
                for (int j = i; j > off && x[j - 1] > x[j]; j--) {
                    swapLD(x, keys, j, j - 1);
                }
            }
            return;
        }

        // Choose a partition element, v
        int m = off + (len >> 1);       // Small arrays, middle element
        if (len > 7) {
            int l = off;
            int n = off + len - 1;
            if (len > 40) {        // Big arrays, pseudomedian of 9
                int s = len / 8;
                l = med3L(x, l, l + s, l + 2 * s);
                m = med3L(x, m - s, m, m + s);
                n = med3L(x, n - 2 * s, n - s, n);
            }
            m = med3L(x, l, m, n); // Mid-size, med of 3
        }
        double v = x[m];

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = off, b = a, c = off + len - 1, d = c;
        while (true) {
            while (b <= c && x[b] <= v) {
                if (x[b] == v) {
                    swapLD(x, keys, a++, b);
                }
                b++;
            }
            while (c >= b && x[c] >= v) {
                if (x[c] == v) {
                    swapLD(x, keys, c, d--);
                }
                c--;
            }
            if (b > c) {
                break;
            }
            swapLD(x, keys, b++, c--);
        }

        // Swap partition elements back to middle
        int s, n = off + len;
        s = Math.min(a - off, b - a);
        vecswapLD(x, keys, off, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vecswapLD(x, keys, b, n - s, s);

        // Recursively sort non-partition-elements
        if ((s = b - a) > 1) {
            sortLD(x, keys, off, s);
        }
        if ((s = d - c) > 1) {
            sortLD(x, keys, n - s, s);
        }
    }

    /**
     * Swaps x[a] with x[b].
     */
    private static void swapLD(long x[], double[] keys, int a, int b) {
        long t = x[a];
        x[a] = x[b];
        x[b] = t;

        double l = keys[a];
        keys[a] = keys[b];
        keys[b] = l;

    }

    /**
     * Swaps x[a .. (a+n-1)] with x[b .. (b+n-1)].
     */
    private static void vecswapLD(long x[], double[] keys, int a, int b, int n) {
        for (int i = 0; i < n; i++, a++, b++) {
            swapLD(x, keys, a, b);
        }
    }

    /**
     * Returns the index of the median of the three indexed doubles.
     */
    private static int med3L(long x[], int a, int b, int c) {
        return (x[a] < x[b] ? (x[b] < x[c] ? b : x[a] < x[c] ? c : a) : (x[b] > x[c] ? b : x[a] > x[c] ? c : a));
    }

    private static void sortDL(double[] x, long[] keys, int off, int len) {
        // Insertion sort on smallest arrays
        if (len < 7) {
            for (int i = off; i < len + off; i++) {
                for (int j = i; j > off && x[j - 1] > x[j]; j--) {
                    swapDL(x, keys, j, j - 1);
                }
            }
            return;
        }

        // Choose a partition element, v
        int m = off + (len >> 1);       // Small arrays, middle element
        if (len > 7) {
            int l = off;
            int n = off + len - 1;
            if (len > 40) {        // Big arrays, pseudomedian of 9
                int s = len / 8;
                l = med3D(x, l, l + s, l + 2 * s);
                m = med3D(x, m - s, m, m + s);
                n = med3D(x, n - 2 * s, n - s, n);
            }
            m = med3D(x, l, m, n); // Mid-size, med of 3
        }
        double v = x[m];

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = off, b = a, c = off + len - 1, d = c;
        while (true) {
            while (b <= c && x[b] <= v) {
                if (x[b] == v) {
                    swapDL(x, keys, a++, b);
                }
                b++;
            }
            while (c >= b && x[c] >= v) {
                if (x[c] == v) {
                    swapDL(x, keys, c, d--);
                }
                c--;
            }
            if (b > c) {
                break;
            }
            swapDL(x, keys, b++, c--);
        }

        // Swap partition elements back to middle
        int s, n = off + len;
        s = Math.min(a - off, b - a);
        vecswapDL(x, keys, off, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vecswapDL(x, keys, b, n - s, s);

        // Recursively sort non-partition-elements
        if ((s = b - a) > 1) {
            sortDL(x, keys, off, s);
        }
        if ((s = d - c) > 1) {
            sortDL(x, keys, n - s, s);
        }
    }

    /**
     * Swaps x[a] with x[b].
     */
    private static void swapDL(double x[], long[] keys, int a, int b) {
        double t = x[a];
        x[a] = x[b];
        x[b] = t;

        long l = keys[a];
        keys[a] = keys[b];
        keys[b] = l;

    }

    /**
     * Swaps x[a .. (a+n-1)] with x[b .. (b+n-1)].
     */
    private static void vecswapDL(double x[], long[] keys, int a, int b, int n) {
        for (int i = 0; i < n; i++, a++, b++) {
            swapDL(x, keys, a, b);
        }
    }

    /**
     * Returns the index of the median of the three indexed doubles.
     */
    private static int med3D(double x[], int a, int b, int c) {
        return (x[a] < x[b] ? (x[b] < x[c] ? b : x[a] < x[c] ? c : a) : (x[b] > x[c] ? b : x[a] > x[c] ? c : a));
    }
}
