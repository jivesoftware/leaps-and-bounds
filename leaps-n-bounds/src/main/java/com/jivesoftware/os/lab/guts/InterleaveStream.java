package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.guts.api.Scanner;
import com.jivesoftware.os.lab.guts.api.StreamRawEntry;
import java.nio.ByteBuffer;
import java.util.PriorityQueue;

/**
 * @author jonathan.colt
 */
public class InterleaveStream implements StreamRawEntry, Scanner {

    private final Rawhide rawhide;
    private final PriorityQueue<Feed> feeds = new PriorityQueue<>();
    private Feed active;
    private Feed until;

    public InterleaveStream(ReadIndex[] indexs, byte[] from, byte[] to, Rawhide rawhide) throws Exception {
        this.rawhide = rawhide;
        boolean rowScan = from == null && to == null;
        for (int i = 0; i < indexs.length; i++) {
            Scanner scanner;
            if (rowScan) {
                scanner = indexs[i].rowScan();
            } else {
                scanner = indexs[i].rangeScan(from, to);
            }
            Feed feed = new Feed(i, scanner, rawhide);
            feed.feedNext();
            feeds.add(feed);
        }
    }

    @Override
    public void close() throws Exception {
        for (Feed feed : feeds) {
            feed.scanner.close();
        }
    }

    @Override
    public boolean stream(RawEntryStream stream) throws Exception {

        Next more = Next.more;
        while (more == Next.more) {
            more = next(stream);
        }
        return more != Next.stopped;
    }

    @Override
    public Next next(RawEntryStream stream) throws Exception {

        // 0.     3, 5, 7, 9
        // 1.     3, 4, 7, 10
        // 2.     3, 6, 8, 11
        if (active == null
            || until != null && compare(active, until) >= 0) {

            if (active != null) {
                feeds.add(active);
            }

            active = feeds.poll();
            if (active == null) {
                return Next.eos;
            }

            while (true) {
                Feed first = feeds.peek();
                if (first == null
                    || compare(first, active) != 0) {
                    until = first;
                    break;
                }

                feeds.poll();
                if (first.feedNext() != null) {
                    feeds.add(first);
                }
            }
        }

        if (active != null) {
            if (active.nextRawEntry != null) {
                if (!stream.stream(active.nextReadKeyFormatTransformer,
                    active.nextReadValueFormatTransformer,
                    active.nextRawEntry)) {
                    return Next.stopped;
                }
            }
            if (active.feedNext() == null) {
                active = null;
                until = null;
            }
            return Next.more;
        } else {
            return Next.eos;
        }
    }

    private int compare(Feed left, Feed right) {
        return rawhide.compareKey(left.nextReadKeyFormatTransformer, left.nextReadValueFormatTransformer, left.nextRawEntry,
            right.nextReadKeyFormatTransformer, right.nextReadValueFormatTransformer, right.nextRawEntry);
    }

    private static class Feed implements Comparable<Feed> {

        private final int index;
        private final Scanner scanner;
        private final Rawhide rawhide;

        private FormatTransformer nextReadKeyFormatTransformer;
        private FormatTransformer nextReadValueFormatTransformer;
        private ByteBuffer nextRawEntry;

        public Feed(int index, Scanner scanner, Rawhide rawhide) {
            this.index = index;
            this.scanner = scanner;
            this.rawhide = rawhide;
        }

        private ByteBuffer feedNext() throws Exception {
            Next hadNext = scanner.next((readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                nextRawEntry = rawEntry;
                nextReadKeyFormatTransformer = readKeyFormatTransformer;
                nextReadValueFormatTransformer = readValueFormatTransformer;
                return true;
            });
            if (hadNext != Next.more) {
                nextRawEntry = null;
            }
            return nextRawEntry;
        }

        @Override
        public int compareTo(Feed o) {
            int c = rawhide.compareKey(nextReadKeyFormatTransformer, nextReadValueFormatTransformer, nextRawEntry,
                o.nextReadKeyFormatTransformer, o.nextReadValueFormatTransformer, o.nextRawEntry);
            if (c == 0) {
                c = Integer.compare(index, o.index);
            }
            return c;
        }
    }

}
