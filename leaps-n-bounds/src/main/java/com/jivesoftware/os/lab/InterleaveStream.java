package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.api.NextRawEntry;
import com.jivesoftware.os.lab.api.RawEntryStream;
import com.jivesoftware.os.lab.io.api.UIO;
import java.util.PriorityQueue;

/**
 * @author jonathan.colt
 */
class InterleaveStream implements NextRawEntry {

    private final PriorityQueue<Feed> feeds = new PriorityQueue<>();
    private Feed active;
    private Feed until;

    /*private final NextRawEntry[] feeds;
    private final int[] nextRawKeyLength;
    private final byte[][] nextRawEntry;
    private final int[] nextOffset;
    private final int[] nextLength;*/
    public InterleaveStream(NextRawEntry[] nextRawEntries) throws Exception {
        for (int i = 0; i < nextRawEntries.length; i++) {
            Feed feed = new Feed(i, nextRawEntries[i]);
            feed.feedNext();
            feeds.add(feed);
        }
    }

    /*private int streamed = -1;
    private int until = -1;*/
    @Override
    public boolean next(RawEntryStream stream) throws Exception {

        // 0.     3, 5, 7, 9
        // 1.     3, 4, 7, 10
        // 2.     3, 6, 8, 11
        if (active == null
            || until != null && IndexUtil.compare(active.nextRawEntry, 4, active.nextRawKeyLength, until.nextRawEntry, 4, until.nextRawKeyLength) >= 0) {

            if (active != null) {
                feeds.add(active);
            }

            active = feeds.poll();
            if (active == null) {
                return false;
            }

            while (true) {
                Feed first = feeds.peek();
                if (first == null
                    || IndexUtil.compare(first.nextRawEntry, 4, first.nextRawKeyLength, active.nextRawEntry, 4, active.nextRawKeyLength) != 0) {
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
                stream.stream(active.nextRawEntry, active.nextOffset, active.nextLength);
            }
            if (active.feedNext() == null) {
                active = null;
                until = null;
            }
            return true;
        } else {
            return false;
        }
    }

    private static class Feed implements Comparable<Feed> {

        private final int index;
        private final NextRawEntry feed;

        private int nextRawKeyLength;
        private byte[] nextRawEntry;
        private int nextOffset;
        private int nextLength;

        public Feed(int index, NextRawEntry feed) {
            this.index = index;
            this.feed = feed;
        }

        private byte[] feedNext() throws Exception {
            boolean hadNext = feed.next((rawEntry, offset, length) -> {
                nextRawKeyLength = UIO.bytesInt(rawEntry, offset);
                nextRawEntry = rawEntry;
                nextOffset = offset;
                nextLength = length;
                return true;
            });
            if (!hadNext) {
                nextRawEntry = null;
            }
            return nextRawEntry;
        }

        @Override
        public int compareTo(Feed o) {
            int c = IndexUtil.compare(nextRawEntry, 4, nextRawKeyLength, o.nextRawEntry, 4, o.nextRawKeyLength);
            if (c == 0) {
                c = Integer.compare(index, o.index);
            }
            return c;
        }
    }

}