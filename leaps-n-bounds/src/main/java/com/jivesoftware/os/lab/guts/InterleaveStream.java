package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.StreamRawEntry;
import java.util.PriorityQueue;

/**
 * @author jonathan.colt
 */
class InterleaveStream implements StreamRawEntry, NextRawEntry {

    private final PriorityQueue<Feed> feeds = new PriorityQueue<>();
    private Feed active;
    private Feed until;

    public InterleaveStream(NextRawEntry[] nextRawEntries, Rawhide rawhide) throws Exception {
        for (int i = 0; i < nextRawEntries.length; i++) {
            Feed feed = new Feed(i, nextRawEntries[i], rawhide);
            feed.feedNext();
            feeds.add(feed);
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
                if (!stream.stream(active.nextRawEntry, active.nextOffset, active.nextLength)) {
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
        return IndexUtil.compare(left.nextRawEntry, left.nextRawKeyOffset, left.nextRawKeyLength,
            right.nextRawEntry, right.nextRawKeyOffset, right.nextRawKeyLength);
    }

    private static class Feed implements Comparable<Feed> {

        private final int index;
        private final NextRawEntry feed;
        private final Rawhide rawhide;

        private int nextRawKeyLength;
        private int nextRawKeyOffset;
        private byte[] nextRawEntry;
        private int nextOffset;
        private int nextLength;

        public Feed(int index, NextRawEntry feed, Rawhide rawhide) {
            this.index = index;
            this.feed = feed;
            this.rawhide = rawhide;
        }

        private byte[] feedNext() throws Exception {
            Next hadNext = feed.next((rawEntry, offset, length) -> {
                nextRawKeyLength = rawhide.keyLength(rawEntry, offset);
                nextRawKeyOffset = rawhide.keyOffset(rawEntry, offset);
                nextRawEntry = rawEntry;
                nextOffset = offset;
                nextLength = length;
                return true;
            });
            if (hadNext != Next.more) {
                nextRawEntry = null;
            }
            return nextRawEntry;
        }

        @Override
        public int compareTo(Feed o) {
            int c = IndexUtil.compare(nextRawEntry, nextRawKeyOffset, nextRawKeyLength, o.nextRawEntry, o.nextRawKeyOffset, o.nextRawKeyLength);
            if (c == 0) {
                c = Integer.compare(index, o.index);
            }
            return c;
        }
    }

}
