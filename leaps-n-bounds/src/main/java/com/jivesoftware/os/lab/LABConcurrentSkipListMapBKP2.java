package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.guts.LABIndex;
import com.jivesoftware.os.lab.guts.allocators.LABConcurrentSkipListMemory;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.Scanner;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class LABConcurrentSkipListMapBKP2 implements LABIndex {

    private static final long BASE_HEADER = -3;
    private static final long SELF = -2;
    private static final long NIL = -1;

    private transient volatile HeadIndex head;

    private static final AtomicReferenceFieldUpdater<LABConcurrentSkipListMapBKP2, HeadIndex> headUpdater = AtomicReferenceFieldUpdater
        .newUpdater(LABConcurrentSkipListMapBKP2.class, HeadIndex.class, "head");

    final LABConcurrentSkipListMemory memory;

    private void initialize() {
        head = new HeadIndex(new Node(NIL, BASE_HEADER, null), null, null, 1);
    }

    private boolean casHead(HeadIndex cmp, HeadIndex val) {
        return headUpdater.compareAndSet(this, cmp, val);
    }

    static final class Node {

        final long key;

        private static final AtomicIntegerFieldUpdater<Node> refCountUpdater =
            AtomicIntegerFieldUpdater.newUpdater(Node.class, "refCount");

        volatile int refCount;

        private static final AtomicLongFieldUpdater<Node> valueUpdater =
            AtomicLongFieldUpdater.newUpdater(Node.class, "value");

        volatile long value;

        volatile Node next;

        private static final AtomicReferenceFieldUpdater<Node, Node> nextUpdater =
            AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");

        /**
         * Creates a new regular node.
         */
        Node(long key, long value, Node next) {
            this.key = key;
            this.value = value;
            this.next = next;
        }

        Node(Node next) {
            this.key = NIL;
            this.value = SELF;
            this.next = next;
        }

        long acquireValue() throws InterruptedException {
            long v = this.value;
            if (v != NIL) {
                int at = refCountUpdater.get(this);
                while (true) {
                    if (at >= 0 && refCountUpdater.compareAndSet(this, at, at + 1)) {
//                        System.out.println(Thread.currentThread() + " acquireValue @" + (at + 1) + " " + Joiner.on("\n").join(new RuntimeException()
//                            .getStackTrace()));
                        break;
                    } else {
                        at = refCountUpdater.get(this);
                        Thread.yield();
                        if (Thread.interrupted()) {
                            throw new InterruptedException();
                        }
                    }
                }
                return v;
            }
            return NIL;
        }

        void releaseValue() {
            int r = refCountUpdater.decrementAndGet(this);
//            System.out.println(Thread.currentThread() + " releaseValue @" + r);
        }

        boolean casValue(LABConcurrentSkipListMemory memory, long cmp, long val) throws Exception {
            while (!refCountUpdater.compareAndSet(this, 0, -1)) {
                //System.out.println(Thread.currentThread() + " acquiring all");
                Thread.yield();
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
            }
//            System.out.println(Thread.currentThread() + " acquired all");
            try {
                boolean cas = valueUpdater.compareAndSet(this, cmp, val);
                if (cas) {
                    memory.release(cmp);
                }
                return cas;
            } finally {
//                System.out.println(Thread.currentThread() + " release all");
                refCountUpdater.compareAndSet(this, -1, 0);
            }
        }

        boolean casNext(Node cmp, Node val) {
            return nextUpdater.compareAndSet(this, cmp, val);
        }

        boolean isMarker() {
            return value == SELF;
        }

        boolean isBaseHeader() {
            return value == BASE_HEADER;
        }

        boolean appendMarker(Node f) {
            return casNext(f, new Node(f));
        }

        void helpDelete(Node b, Node f) {
            /*
             * Rechecking links and then doing only one of the
             * help-out stages per call tends to minimize CAS
             * interference among helping threads.
             */
            if (f == next && this == b.next) {
                if (f == null || f.value != SELF) // not already marked
                {
                    casNext(f, new Node(f));
                } else {
                    b.casNext(this, f.next);
                }
            }
        }

    }

    static class Index {

        final Node node;
        final Index down;
        volatile Index right;

        private static final AtomicReferenceFieldUpdater<Index, Index> rightUpdater =
            AtomicReferenceFieldUpdater.newUpdater(Index.class, Index.class, "right");

        /**
         * Creates index node with given values.
         */
        Index(Node node, Index down, Index right) {
            this.node = node;
            this.down = down;
            this.right = right;
        }

        /**
         * compareAndSet right field
         */
        final boolean casRight(Index cmp, Index val) {
            return rightUpdater.compareAndSet(this, cmp, val);
        }

        /**
         * Returns true if the node this indexes has been deleted.
         * @return true if indexed node is known to be deleted
         */
        final boolean indexesDeletedNode() {
            return node.value == NIL;
        }

        /**
         * Tries to CAS newSucc as successor.  To minimize races with
         * unlink that may lose this index node, if the node being
         * indexed is known to be deleted, it doesn't try to link in.
         * @param succ the expected current successor
         * @param newSucc the new successor
         * @return true if successful
         */
        final boolean link(Index succ, Index newSucc) {
            Node n = node;
            newSucc.right = succ;
            return n.value != NIL && casRight(succ, newSucc);
        }

        /**
         * Tries to CAS right field to skip over apparent successor
         * succ.  Fails (forcing a retraversal by caller) if this node
         * is known to be deleted.
         * @param succ the expected current successor
         * @return true if successful
         */
        final boolean unlink(Index succ) {
            return node.value != NIL && casRight(succ, succ.right);
        }

    }

    /* ---------------- Head nodes -------------- */
    /**
     * Nodes heading each level keep track of their level.
     */
    static final class HeadIndex extends Index {

        final int level;

        HeadIndex(Node node, Index down, Index right, int level) {
            super(node, down, right);
            this.level = level;

            if (right == this) {
                System.out.println("Booo");
            }
        }
    }

    /* ---------------- Traversal -------------- */
    /**
     * Returns a base-level node with key strictly less than given key,
     * or the base-level header if there is no such node.  Also
     * unlinks indexes to deleted nodes found along the way.  Callers
     * rely on this side-effect of clearing indices to deleted nodes.
     * @param key the key
     * @return a predecessor of key
     */
    private Node findPredecessor(BolBuffer key) {
        if (key == null || key.length == -1) {
            throw new NullPointerException(); // don't postpone errors
        }
        for (;;) {
            for (Index q = head, r = q.right, d;;) {
                if (r != null) {
                    Node n = r.node;
                    long k = n.key;
                    if (n.value == NIL) {
                        if (!q.unlink(r)) {
                            break;           // restart
                        }
                        r = q.right;         // reread r
                        continue;
                    }
                    if (memory.compare(key.bytes, key.offset, key.length, k) > 0) {
                        q = r;
                        r = r.right;
                        continue;
                    }
                }
                if ((d = q.down) == null) {
                    return q.node;
                }
                q = d;
                r = d.right;
            }
        }
    }

    /**
     * Returns node holding key or null if no such, clearing out any
     * deleted nodes seen along the way.  Repeatedly traverses at
     * base-level looking for key starting at predecessor returned
     * from findPredecessor, processing base-level deletions as
     * encountered. Some callers rely on this side-effect of clearing
     * deleted nodes.
     *
     * Restarts occur, at traversal step centered on node n, if:
     *
     *   (1) After reading n's next field, n is no longer assumed
     *       predecessor b's current successor, which means that
     *       we don't have a consistent 3-node snapshot and so cannot
     *       unlink any subsequent deleted nodes encountered.
     *
     *   (2) n's value field is null, indicating n is deleted, in
     *       which case we help out an ongoing structural deletion
     *       before retrying.  Even though there are cases where such
     *       unlinking doesn't require restart, they aren't sorted out
     *       here because doing so would not usually outweigh cost of
     *       restarting.
     *
     *   (3) n is a marker or n's predecessor's value field is null,
     *       indicating (among other possibilities) that
     *       findPredecessor returned a deleted node. We can't unlink
     *       the node because we don't know its predecessor, so rely
     *       on another call to findPredecessor to notice and return
     *       some earlier predecessor, which it will do. This check is
     *       only strictly needed at beginning of loop, (and the
     *       b.value check isn't strictly needed at all) but is done
     *       each iteration to help avoid contention with other
     *       threads by callers that will fail to be able to change
     *       links, and so will retry anyway.
     *
     * The traversal loops in doPut, doRemove, and findNear all
     * include the same three kinds of checks. And specialized
     * versions appear in findFirst, and findLast and their
     * variants. They can't easily share code because each uses the
     * reads of fields held in locals occurring in the orders they
     * were performed.
     *
     * @param key the key
     * @return node holding key, or null if no such
     */
    private Node findNode(BolBuffer key) {
        if (key == null || key.length == -1) {
            throw new NullPointerException(); // don't postpone errors
        }
        outer:
        for (;;) {
            for (Node b = findPredecessor(key), n = b.next;;) {
                long v;
                int c;
                if (n == null) {
                    break outer;
                }
                Node f = n.next;
                if (n != b.next) // inconsistent read
                {
                    break;
                }
                if ((v = n.value) == NIL) {    // n is deleted
                    n.helpDelete(b, f);
                    break;
                }
                if (b.value == NIL || v == SELF) // b is deleted
                {
                    break;
                }
                if ((c = memory.compare(key.bytes, key.offset, key.length, n.key)) == 0) {
                    return n;
                }
                if (c < 0) {
                    break outer;
                }
                b = n;
                n = f;
            }
        }
        return null;
    }

    /**
     * Gets value for key. Almost the same as findNode, but returns
     * the found value (to avoid retries during re-reads)
     *
     * @param key the key
     * @return the value, or null if absent
     */
    private long doGet(BolBuffer key) {
        if (key == null || key.length == -1) {
            throw new NullPointerException();
        }
        outer:
        for (;;) {
            for (Node b = findPredecessor(key), n = b.next;;) {
                long v;
                int c;
                if (n == null) {
                    break outer;
                }
                Node f = n.next;
                if (n != b.next) // inconsistent read
                {
                    break;
                }
                if ((v = n.value) == NIL) {    // n is deleted
                    n.helpDelete(b, f);
                    break;
                }
                if (b.value == NIL || v == SELF) // b is deleted
                {
                    break;
                }
                if ((c = memory.compare(key.bytes, key.offset, key.length, n.key)) == 0) {
                    return v;
                }
                if (c < 0) {
                    break outer;
                }
                b = n;
                n = f;
            }
        }
        return -1;

    }

    /* ---------------- Finding and removing first element -------------- */
    /**
     * Specialized variant of findNode to get first valid node.
     * @return first node or null if empty
     */
    final Node findFirst() {
        for (Node b, n;;) {
            if ((n = (b = head.node).next) == null) {
                return null;
            }
            if (n.value != NIL) {
                return n;
            }
            n.helpDelete(b, n.next);
        }
    }


    /* ---------------- Finding and removing last element -------------- */
    /**
     * Specialized version of find to get last valid node.
     * @return last node or null if empty
     */
    final Node findLast() {
        /*
         * findPredecessor can't be used to traverse index level
         * because this doesn't use comparisons.  So traversals of
         * both levels are folded together.
         */
        Index q = head;
        for (;;) {
            Index d, r;
            if ((r = q.right) != null) {
                if (r.indexesDeletedNode()) {
                    q.unlink(r);
                    q = head; // restart
                } else {
                    q = r;
                }
            } else if ((d = q.down) != null) {
                q = d;
            } else {
                for (Node b = q.node, n = b.next;;) {
                    if (n == null) {
                        return b.isBaseHeader() ? null : b;
                    }
                    Node f = n.next;            // inconsistent read
                    if (n != b.next) {
                        break;
                    }
                    long v = n.value;
                    if (v == NIL) {                 // n is deleted
                        n.helpDelete(b, f);
                        break;
                    }
                    if (b.value == NIL || v == SELF) // b is deleted
                    {
                        break;
                    }
                    b = n;
                    n = f;
                }
                q = head; // restart
            }
        }
    }

    /* ---------------- Relational operations -------------- */
    // Control values OR'ed as arguments to findNear
    private static final int EQ = 1;
    private static final int LT = 2;
    private static final int GT = 0; // Actually checked as !LT

    /**
     * Utility for ceiling, floor, lower, higher methods.
     * @param key the key
     * @param rel the relation -- OR'ed combination of EQ, LT, GT
     * @return nearest node fitting relation, or null if no such
     */
    final Node findNear(BolBuffer key, int rel) {
        if (key == null || key.length == -1) {
            throw new NullPointerException();
        }
        for (;;) {
            for (Node b = findPredecessor(key), n = b.next;;) {
                long v;
                if (n == null) {
                    return ((rel & LT) == 0 || b.isBaseHeader()) ? null : b;
                }
                Node f = n.next;
                if (n != b.next) // inconsistent read
                {
                    break;
                }
                if ((v = n.value) == NIL) {      // n is deleted
                    n.helpDelete(b, f);
                    break;
                }
                if (b.value == NIL || v == SELF) // b is deleted
                {
                    break;
                }
                int c = memory.compare(key.bytes, key.offset, key.length, n.key);
                if ((c == 0 && (rel & EQ) != 0)
                    || (c < 0 && (rel & LT) == 0)) {
                    return n;
                }
                if (c <= 0 && (rel & LT) != 0) {
                    return b.isBaseHeader() ? null : b;
                }
                b = n;
                n = f;
            }
        }
    }

    public LABConcurrentSkipListMapBKP2(LABConcurrentSkipListMemory comparator) {
        this.memory = comparator;
        initialize();
    }

    @Override
    public BolBuffer get(BolBuffer keyBytes, BolBuffer valueBytes) throws Exception {
        long address = doGet(keyBytes);
        if (address == -1) {
            return null;
        } else {
            memory.acquireBytes(address, valueBytes);
            return valueBytes;
        }
    }

    public int size() {
        long count = 0;
        for (Node n = findFirst(); n != null; n = n.next) {
            if (n.value != NIL && n.value != SELF) {
                ++count;
            }
        }
        return (count >= Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) count;
    }

    @Override
    public boolean isEmpty() {
        return findFirst() == null;
    }

    @Override
    public void clear() {
        initialize();
    }

    @Override
    public void compute(BolBuffer keyBytes, BolBuffer valueBytes, Compute remappingFunction) throws Exception {
        if (keyBytes == null || keyBytes.length == -1 || remappingFunction == null) {
            throw new NullPointerException();
        }

        for (;;) {
            Node n;
            long v;
            BolBuffer r;
            if ((n = findNode(keyBytes)) == null) {
                if ((r = remappingFunction.apply(null)) == null) {
                    break;
                }
                if (doPut(keyBytes, r, true) == null) {
                    return;
                }
            } else if ((v = n.value) != NIL) {
                long va = n.acquireValue();
                if (va == NIL || va == SELF) {
                    n.releaseValue();
                } else {
                    long rid;
                    try {
                        memory.acquireBytes(va, valueBytes);

                        r = remappingFunction.apply(valueBytes);
                        rid = r == null ? NIL : memory.allocate(r);
                    } finally {
                        n.releaseValue();
                    }
                    if (n.casValue(memory, va, rid)) {
                        return;
                    } else if (rid != NIL) {
                        memory.release(rid);
                    }
                }
            }
        }
    }

    private byte[] doPut(BolBuffer keyBytes, BolBuffer valueBytes, boolean onlyIfAbsent) throws Exception {

        Node z;             // added node
        if (keyBytes == null || keyBytes.length == -1) {
            throw new NullPointerException();
        }

        long kid = NIL;
        long vid = NIL;

        outer:
        for (;;) {
            for (Node b = findPredecessor(keyBytes), n = b.next;;) {
                if (n != null) {
                    long v;
                    int c;
                    Node f = n.next;
                    if (n != b.next) // inconsistent read
                    {
                        break;
                    }
                    if ((v = n.value) == NIL) {   // n is deleted
                        n.helpDelete(b, f);
                        break;
                    }
                    if (b.value == NIL || v == SELF) // b is deleted
                    {
                        break;
                    }
                    if ((c = memory.compare(keyBytes.bytes, keyBytes.offset, keyBytes.length, n.key)) > 0) {
                        b = n;
                        n = f;
                        continue;
                    }
                    if (c == 0) {
                        if (onlyIfAbsent) {
                            if (kid != NIL) {
                                memory.release(kid);
                            }
                            if (vid != NIL) {
                                memory.release(vid);
                            }
                            return memory.bytes(v);
                        }
                        if (vid == NIL) {
                            vid = valueBytes == null ? NIL : memory.allocate(valueBytes);
                        }
                        if (n.casValue(memory, v, vid)) {
                            byte[] was = memory.bytes(v);
                            memory.release(v);
                            if (kid != NIL) {
                                memory.release(kid);
                            }
                            if (vid != NIL) {
                                memory.release(vid);
                            }
                            return was;
                        }
                        break; // restart if lost race to replace value
                    }
                    // else c < 0; fall through
                }

                if (kid == NIL) {
                    kid = memory.allocate(keyBytes);
                }
                if (vid == NIL) {
                    vid = valueBytes == null ? NIL : memory.allocate(valueBytes);
                }
                z = new Node(kid, vid, n);
                if (!b.casNext(n, z)) {
                    break;         // restart if lost race to append to b
                } else {
                    kid = NIL;
                    vid = NIL;
                }
                break outer;
            }
        }

        if (kid != NIL) {
            memory.release(kid);
        }

        if (vid != NIL) {
            memory.release(vid);
        }

        int rnd = new Random().nextInt(); // BARF
        if ((rnd & 0x80000001) == 0) { // test highest and lowest bits
            int level = 1, max;
            while (((rnd >>>= 1) & 1) != 0) {
                ++level;
            }
            Index idx = null;
            HeadIndex h = head;
            if (level <= (max = h.level)) {
                for (int i = 1; i <= level; ++i) {
                    idx = new Index(z, idx, null);
                }
            } else { // try to grow by one level
                level = max + 1; // hold in array and later pick the one to use
                @SuppressWarnings("unchecked")
                Index[] idxs =
                    new Index[level + 1];
                for (int i = 1; i <= level; ++i) {
                    idxs[i] = idx = new Index(z, idx, null);
                }
                for (;;) {
                    h = head;
                    int oldLevel = h.level;
                    if (level <= oldLevel) // lost race to add level
                    {
                        break;
                    }
                    HeadIndex newh = h;
                    Node oldbase = h.node;
                    for (int j = oldLevel + 1; j <= level; ++j) {
                        newh = new HeadIndex(oldbase, newh, idxs[j], j);
                    }
                    if (casHead(h, newh)) {
                        h = newh;
                        idx = idxs[level = oldLevel];
                        break;
                    }
                }
            }
            // find insertion points and splice in
            splice:
            for (int insertionLevel = level;;) {
                int j = h.level;
                for (Index q = h, r = q.right, t = idx;;) {
                    if (q == null || t == null) {
                        break splice;
                    }
                    if (r != null) {
                        Node n = r.node;
                        // compare before deletion check avoids needing recheck
                        int c = memory.compare(keyBytes.bytes, keyBytes.offset, keyBytes.length, n.key);
                        if (n.value == NIL) {
                            if (!q.unlink(r)) {
                                break;
                            }
                            r = q.right;
                            continue;
                        }
                        if (c > 0) {
                            q = r;
                            r = r.right;
                            continue;
                        }
                    }

                    if (j == insertionLevel) {
                        if (!q.link(r, t)) {
                            break; // restart
                        }
                        if (t.node.value == NIL) {
                            findNode(keyBytes);
                            break splice;
                        }
                        if (--insertionLevel == 0) {
                            break splice;
                        }
                    }

                    if (--j >= insertionLevel && j < level) {
                        t = t.down;
                    }
                    q = q.down;
                    r = q.right;
                }
            }
        }
        return null;

    }

    @Override
    public byte[] firstKey() throws InterruptedException {
        Node n = findFirst();
        if (n == null) {
            throw new NoSuchElementException();
        }
        return memory.bytes(n.key);
    }

    @Override
    public byte[] lastKey() throws InterruptedException {
        Node n = findLast();
        if (n == null) {
            throw new NoSuchElementException();
        }
        return memory.bytes(n.key);
    }

    public void freeAll() throws Exception {
        for (Node n = findFirst(); n != null; n = n.next) {
            if (n.value != NIL && n.value != SELF) {
                memory.release(n.key);
                memory.release(n.value);
            }
        }
        memory.freeAll();
    }

    class AllEntryStream implements EntryStream {

        /** the last node returned by next() */
        Node lastReturned;
        /** the next node to return from next(); */
        Node next;
        /** Cache of next value field to maintain weak consistency */
        long nextValue = NIL;

        /** Initializes ascending iterator for entire range. */
        AllEntryStream() throws InterruptedException {
            while ((next = findFirst()) != null) {
                long x = next.value;
                if (x != NIL && x != SELF) {
                    long va = next.acquireValue();
                    if (va == NIL || va == SELF) {
                        next.releaseValue();
                    } else {
                        nextValue = va;
                        break;
                    }
                }
            }
        }

        @Override
        public void close() {
            if (next != null) {
                next.releaseValue();
            }
        }

        @Override
        public final boolean hasNext() {
            return next != null;
        }

        public boolean next(RawEntryStream entryStream) throws Exception {
            byte[] v = memory.bytes(nextValue);
            advance();
            return entryStream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(v));
        }

        void advance() throws InterruptedException {
            if (next == null) {
                throw new NoSuchElementException();
            }
            lastReturned = next;
            lastReturned.releaseValue();
            while ((next = next.next) != null) {
                long x = next.value;
                if (x != NIL && x != SELF) {
                    long va = next.acquireValue();
                    if (va == NIL || va == SELF) {
                        next.releaseValue();
                    } else {
                        nextValue = va;
                        break;
                    }
                }
            }
        }

    }


    /* ---------------- View Classes -------------- */
    /**
     * Submaps returned by {@link LABConcurrentSkipListMap} submap operations
     * represent a subrange of mappings of their underlying
     * maps. Instances of this class support all methods of their
     * underlying maps, differing in that mappings outside their range are
     * ignored, and attempts to add mappings outside their ranges result
     * in {@link IllegalArgumentException}.  Instances of this class are
     * constructed only using the {@code subMap}, {@code headMap}, and
     * {@code tailMap} methods of their underlying maps.
     *
     * @serial include
     */
    static final class SubRangeEntryStream implements EntryStream {

        /** Underlying map */
        private final LABConcurrentSkipListMapBKP2 m;
        /** lower bound key, or null if from start */
        private final BolBuffer lo;
        /** upper bound key, or null if to end */
        private final BolBuffer hi;
        /** inclusion flag for lo */
        private final boolean loInclusive;
        /** inclusion flag for hi */
        private final boolean hiInclusive;

        /** the last node returned by next() */
        Node lastReturned;
        /** the next node to return from next(); */
        Node next;
        /** Cache of next value field to maintain weak consistency */
        long nextValue;

        /**
         * Creates a new submap, initializing all fields.
         */
        SubRangeEntryStream(LABConcurrentSkipListMapBKP2 map,
            byte[] fromKey, boolean fromInclusive,
            byte[] toKey, boolean toInclusive) throws InterruptedException, Exception {
            if (fromKey != null && toKey != null) {
                if (map.memory.compare(fromKey, 0, fromKey.length, toKey, 0, toKey.length) > 0) {
                    throw new IllegalArgumentException("inconsistent range");
                }
            }
            this.m = map;
            this.lo = fromKey == null ? null : new BolBuffer(fromKey);
            this.hi = toKey == null ? null : new BolBuffer(toKey);
            this.loInclusive = fromInclusive;
            this.hiInclusive = toInclusive;

            BolBuffer keyBolBuffer = new BolBuffer(); // Grr
            for (;;) {
                next = loNode();
                if (next == null) {
                    break;
                }
                long x = next.value;
                if (x != NIL && x != SELF) {
                    m.memory.acquireBytes(next.key, keyBolBuffer);
                    if (!inBounds(keyBolBuffer)) {
                        next = null;
                    } else {
                        long va = next.acquireValue();
                        if (va == NIL) {
                            next.releaseValue();
                        } else {
                            nextValue = va;
                        }
                    }
                    break;
                }
            }
        }

        @Override
        public void close() {
            if (next != null) {
                next.releaseValue();
            }
        }

        @Override
        public boolean next(RawEntryStream stream) throws Exception {
            Node n = next;
            byte[] v = m.memory.bytes(nextValue);
            advance(new BolBuffer()); // Grr
            return stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(v));
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        void advance(BolBuffer keyBolBuffer) throws InterruptedException {
            if (next == null) {
                throw new NoSuchElementException();
            }
            lastReturned = next;
            lastReturned.releaseValue();
            for (;;) {
                next = next.next;
                if (next == null) {
                    break;
                }
                long x = next.value;
                if (x != NIL && x != SELF) {
                    if (tooHigh(next.key)) {
                        next = null;
                    } else {
                        long va = next.acquireValue();
                        if (va == NIL || va == SELF) {
                            next.releaseValue();
                        } else {
                            nextValue = va;
                        }
                    }
                    break;
                }
            }
        }


        /* ----------------  Utilities -------------- */
        boolean tooLow(long key) {
            int c;

            if (lo != null) {
                return ((c = m.memory.compare(key, lo.bytes, lo.offset, lo.length)) < 0 || (c == 0 && !loInclusive));

            } else {
                return false;
            }

        }

        boolean tooHigh(long key) {
            int c;
            if (hi != null) {
                return ((c = m.memory.compare(key, hi.bytes, hi.offset, hi.length)) > 0 || (c == 0 && !hiInclusive));
            } else {
                return false;
            }

        }

        boolean inBounds(long key) {
            return !tooLow(key) && !tooHigh(key);
        }

        boolean tooLow(BolBuffer key) {
            int c;

            if (lo != null) {
                return ((c = m.memory.compare(key.bytes, key.offset, key.length, lo.bytes, lo.offset, lo.length)) < 0 || (c == 0 && !loInclusive));

            } else {
                return false;
            }

        }

        boolean tooHigh(BolBuffer key) {
            int c;
            if (hi != null) {
                return ((c = m.memory.compare(key.bytes, key.offset, key.length, hi.bytes, hi.offset, hi.length)) > 0 || (c == 0 && !hiInclusive));
            } else {
                return false;
            }

        }

        boolean inBounds(BolBuffer key) {
            return !tooLow(key) && !tooHigh(key);
        }

        /**
         * Returns true if node key is less than upper bound of range.
         */
        boolean isBeforeEnd(LABConcurrentSkipListMapBKP2.Node n) {
            if (n == null) {
                return false;
            }
            if (hi == null) {
                return true;
            }
            long k = n.key;
            if (k == NIL) // pass by markers and headers
            {
                return true;
            }
            int c = m.memory.compare(k, hi.bytes, hi.offset, hi.length);
            if (c > 0 || (c == 0 && !hiInclusive)) {
                return false;
            }
            return true;

        }

        /**
         * Returns lowest node. This node might not be in range, so
         * most usages need to check bounds.
         */
        LABConcurrentSkipListMapBKP2.Node loNode() {
            if (lo == null) {
                return m.findFirst();
            } else if (loInclusive) {
                return m.findNear(lo, GT | EQ);

            } else {
                return m.findNear(lo, GT);
            }
        }

        /**
         * Returns highest node. This node might not be in range, so
         * most usages need to check bounds.
         */
        LABConcurrentSkipListMapBKP2.Node hiNode() {
            if (hi == null) {
                return m.findLast();
            } else if (hiInclusive) {
                return m.findNear(hi, LT | EQ);

            } else {
                return m.findNear(hi, LT);

            }
        }

        public int size() {
            long count = 0;
            for (LABConcurrentSkipListMapBKP2.Node n = loNode();
                isBeforeEnd(n);
                n = n.next) {
                if (n.value != NIL && n.value != SELF) {
                    ++count;
                }
            }
            return count >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) count;
        }

        public boolean isEmpty() {
            return !isBeforeEnd(loNode());
        }

    }

    @Override
    public boolean contains(byte[] from, byte[] to) throws Exception {
        return rangeMap(from, to).hasNext();
    }

    private EntryStream rangeMap(byte[] from, byte[] to) throws Exception {
        if (from != null && to != null) {
            return new SubRangeEntryStream(this, from, true, to, false);
        } else if (from != null) {
            return new SubRangeEntryStream(this, from, true, null, false);
        } else if (to != null) {
            return new SubRangeEntryStream(this, null, false, to, false);
        } else {
            return new AllEntryStream();
        }
    }

    @Override
    public Scanner scanner(byte[] from, byte[] to) throws Exception {

        EntryStream entryStream = rangeMap(from, to);
        return new Scanner() {
            @Override
            public Scanner.Next next(RawEntryStream stream) throws Exception {
                if (entryStream.hasNext()) {
                    boolean more = entryStream.next(stream);
                    return more ? Scanner.Next.more : Scanner.Next.stopped;
                }
                return Scanner.Next.eos;
            }

            @Override
            public void close() {
                entryStream.close();
            }
        };
    }

    static public interface EntryStream {

        boolean hasNext();

        boolean next(RawEntryStream stream) throws Exception;

        void close();
    }

}
