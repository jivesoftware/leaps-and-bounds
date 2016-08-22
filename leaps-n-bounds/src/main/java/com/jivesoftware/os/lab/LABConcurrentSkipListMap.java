package com.jivesoftware.os.lab;

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class LABConcurrentSkipListMap extends AbstractMap<byte[], byte[]>
    implements ConcurrentNavigableMap<byte[], byte[]>, Cloneable, Serializable {

    /*
     * This class implements a tree-like two-dimensionally linked skip
     * list in which the index levels are represented in separate
     * nodes from the base nodes holding data.  There are two reasons
     * for taking this approach instead of the usual array-based
     * structure: 1) Array based implementations seem to encounter
     * more complexity and overhead 2) We can use cheaper algorithms
     * for the heavily-traversed index lists than can be used for the
     * base lists.  Here's a picture of some of the basics for a
     * possible list with 2 levels of index:
     *
     * Head nodes          Index nodes
     * +-+    right        +-+                      +-+
     * |2|---------------->| |--------------------->| |->null
     * +-+                 +-+                      +-+
     *  | down              |                        |
     *  v                   v                        v
     * +-+            +-+  +-+       +-+            +-+       +-+
     * |1|----------->| |->| |------>| |----------->| |------>| |->null
     * +-+            +-+  +-+       +-+            +-+       +-+
     *  v              |    |         |              |         |
     * Nodes  next     v    v         v              v         v
     * +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+
     * | |->|A|->|B|->|C|->|D|->|E|->|F|->|G|->|H|->|I|->|J|->|K|->null
     * +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+
     *
     * The base lists use a variant of the HM linked ordered set
     * algorithm. See Tim Harris, "A pragmatic implementation of
     * non-blocking linked lists"
     * http://www.cl.cam.ac.uk/~tlh20/publications.html and Maged
     * Michael "High Performance Dynamic Lock-Free Hash Tables and
     * List-Based Sets"
     * http://www.research.ibm.com/people/m/michael/pubs.htm.  The
     * basic idea in these lists is to mark the "next" pointers of
     * deleted nodes when deleting to avoid conflicts with concurrent
     * insertions, and when traversing to keep track of triples
     * (predecessor, node, successor) in order to detect when and how
     * to unlink these deleted nodes.
     *
     * Rather than using mark-bits to mark list deletions (which can
     * be slow and space-intensive using AtomicMarkedReference), nodes
     * use direct CAS'able next pointers.  On deletion, instead of
     * marking a pointer, they splice in another node that can be
     * thought of as standing for a marked pointer (indicating this by
     * using otherwise impossible field values).  Using plain nodes
     * acts roughly like "boxed" implementations of marked pointers,
     * but uses new nodes only when nodes are deleted, not for every
     * link.  This requires less space and supports faster
     * traversal. Even if marked references were better supported by
     * JVMs, traversal using this technique might still be faster
     * because any search need only read ahead one more node than
     * otherwise required (to check for trailing marker) rather than
     * unmasking mark bits or whatever on each read.
     *
     * This approach maintains the essential property needed in the HM
     * algorithm of changing the next-pointer of a deleted node so
     * that any other CAS of it will fail, but implements the idea by
     * changing the pointer to point to a different node, not by
     * marking it.  While it would be possible to further squeeze
     * space by defining marker nodes not to have key/value fields, it
     * isn't worth the extra type-testing overhead.  The deletion
     * markers are rarely encountered during traversal and are
     * normally quickly garbage collected. (Note that this technique
     * would not work well in systems without garbage collection.)
     *
     * In addition to using deletion markers, the lists also use
     * nullness of value fields to indicate deletion, in a style
     * similar to typical lazy-deletion schemes.  If a node's value is
     * null, then it is considered logically deleted and ignored even
     * though it is still reachable. This maintains proper control of
     * concurrent replace vs delete operations -- an attempted replace
     * must fail if a delete beat it by nulling field, and a delete
     * must return the last non-null value held in the field. (Note:
     * Null, rather than some special marker, is used for value fields
     * here because it just so happens to mesh with the Map API
     * requirement that method get returns null if there is no
     * mapping, which allows nodes to remain concurrently readable
     * even when deleted. Using any other marker value here would be
     * messy at best.)
     *
     * Here's the sequence of events for a deletion of node n with
     * predecessor b and successor f, initially:
     *
     *        +------+       +------+      +------+
     *   ...  |   b  |------>|   n  |----->|   f  | ...
     *        +------+       +------+      +------+
     *
     * 1. CAS n's value field from non-null to null.
     *    From this point on, no public operations encountering
     *    the node consider this mapping to exist. However, other
     *    ongoing insertions and deletions might still modify
     *    n's next pointer.
     *
     * 2. CAS n's next pointer to point to a new marker node.
     *    From this point on, no other nodes can be appended to n.
     *    which avoids deletion errors in CAS-based linked lists.
     *
     *        +------+       +------+      +------+       +------+
     *   ...  |   b  |------>|   n  |----->|marker|------>|   f  | ...
     *        +------+       +------+      +------+       +------+
     *
     * 3. CAS b's next pointer over both n and its marker.
     *    From this point on, no new traversals will encounter n,
     *    and it can eventually be GCed.
     *        +------+                                    +------+
     *   ...  |   b  |----------------------------------->|   f  | ...
     *        +------+                                    +------+
     *
     * A failure at step 1 leads to simple retry due to a lost race
     * with another operation. Steps 2-3 can fail because some other
     * thread noticed during a traversal a node with null value and
     * helped out by marking and/or unlinking.  This helping-out
     * ensures that no thread can become stuck waiting for progress of
     * the deleting thread.  The use of marker nodes slightly
     * complicates helping-out code because traversals must track
     * consistent reads of up to four nodes (b, n, marker, f), not
     * just (b, n, f), although the next field of a marker is
     * immutable, and once a next field is CAS'ed to point to a
     * marker, it never again changes, so this requires less care.
     *
     * Skip lists add indexing to this scheme, so that the base-level
     * traversals start close to the locations being found, inserted
     * or deleted -- usually base level traversals only traverse a few
     * nodes. This doesn't change the basic algorithm except for the
     * need to make sure base traversals start at predecessors (here,
     * b) that are not (structurally) deleted, otherwise retrying
     * after processing the deletion.
     *
     * Index levels are maintained as lists with volatile next fields,
     * using CAS to link and unlink.  Races are allowed in index-list
     * operations that can (rarely) fail to link in a new index node
     * or delete one. (We can't do this of course for data nodes.)
     * However, even when this happens, the index lists remain sorted,
     * so correctly serve as indices.  This can impact performance,
     * but since skip lists are probabilistic anyway, the net result
     * is that under contention, the effective "p" value may be lower
     * than its nominal value. And race windows are kept small enough
     * that in practice these failures are rare, even under a lot of
     * contention.
     *
     * The fact that retries (for both base and index lists) are
     * relatively cheap due to indexing allows some minor
     * simplifications of retry logic. Traversal restarts are
     * performed after most "helping-out" CASes. This isn't always
     * strictly necessary, but the implicit backoffs tend to help
     * reduce other downstream failed CAS's enough to outweigh restart
     * cost.  This worsens the worst case, but seems to improve even
     * highly contended cases.
     *
     * Unlike most skip-list implementations, index insertion and
     * deletion here require a separate traversal pass occurring after
     * the base-level action, to add or remove index nodes.  This adds
     * to single-threaded overhead, but improves contended
     * multithreaded performance by narrowing interference windows,
     * and allows deletion to ensure that all index nodes will be made
     * unreachable upon return from a public remove operation, thus
     * avoiding unwanted garbage retention. This is more important
     * here than in some other data structures because we cannot null
     * out node fields referencing user keys since they might still be
     * read by other ongoing traversals.
     *
     * Indexing uses skip list parameters that maintain good search
     * performance while using sparser-than-usual indices: The
     * hardwired parameters k=1, p=0.5 (see method doPut) mean
     * that about one-quarter of the nodes have indices. Of those that
     * do, half have one level, a quarter have two, and so on (see
     * Pugh's Skip List Cookbook, sec 3.4).  The expected total space
     * requirement for a map is slightly less than for the current
     * implementation of java.util.TreeMap.
     *
     * Changing the level of the index (i.e, the height of the
     * tree-like structure) also uses CAS. The head index has initial
     * level/height of one. Creation of an index with height greater
     * than the current level adds a level to the head index by
     * CAS'ing on a new top-most head. To maintain good performance
     * after a lot of removals, deletion methods heuristically try to
     * reduce the height if the topmost levels appear to be empty.
     * This may encounter races in which it possible (but rare) to
     * reduce and "lose" a level just as it is about to contain an
     * index (that will then never be encountered). This does no
     * structural harm, and in practice appears to be a better option
     * than allowing unrestrained growth of levels.
     *
     * The code for all this is more verbose than you'd like. Most
     * operations entail locating an element (or position to insert an
     * element). The code to do this can't be nicely factored out
     * because subsequent uses require a snapshot of predecessor
     * and/or successor and/or value fields which can't be returned
     * all at once, at least not without creating yet another object
     * to hold them -- creating such little objects is an especially
     * bad idea for basic internal search operations because it adds
     * to GC overhead.  (This is one of the few times I've wished Java
     * had macros.) Instead, some traversal code is interleaved within
     * insertion and removal operations.  The control logic to handle
     * all the retry conditions is sometimes twisty. Most search is
     * broken into 2 parts. findPredecessor() searches index nodes
     * only, returning a base-level predecessor of the key. findNode()
     * finishes out the base-level search. Even with this factoring,
     * there is a fair amount of near-duplication of code to handle
     * variants.
     *
     * To produce random values without interference across threads,
     * we use within-JDK thread local random support (via the
     * "secondary seed", to avoid interference with user-level
     * ThreadLocalRandom.)
     *
     * A previous version of this class wrapped non-comparable keys
     * with their comparators to emulate Comparables when using
     * comparators vs Comparables.  However, JVMs now appear to better
     * handle infusing comparator-vs-comparable choice into search
     * loops. Static method cpr(comparator, x, y) is used for all
     * comparisons, which works well as long as the comparator
     * argument is set up outside of loops (thus sometimes passed as
     * an argument to internal methods) to avoid field re-reads.
     *
     * For explanation of algorithms sharing at least a couple of
     * features with this one, see Mikhail Fomitchev's thesis
     * (http://www.cs.yorku.ca/~mikhail/), Keir Fraser's thesis
     * (http://www.cl.cam.ac.uk/users/kaf24/), and Hakan Sundell's
     * thesis (http://www.cs.chalmers.se/~phs/).
     *
     * Given the use of tree-like index nodes, you might wonder why
     * this doesn't use some kind of search tree instead, which would
     * support somewhat faster search operations. The reason is that
     * there are no known efficient lock-free insertion and deletion
     * algorithms for search trees. The immutability of the "down"
     * links of index nodes (as opposed to mutable "left" fields in
     * true trees) makes this tractable using only CAS operations.
     *
     * Notation guide for local variables
     * Node:         b, n, f    for  predecessor, node, successor
     * Index:        q, r, d    for index node, right, down.
     *               t          for another index node
     * Head:         h
     * Levels:       j
     * Keys:         k, key
     * Values:       v, value
     * Comparisons:  c
     */
    private static final long serialVersionUID = -8627078645895051609L;

    /**
     * Special value used to identify base-level header
     */
    private static final long BASE_HEADER = -3;
    private static final long SELF = -2;
    private static final long NIL = -1;

    /**
     * The topmost head index of the skiplist.
     */
    private transient volatile HeadIndex head;

    private static final AtomicReferenceFieldUpdater<LABConcurrentSkipListMap, HeadIndex> headUpdater =
        AtomicReferenceFieldUpdater.newUpdater(LABConcurrentSkipListMap.class, HeadIndex.class, "head");

    /**
     * The comparator used to maintain order in this map, or null if
     * using natural ordering.  (Non-private to simplify access in
     * nested classes.)
     * @serial
     */
    final LABIndexableMemory comparator;

    /** Lazily initialized key set */
    private transient KeySet keySet;
    /** Lazily initialized entry set */
    private transient EntrySet entrySet;
    /** Lazily initialized values collection */
    private transient Values values;
    /** Lazily initialized descending key set */
    private transient ConcurrentNavigableMap<byte[], byte[]> descendingMap;

    /**
     * Initializes or resets state. Needed by constructors, clone,
     * clear, readObject. and ConcurrentSkipListSet.clone.
     * (Note that comparator must be separately initialized.)
     */
    private void initialize() {
        keySet = null;
        entrySet = null;
        values = null;
        descendingMap = null;
        head = new HeadIndex(new Node(NIL, BASE_HEADER, null),
            null, null, 1);
    }

    /**
     * compareAndSet head node
     */
    private boolean casHead(HeadIndex cmp, HeadIndex val) {
        return headUpdater.compareAndSet(this, cmp, val);
    }

    /* ---------------- Nodes -------------- */
    /**
     * Nodes hold keys and values, and are singly linked in sorted
     * order, possibly with some intervening marker nodes. The list is
     * headed by a dummy node accessible as head.node. The value field
     * is declared only as Object because it takes special non-V
     * values for marker and header nodes.
     */
    static final class Node {

        final long key;

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

        /**
         * Creates a new marker node. A marker is distinguished by
         * having its value field point to itself.  Marker nodes also
         * have null keys, a fact that is exploited in a few places,
         * but this doesn't distinguish markers from the base-level
         * header node (head.node), which also has a null key.
         */
        Node(Node next) {
            this.key = NIL;
            this.value = SELF;
            this.next = next;
        }

        /**
         * compareAndSet value field
         */
        boolean casValue(long cmp, long val) {
            return valueUpdater.compareAndSet(this, cmp, val);
        }

        /**
         * compareAndSet next field
         */
        boolean casNext(Node cmp, Node val) {
            return nextUpdater.compareAndSet(this, cmp, val);
        }

        /**
         * Returns true if this node is a marker. This method isn't
         * actually called in any current code checking for markers
         * because callers will have already read value field and need
         * to use that read (not another done here) and so directly
         * test if value points to node.
         *
         * @return true if this node is a marker node
         */
        boolean isMarker() {
            return value == SELF;
        }

        /**
         * Returns true if this node is the header of base-level list.
         * @return true if this node is header node
         */
        boolean isBaseHeader() {
            return value == BASE_HEADER;
        }

        /**
         * Tries to append a deletion marker to this node.
         * @param f the assumed current successor of this node
         * @return true if successful
         */
        boolean appendMarker(Node f) {
            return casNext(f, new Node(f));
        }

        /**
         * Helps out a deletion by appending marker or unlinking from
         * predecessor. This is called during traversals when value
         * field seen to be null.
         * @param b predecessor
         * @param f successor
         */
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

        /**
         * Returns value if this node contains a valid key-value pair,
         * else null.
         * @return this node's value if it isn't a marker or header or
         * is deleted, else null
         */
        byte[] getValidValue(LABIndexableMemory comparator) {
            long v = value;
            if (v == SELF || v == BASE_HEADER) {
                return null;
            }
            return comparator.bytes(v);
        }

        /**
         * Creates and returns a new SimpleImmutableEntry holding current
         * mapping if this node holds a valid value, else null.
         * @return new entry or null
         */
        AbstractMap.SimpleImmutableEntry createSnapshot(LABIndexableMemory comparator) {
            long v = value;
            if (v == NIL || v == SELF || v == BASE_HEADER) {
                return null;
            }
            return new AbstractMap.SimpleImmutableEntry<>(key == NIL ? null : key, comparator.bytes(v));
        }

    }

    /* ---------------- Indexing -------------- */
    /**
     * Index nodes represent the levels of the skip list.  Note that
     * even though both Nodes and Indexes have forward-pointing
     * fields, they have different types and are handled in different
     * ways, that can't nicely be captured by placing field in a
     * shared abstract class.
     */
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
        }
    }

    /* ---------------- Comparison utilities -------------- */
    /**
     * Compares using comparator or natural ordering if null.
     * Called only by methods that have performed required type checks.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static final int cprll(LABIndexableMemory c, long x, long y) {
        return c.compare(x, y);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static final int cprlb(LABIndexableMemory c, long x, byte[] y) {
        return c.compare(x, y);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static final int cprbl(LABIndexableMemory c, byte[] x, long y) {
        return c.compare(x, y);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static final int cprbb(LABIndexableMemory c, byte[] x, byte[] y) {
        return c.compare(x, y);
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
    private Node findPredecessor(byte[] key, LABIndexableMemory cmp) {
        if (key == null) {
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
                    if (cprbl(cmp, key, k) > 0) {
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
    private Node findNode(byte[] key) {
        if (key == null) {
            throw new NullPointerException(); // don't postpone errors
        }
        outer:
        for (;;) {
            for (Node b = findPredecessor(key, comparator), n = b.next;;) {
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
                if ((c = cprbl(comparator, key, n.key)) == 0) {
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
    private byte[] doGet(byte[] key) {
        if (key == null) {
            throw new NullPointerException();
        }
        outer:
        for (;;) {
            for (Node b = findPredecessor(key, comparator), n = b.next;;) {
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
                if ((c = cprbl(comparator, key, n.key)) == 0) {
                    return comparator.bytes(v);
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

    /* ---------------- Insertion -------------- */
    /**
     * Main insertion method.  Adds element if not present, or
     * replaces value if present and onlyIfAbsent is false.
     * @param key the key
     * @param value the value that must be associated with key
     * @param onlyIfAbsent if should not insert if already present
     * @return the old value, or null if newly inserted
     */
    private byte[] doPut(byte[] keyBytes, byte[] valueBytes, boolean onlyIfAbsent) {

        Node z;             // added node
        if (keyBytes == null) {
            throw new NullPointerException();
        }

        long kid = NIL;
        long vid = NIL;

        outer:
        for (;;) {
            for (Node b = findPredecessor(keyBytes, comparator), n = b.next;;) {
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
                    if ((c = cprbl(comparator, keyBytes, n.key)) > 0) {
                        b = n;
                        n = f;
                        continue;
                    }
                    if (c == 0) {
                        if (onlyIfAbsent) {
                            return comparator.bytes(v);
                        }
                        if (n.casValue(v, vid)) {
                            byte[] was = comparator.bytes(v);
                            comparator.free(v);
                            return was;
                        }
                        break; // restart if lost race to replace value
                    }
                    // else c < 0; fall through
                }

                if (kid == NIL) {
                    kid = comparator.allocate(keyBytes);
                }
                if (vid == NIL) {
                    vid = valueBytes == null ? NIL : comparator.allocate(valueBytes);
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
            comparator.free(kid);
        }

        if (vid != NIL) {
            comparator.free(vid);
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
                        int c = cprbl(comparator, keyBytes, n.key);
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

    /* ---------------- Deletion -------------- */
    /**
     * Main deletion method. Locates node, nulls value, appends a
     * deletion marker, unlinks predecessor, removes associated index
     * nodes, and possibly reduces head index level.
     *
     * Index nodes are cleared out simply by calling findPredecessor.
     * which unlinks indexes to deleted nodes found along path to key,
     * which will include the indexes to this node.  This is done
     * unconditionally. We can't check beforehand whether there are
     * index nodes because it might be the case that some or all
     * indexes hadn't been inserted yet for this node during initial
     * search for it, and we'd like to ensure lack of garbage
     * retention, so must call to be sure.
     *
     * @param key the key
     * @param value if non-null, the value that must be
     * associated with key
     * @return the node, or null if not found
     */
    final byte[] doRemove(byte[] key, byte[] value) {
        if (key == null) {
            throw new NullPointerException();
        }

        byte[] was = null;
        outer:
        for (;;) {
            for (Node b = findPredecessor(key, comparator), n = b.next;;) {
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
                if ((v = n.value) == NIL) {        // n is deleted
                    n.helpDelete(b, f);
                    break;
                }
                if (b.value == NIL || v == SELF) // b is deleted
                {
                    break;
                }
                if ((c = cprbl(comparator, key, n.key)) < 0) {
                    break outer;
                }
                if (c > 0) {
                    b = n;
                    n = f;
                    continue;
                }
                if (value != null && !Arrays.equals((byte[]) value, comparator.bytes(v))) {
                    break outer;
                }
                if (!n.casValue(v, NIL)) {
                    break;
                } else {
                    was = comparator.bytes(v);
                    comparator.free(v);
                }
                if (!n.appendMarker(f) || !b.casNext(n, f)) {
                    findNode(key);                  // retry via findNode
                } else {
                    findPredecessor(key, comparator);      // clean index
                    if (head.right == null) {
                        tryReduceLevel();
                    }
                }
                return was;
            }
        }
        return null;
    }

    /**
     * Possibly reduce head level if it has no nodes.  This method can
     * (rarely) make mistakes, in which case levels can disappear even
     * though they are about to contain index nodes. This impacts
     * performance, not correctness.  To minimize mistakes as well as
     * to reduce hysteresis, the level is reduced by one only if the
     * topmost three levels look empty. Also, if the removed level
     * looks non-empty after CAS, we try to change it back quick
     * before anyone notices our mistake! (This trick works pretty
     * well because this method will practically never make mistakes
     * unless current thread stalls immediately before first CAS, in
     * which case it is very unlikely to stall again immediately
     * afterwards, so will recover.)
     *
     * We put up with all this rather than just let levels grow
     * because otherwise, even a small map that has undergone a large
     * number of insertions and removals will have a lot of levels,
     * slowing down access more than would an occasional unwanted
     * reduction.
     */
    private void tryReduceLevel() {
        HeadIndex h = head;
        HeadIndex d;
        HeadIndex e;
        if (h.level > 3
            && (d = (HeadIndex) h.down) != null
            && (e = (HeadIndex) d.down) != null
            && e.right == null
            && d.right == null
            && h.right == null
            && casHead(h, d)
            && // try to set
            h.right != null) // recheck
        {
            casHead(d, h);   // try to backout
        }
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

    /**
     * Removes first entry; returns its snapshot.
     * @return null if empty, else snapshot of first entry
     */
    private Map.Entry doRemoveFirstEntry() {
        for (Node b, n;;) {
            if ((n = (b = head.node).next) == null) {
                return null;
            }
            Node f = n.next;
            if (n != b.next) {
                continue;
            }
            long v = n.value;
            if (v == NIL) {
                n.helpDelete(b, f);
                continue;
            }
            byte[] was = null;
            if (!n.casValue(v, NIL)) {
                continue;
            } else {
                was = comparator.bytes(v);
                comparator.free(v);
            }
            if (!n.appendMarker(f) || !b.casNext(n, f)) {
                findFirst(); // retry
            }
            clearIndexToFirst();
            return new AbstractMap.SimpleImmutableEntry<>(comparator.bytes(n.key), was);
        }
    }

    /**
     * Clears out index nodes associated with deleted first entry.
     */
    private void clearIndexToFirst() {
        for (;;) {
            for (Index q = head;;) {
                Index r = q.right;
                if (r != null && r.indexesDeletedNode() && !q.unlink(r)) {
                    break;
                }
                if ((q = q.down) == null) {
                    if (head.right == null) {
                        tryReduceLevel();
                    }
                    return;
                }
            }
        }
    }

    /**
     * Removes last entry; returns its snapshot.
     * Specialized variant of doRemove.
     * @return null if empty, else snapshot of last entry
     */
    private Map.Entry doRemoveLastEntry() {
        for (;;) {
            Node b = findPredecessorOfLast();
            Node n = b.next;
            if (n == null) {
                if (b.isBaseHeader()) // empty
                {
                    return null;
                } else {
                    continue; // all b's successors are deleted; retry
                }
            }
            for (;;) {
                Node f = n.next;
                if (n != b.next) // inconsistent read
                {
                    break;
                }
                long v = n.value;
                if (v == NIL) {                    // n is deleted
                    n.helpDelete(b, f);
                    break;
                }
                if (b.value == NIL || v == SELF) // b is deleted
                {
                    break;
                }
                if (f != null) {
                    b = n;
                    n = f;
                    continue;
                }
                byte[] was = null;
                if (!n.casValue(v, NIL)) {
                    break;
                } else {
                    was = comparator.bytes(v);
                    comparator.free(v);
                }
                byte[] key = comparator.bytes(n.key);
                if (!n.appendMarker(f) || !b.casNext(n, f)) {
                    findNode(key);                  // retry via findNode
                } else {                              // clean index
                    findPredecessor(key, comparator);
                    if (head.right == null) {
                        tryReduceLevel();
                    }
                }
                return new AbstractMap.SimpleImmutableEntry(key, was);
            }
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

    /**
     * Specialized variant of findPredecessor to get predecessor of last
     * valid node.  Needed when removing the last entry.  It is possible
     * that all successors of returned node will have been deleted upon
     * return, in which case this method can be retried.
     * @return likely predecessor of last node
     */
    private Node findPredecessorOfLast() {
        for (;;) {
            for (Index q = head;;) {
                Index d, r;
                if ((r = q.right) != null) {
                    if (r.indexesDeletedNode()) {
                        q.unlink(r);
                        break;    // must restart
                    }
                    // proceed as far across as possible without overshooting
                    if (r.node.next != null) {
                        q = r;
                        continue;
                    }
                }
                if ((d = q.down) != null) {
                    q = d;
                } else {
                    return q.node;
                }
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
    final Node findNear(byte[] key, int rel, LABIndexableMemory cmp) {
        if (key == null) {
            throw new NullPointerException();
        }
        for (;;) {
            for (Node b = findPredecessor(key, cmp), n = b.next;;) {
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
                int c = cprbl(cmp, key, n.key);
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

    /**
     * Returns SimpleImmutableEntry for results of findNear.
     * @param key the key
     * @param rel the relation -- OR'ed combination of EQ, LT, GT
     * @return Entry fitting relation, or null if no such
     */
    final AbstractMap.SimpleImmutableEntry<byte[], byte[]> getNear(byte[] key, int rel) {
        for (;;) {
            Node n = findNear(key, rel, comparator);
            if (n == null) {
                return null;
            }
            AbstractMap.SimpleImmutableEntry e = n.createSnapshot(comparator);
            if (e != null) {
                return e;
            }
        }
    }

    public LABConcurrentSkipListMap(LABIndexableMemory comparator) {
        this.comparator = comparator;
        initialize();
    }

    /* ------ Map API methods ------ */
    /**
     * Returns {@code true} if this map contains a mapping for the specified
     * key.
     *
     * @param key key whose presence in this map is to be tested
     * @return {@code true} if this map contains a mapping for the specified key
     * @throws ClassCastException if the specified key cannot be compared
     *         with the keys currently in the map
     * @throws NullPointerException if the specified key is null
     */
    public boolean containsKey(Object keyBytes) {
        return doGet((byte[]) keyBytes) != null;
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     *
     * <p>More formally, if this map contains a mapping from a key
     * {@code k} to a value {@code v} such that {@code key} compares
     * equal to {@code k} according to the map's ordering, then this
     * method returns {@code v}; otherwise it returns {@code null}.
     * (There can be at most one such mapping.)
     *
     * @throws ClassCastException if the specified key cannot be compared
     *         with the keys currently in the map
     * @throws NullPointerException if the specified key is null
     */
    public byte[] get(Object keyBytes) {
        return doGet((byte[]) keyBytes);
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or the given defaultValue if this map contains no mapping for the key.
     *
     * @param key the key
     * @param defaultValue the value to return if this map contains
     * no mapping for the given key
     * @return the mapping for the key, if present; else the defaultValue
     * @throws NullPointerException if the specified key is null
     * @since 1.8
     */
    @Override
    public byte[] getOrDefault(Object keyBytes, byte[] defaultValue) {
        byte[] v;
        return (v = doGet((byte[]) keyBytes)) == null ? defaultValue : v;

    }

    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old
     * value is replaced.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with the specified key, or
     *         {@code null} if there was no mapping for the key
     * @throws ClassCastException if the specified key cannot be compared
     *         with the keys currently in the map
     * @throws NullPointerException if the specified key or value is null
     */
    @Override
    public byte[] put(byte[] keyBytes, byte[] value) {
        if (value == null) {
            throw new NullPointerException();
        }
        return doPut(keyBytes, value, false);
    }

    /**
     * Removes the mapping for the specified key from this map if present.
     *
     * @param  key key for which mapping should be removed
     * @return the previous value associated with the specified key, or
     *         {@code null} if there was no mapping for the key
     * @throws ClassCastException if the specified key cannot be compared
     *         with the keys currently in the map
     * @throws NullPointerException if the specified key is null
     */
    @Override
    public byte[] remove(Object keyBytes) {
        return doRemove((byte[]) keyBytes, null);
    }

    /**
     * Returns {@code true} if this map maps one or more keys to the
     * specified value.  This operation requires time linear in the
     * map size. Additionally, it is possible for the map to change
     * during execution of this method, in which case the returned
     * result may be inaccurate.
     *
     * @param value value whose presence in this map is to be tested
     * @return {@code true} if a mapping to {@code value} exists;
     *         {@code false} otherwise
     * @throws NullPointerException if the specified value is null
     */
    public boolean containsValue(Object value) {
        if (value == null) {
            throw new NullPointerException();
        }
        for (Node n = findFirst(); n != null; n = n.next) {
            byte[] v = n.getValidValue(comparator);
            if (v != null && Arrays.equals((byte[]) value, v)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the number of key-value mappings in this map.  If this map
     * contains more than {@code Integer.MAX_VALUE} elements, it
     * returns {@code Integer.MAX_VALUE}.
     *
     * <p>Beware that, unlike in most collections, this method is
     * <em>NOT</em> a constant-time operation. Because of the
     * asynchronous nature of these maps, determining the current
     * number of elements requires traversing them all to count them.
     * Additionally, it is possible for the size to change during
     * execution of this method, in which case the returned result
     * will be inaccurate. Thus, this method is typically not very
     * useful in concurrent applications.
     *
     * @return the number of elements in this map
     */
    public int size() {
        long count = 0;
        for (Node n = findFirst(); n != null; n = n.next) {
            if (n.getValidValue(comparator) != null) {
                ++count;
            }
        }
        return (count >= Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) count;
    }

    /**
     * Returns {@code true} if this map contains no key-value mappings.
     * @return {@code true} if this map contains no key-value mappings
     */
    public boolean isEmpty() {
        return findFirst() == null;
    }

    /**
     * Removes all of the mappings from this map.
     */
    public void clear() {
        initialize();
    }

    /**
     * If the specified key is not already associated with a value,
     * attempts to compute its value using the given mapping function
     * and enters it into this map unless {@code null}.  The function
     * is <em>NOT</em> guaranteed to be applied once atomically only
     * if the value is not present.
     *
     * @param key key with which the specified value is to be associated
     * @param mappingFunction the function to compute a value
     * @return the current (existing or computed) value associated with
     *         the specified key, or null if the computed value is null
     * @throws NullPointerException if the specified key is null
     *         or the mappingFunction is null
     * @since 1.8
     */
    public byte[] computeIfAbsent(byte[] keyBytes,
        Function<byte[], byte[]> mappingFunction) {
        if (keyBytes == null || mappingFunction == null) {
            throw new NullPointerException();
        }
        byte[] v, p, r;
        if ((v = doGet(keyBytes)) == null) {
            if ((r = mappingFunction.apply(keyBytes)) != null) {
                v = (p = doPut(keyBytes, r, true)) == null ? r : p;
            }
        }
        return v;

    }

    /**
     * If the value for the specified key is present, attempts to
     * compute a new mapping given the key and its current mapped
     * value. The function is <em>NOT</em> guaranteed to be applied
     * once atomically.
     *
     * @param key key with which a value may be associated
     * @param remappingFunction the function to compute a value
     * @return the new value associated with the specified key, or null if none
     * @throws NullPointerException if the specified key is null
     *         or the remappingFunction is null
     * @since 1.8
     */
    public byte[] computeIfPresent(byte[] keyBytes,
        BiFunction<byte[], byte[], byte[]> remappingFunction) {
        if (keyBytes == null || remappingFunction == null) {
            throw new NullPointerException();
        }
        Node n;
        long v;
        while ((n = findNode(keyBytes)) != null) {
            if ((v = n.value) != NIL) {
                byte[] vv = comparator.bytes(v);
                byte[] rBytes = remappingFunction.apply(keyBytes, vv);
                if (rBytes != null) {
                    long r = comparator.allocate(rBytes);
                    if (n.casValue(v, r)) {
                        comparator.free(v);
                        return rBytes;
                    } else {
                        comparator.free(r);
                    }
                } else if (doRemove(keyBytes, vv) != null) {
                    break;
                }
            }
        }
        return null;
    }

    /**
     * Attempts to compute a mapping for the specified key and its
     * current mapped value (or {@code null} if there is no current
     * mapping). The function is <em>NOT</em> guaranteed to be applied
     * once atomically.
     *
     * @param key key with which the specified value is to be associated
     * @param remappingFunction the function to compute a value
     * @return the new value associated with the specified key, or null if none
     * @throws NullPointerException if the specified key is null
     *         or the remappingFunction is null
     * @since 1.8
     */
    public byte[] compute(byte[] keyBytes,
        BiFunction<byte[], byte[], byte[]> remappingFunction) {
        if (keyBytes == null || remappingFunction == null) {
            throw new NullPointerException();
        }

        for (;;) {
            Node n;
            long v;
            byte[] r;
            if ((n = findNode(keyBytes)) == null) {
                if ((r = remappingFunction.apply(keyBytes, null)) == null) {
                    break;
                }
                if (doPut(keyBytes, r, true) == null) {
                    return r;
                }
            } else if ((v = n.value) != NIL) {
                byte[] vv = comparator.bytes(v);
                if ((r = remappingFunction.apply(keyBytes, vv)) != null) {
                    long rid = comparator.allocate(r);
                    if (n.casValue(v, rid)) {
                        comparator.free(v);
                        return r;
                    } else {
                        comparator.free(rid);
                    }
                } else if (doRemove(keyBytes, vv) != null) {
                    break;
                }
            }
        }

        return null;
    }

    /**
     * If the specified key is not already associated with a value,
     * associates it with the given value.  Otherwise, replaces the
     * value with the results of the given remapping function, or
     * removes if {@code null}. The function is <em>NOT</em>
     * guaranteed to be applied once atomically.
     *
     * @param key key with which the specified value is to be associated
     * @param value the value to use if absent
     * @param remappingFunction the function to recompute a value if present
     * @return the new value associated with the specified key, or null if none
     * @throws NullPointerException if the specified key or value is null
     *         or the remappingFunction is null
     * @since 1.8
     */
    public byte[] merge(byte[] keyBytes, byte[] value,
        BiFunction<byte[], byte[], byte[]> remappingFunction) {
        if (keyBytes == null || value == null || remappingFunction == null) {
            throw new NullPointerException();
        }

        for (;;) {
            Node n;
            long v;
            byte[] r;
            if ((n = findNode(keyBytes)) == null) {
                if (doPut(keyBytes, value, true) == null) {
                    return value;
                }
            } else if ((v = n.value) != NIL) {
                byte[] vv = comparator.bytes(v);
                if ((r = remappingFunction.apply(vv, value)) != null) {
                    long rid = comparator.allocate(r);
                    if (n.casValue(v, rid)) {
                        comparator.free(v);
                        return r;
                    } else {
                        comparator.free(rid);
                    }
                } else if (doRemove(keyBytes, vv) != null) {
                    return null;
                }
            }
        }

    }

    public void freeAll() {
        for (Iterator<byte[]> iterator = new FreeIterator(); iterator.hasNext();) {
            iterator.next();
        }
        comparator.freeAll();
    }

    /* ---------------- View methods -------------- */

 /*
     * Note: Lazy initialization works for views because view classes
     * are stateless/immutable so it doesn't matter wrt correctness if
     * more than one is created (which will only rarely happen).  Even
     * so, the following idiom conservatively ensures that the method
     * returns the one it created if it does so, not one created by
     * another racing thread.
     */
    /**
     * Returns a {@link NavigableSet} view of the keys contained in this map.
     *
     * <p>The set's iterator returns the keys in ascending order.
     * The set's spliterator additionally reports {@link Spliterator#CONCURRENT},
     * {@link Spliterator#NONNULL}, {@link Spliterator#SORTED} and
     * {@link Spliterator#ORDERED}, with an encounter order that is ascending
     * key order.  The spliterator's comparator (see
     * {@link java.util.Spliterator#getComparator()}) is {@code null} if
     * the map's comparator (see {@link #comparator()}) is {@code null}.
     * Otherwise, the spliterator's comparator is the same as or imposes the
     * same total ordering as the map's comparator.
     *
     * <p>The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  The set supports element
     * removal, which removes the corresponding mapping from the map,
     * via the {@code Iterator.remove}, {@code Set.remove},
     * {@code removeAll}, {@code retainAll}, and {@code clear}
     * operations.  It does not support the {@code add} or {@code addAll}
     * operations.
     *
     * <p>The view's iterators and spliterators are
     * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
     *
     * <p>This method is equivalent to method {@code navigableKeySet}.
     *
     * @return a navigable set view of the keys in this map
     */
    public NavigableSet<byte[]> keySet() {
        KeySet ks = keySet;
        return (ks != null) ? ks : (keySet = new KeySet(this));
    }

    public NavigableSet<byte[]> navigableKeySet() {
        KeySet ks = keySet;
        return (ks != null) ? ks : (keySet = new KeySet(this));
    }

    /**
     * Returns a {@link Collection} view of the values contained in this map.
     * <p>The collection's iterator returns the values in ascending order
     * of the corresponding keys. The collections's spliterator additionally
     * reports {@link Spliterator#CONCURRENT}, {@link Spliterator#NONNULL} and
     * {@link Spliterator#ORDERED}, with an encounter order that is ascending
     * order of the corresponding keys.
     *
     * <p>The collection is backed by the map, so changes to the map are
     * reflected in the collection, and vice-versa.  The collection
     * supports element removal, which removes the corresponding
     * mapping from the map, via the {@code Iterator.remove},
     * {@code Collection.remove}, {@code removeAll},
     * {@code retainAll} and {@code clear} operations.  It does not
     * support the {@code add} or {@code addAll} operations.
     *
     * <p>The view's iterators and spliterators are
     * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
     */
    public Collection<byte[]> values() {
        Values vs = values;
        return (vs != null) ? vs : (values = new Values(this));
    }

    /**
     * Returns a {@link Set} view of the mappings contained in this map.
     *
     * <p>The set's iterator returns the entries in ascending key order.  The
     * set's spliterator additionally reports {@link Spliterator#CONCURRENT},
     * {@link Spliterator#NONNULL}, {@link Spliterator#SORTED} and
     * {@link Spliterator#ORDERED}, with an encounter order that is ascending
     * key order.
     *
     * <p>The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  The set supports element
     * removal, which removes the corresponding mapping from the map,
     * via the {@code Iterator.remove}, {@code Set.remove},
     * {@code removeAll}, {@code retainAll} and {@code clear}
     * operations.  It does not support the {@code add} or
     * {@code addAll} operations.
     *
     * <p>The view's iterators and spliterators are
     * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
     *
     * <p>The {@code Map.Entry} elements traversed by the {@code iterator}
     * or {@code spliterator} do <em>not</em> support the {@code setValue}
     * operation.
     *
     * @return a set view of the mappings contained in this map,
     *         sorted in ascending key order
     */
    public Set<Map.Entry<byte[], byte[]>> entrySet() {
        EntrySet es = entrySet;
        return (es != null) ? es : (entrySet = new EntrySet(this));
    }

    public ConcurrentNavigableMap<byte[], byte[]> descendingMap() {
        ConcurrentNavigableMap<byte[], byte[]> dm = descendingMap;
        return (dm != null) ? dm : (descendingMap = new SubMap(this, null, false, null, false, true));
    }

    public NavigableSet<byte[]> descendingKeySet() {
        return descendingMap().navigableKeySet();
    }

    /* ---------------- AbstractMap Overrides -------------- */
    /**
     * Compares the specified object with this map for equality.
     * Returns {@code true} if the given object is also a map and the
     * two maps represent the same mappings.  More formally, two maps
     * {@code m1} and {@code m2} represent the same mappings if
     * {@code m1.entrySet().equals(m2.entrySet())}.  This
     * operation may return misleading results if either map is
     * concurrently modified during execution of this method.
     *
     * @param o object to be compared for equality with this map
     * @return {@code true} if the specified object is equal to this map
     */
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Map)) {
            return false;
        }
        Map<?, ?> m = (Map<?, ?>) o;
        try {
            for (Map.Entry<byte[], byte[]> e : this.entrySet()) {
                if (!e.getValue().equals(m.get(e.getKey()))) {
                    return false;
                }
            }
            for (Map.Entry<?, ?> e : m.entrySet()) {
                Object k = e.getKey();
                Object v = e.getValue();
                if (k == null || v == null || !v.equals(get(k))) {
                    return false;
                }
            }
            return true;
        } catch (ClassCastException unused) {
            return false;
        } catch (NullPointerException unused) {
            return false;
        }
    }

    /* ------ ConcurrentMap API methods ------ */
    /**
     * {@inheritDoc}
     *
     * @return the previous value associated with the specified key,
     *         or {@code null} if there was no mapping for the key
     * @throws ClassCastException if the specified key cannot be compared
     *         with the keys currently in the map
     * @throws NullPointerException if the specified key or value is null
     */
    @Override
    public byte[] putIfAbsent(byte[] keyBytes, byte[] value) {
        if (value == null) {
            throw new NullPointerException();
        }
        return doPut(keyBytes, value, true);
    }

    /**
     * {@inheritDoc}
     *
     * @throws ClassCastException if the specified key cannot be compared
     *         with the keys currently in the map
     * @throws NullPointerException if the specified key is null
     */
    @Override
    public boolean remove(Object keyBytes, Object value) {
        if (keyBytes == null) {
            throw new NullPointerException();
        }
        return value != null && doRemove((byte[]) keyBytes, (byte[])value) != null;

    }

    /**
     * {@inheritDoc}
     *
     * @throws ClassCastException if the specified key cannot be compared
     *         with the keys currently in the map
     * @throws NullPointerException if any of the arguments are null
     */
    public boolean replace(byte[] keyBytes, byte[] oldValue, byte[] newValue) {
        if (keyBytes == null || oldValue == null || newValue == null) {
            throw new NullPointerException();
        }
        for (;;) {
            Node n;
            long v;
            if ((n = findNode(keyBytes)) == null) {
                return false;
            }
            if ((v = n.value) != NIL) {
                if (!Arrays.equals(oldValue, comparator.bytes(v))) {
                    return false;
                }
                long nid = comparator.allocate(newValue);
                if (n.casValue(v, nid)) {
                    comparator.free(v);
                    return true;
                } else {
                    comparator.free(nid);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * @return the previous value associated with the specified key,
     *         or {@code null} if there was no mapping for the key
     * @throws ClassCastException if the specified key cannot be compared
     *         with the keys currently in the map
     * @throws NullPointerException if the specified key or value is null
     */
    public byte[] replace(byte[] keyBytes, byte[] value) {
        if (keyBytes == null || value == null) {
            throw new NullPointerException();
        }
        for (;;) {
            Node n;
            long v;
            if ((n = findNode(keyBytes)) == null) {
                return null;
            }
            if ((v = n.value) != NIL) {
                long vid = comparator.allocate(value);
                if (n.casValue(v, vid)) {
                    byte[] vv = comparator.bytes(v);
                    comparator.free(v);
                    return vv;
                } else {
                    comparator.free(vid);
                }
            }
        }
    }

    /* ------ SortedMap API methods ------ */
    public Comparator<? super byte[]> comparator() {
        return comparator.bytesCompartor();
    }

    /**
     * @throws NoSuchElementException {@inheritDoc}
     */
    public byte[] firstKey() {
        Node n = findFirst();
        if (n == null) {
            throw new NoSuchElementException();
        }
        return comparator.bytes(n.key);
    }

    /**
     * @throws NoSuchElementException {@inheritDoc}
     */
    public byte[] lastKey() {
        Node n = findLast();
        if (n == null) {
            throw new NoSuchElementException();
        }
        return comparator.bytes(n.key);
    }

    /**
     * @throws ClassCastException {@inheritDoc}
     * @throws NullPointerException if {@code fromKey} or {@code toKey} is null
     * @throws IllegalArgumentException {@inheritDoc}
     */
    public ConcurrentNavigableMap subMap(byte[] fromKey,
        boolean fromInclusive,
        byte[] toKey,
        boolean toInclusive) {
        if (fromKey == null || toKey == null) {
            throw new NullPointerException();
        }
        return new SubMap(this, fromKey, fromInclusive, toKey, toInclusive, false);
    }

    /**
     * @throws ClassCastException {@inheritDoc}
     * @throws NullPointerException if {@code toKey} is null
     * @throws IllegalArgumentException {@inheritDoc}
     */
    public ConcurrentNavigableMap<byte[], byte[]> headMap(byte[] toKey,
        boolean inclusive) {
        if (toKey == null) {
            throw new NullPointerException();
        }
        return new SubMap(this, null, false, toKey, inclusive, false);
    }

    /**
     * @throws ClassCastException {@inheritDoc}
     * @throws NullPointerException if {@code fromKey} is null
     * @throws IllegalArgumentException {@inheritDoc}
     */
    public ConcurrentNavigableMap<byte[], byte[]> tailMap(byte[] fromKey,
        boolean inclusive) {
        if (fromKey == null) {
            throw new NullPointerException();
        }
        return new SubMap(this, fromKey, inclusive, null, false, false);
    }

    /**
     * @throws ClassCastException {@inheritDoc}
     * @throws NullPointerException if {@code fromKey} or {@code toKey} is null
     * @throws IllegalArgumentException {@inheritDoc}
     */
    public ConcurrentNavigableMap<byte[], byte[]> subMap(byte[] fromKey, byte[] toKey) {
        return subMap(fromKey, true, toKey, false);
    }

    /**
     * @throws ClassCastException {@inheritDoc}
     * @throws NullPointerException if {@code toKey} is null
     * @throws IllegalArgumentException {@inheritDoc}
     */
    public ConcurrentNavigableMap<byte[], byte[]> headMap(byte[] toKey) {
        return headMap(toKey, false);
    }

    /**
     * @throws ClassCastException {@inheritDoc}
     * @throws NullPointerException if {@code fromKey} is null
     * @throws IllegalArgumentException {@inheritDoc}
     */
    public ConcurrentNavigableMap<byte[], byte[]> tailMap(byte[] fromKey) {
        return tailMap(fromKey, true);
    }

    /* ---------------- Relational operations -------------- */
    /**
     * Returns a key-value mapping associated with the greatest key
     * strictly less than the given key, or {@code null} if there is
     * no such key. The returned entry does <em>not</em> support the
     * {@code Entry.setValue} method.
     *
     * @throws ClassCastException {@inheritDoc}
     * @throws NullPointerException if the specified key is null
     */
    public Map.Entry<byte[], byte[]> lowerEntry(byte[] keyBytes) {
        return getNear(keyBytes, LT);
    }

    /**
     * @throws ClassCastException {@inheritDoc}
     * @throws NullPointerException if the specified key is null
     */
    public byte[] lowerKey(byte[] keyBytes) {
        Node n = findNear(keyBytes, LT, comparator);
        return (n == null) ? null : comparator.bytes(n.key);
    }

    /**
     * Returns a key-value mapping associated with the greatest key
     * less than or equal to the given key, or {@code null} if there
     * is no such key. The returned entry does <em>not</em> support
     * the {@code Entry.setValue} method.
     *
     * @param key the key
     * @throws ClassCastException {@inheritDoc}
     * @throws NullPointerException if the specified key is null
     */
    public Map.Entry<byte[], byte[]> floorEntry(byte[] keyBytes) {
        return getNear(keyBytes, LT | EQ);
    }

    /**
     * @param key the key
     * @throws ClassCastException {@inheritDoc}
     * @throws NullPointerException if the specified key is null
     */
    public byte[] floorKey(byte[] keyBytes) {
        Node n = findNear(keyBytes, LT | EQ, comparator);
        return (n == null) ? null : comparator.bytes(n.key);
    }

    /**
     * Returns a key-value mapping associated with the least key
     * greater than or equal to the given key, or {@code null} if
     * there is no such entry. The returned entry does <em>not</em>
     * support the {@code Entry.setValue} method.
     *
     * @throws ClassCastException {@inheritDoc}
     * @throws NullPointerException if the specified key is null
     */
    public Map.Entry<byte[], byte[]> ceilingEntry(byte[] keyBytes) {
        return getNear(keyBytes, GT | EQ);
    }

    /**
     * @throws ClassCastException {@inheritDoc}
     * @throws NullPointerException if the specified key is null
     */
    public byte[] ceilingKey(byte[] keyBytes) {
        Node n = findNear(keyBytes, GT | EQ, comparator);
        return (n == null) ? null : comparator.bytes(n.key);
    }

    /**
     * Returns a key-value mapping associated with the least key
     * strictly greater than the given key, or {@code null} if there
     * is no such key. The returned entry does <em>not</em> support
     * the {@code Entry.setValue} method.
     *
     * @param key the key
     * @throws ClassCastException {@inheritDoc}
     * @throws NullPointerException if the specified key is null
     */
    public Map.Entry<byte[], byte[]> higherEntry(byte[] keyBytes) {
        return getNear(keyBytes, GT);
    }

    /**
     * @param key the key
     * @throws ClassCastException {@inheritDoc}
     * @throws NullPointerException if the specified key is null
     */
    public byte[] higherKey(byte[] keyBytes) {
        Node n = findNear(keyBytes, GT, comparator);
        return (n == null) ? null : comparator.bytes(n.key);
    }

    /**
     * Returns a key-value mapping associated with the least
     * key in this map, or {@code null} if the map is empty.
     * The returned entry does <em>not</em> support
     * the {@code Entry.setValue} method.
     */
    public Map.Entry<byte[], byte[]> firstEntry() {
        for (;;) {
            Node n = findFirst();
            if (n == null) {
                return null;
            }
            AbstractMap.SimpleImmutableEntry e = n.createSnapshot(comparator);
            if (e != null) {
                return e;
            }
        }
    }

    /**
     * Returns a key-value mapping associated with the greatest
     * key in this map, or {@code null} if the map is empty.
     * The returned entry does <em>not</em> support
     * the {@code Entry.setValue} method.
     */
    public Map.Entry<byte[], byte[]> lastEntry() {
        for (;;) {
            Node n = findLast();
            if (n == null) {
                return null;
            }
            AbstractMap.SimpleImmutableEntry e = n.createSnapshot(comparator);
            if (e != null) {
                return e;
            }
        }
    }

    /**
     * Removes and returns a key-value mapping associated with
     * the least key in this map, or {@code null} if the map is empty.
     * The returned entry does <em>not</em> support
     * the {@code Entry.setValue} method.
     */
    @Override
    public Map.Entry<byte[], byte[]> pollFirstEntry() {
        return doRemoveFirstEntry();
    }

    /**
     * Removes and returns a key-value mapping associated with
     * the greatest key in this map, or {@code null} if the map is empty.
     * The returned entry does <em>not</em> support
     * the {@code Entry.setValue} method.
     */
    @Override
    public Map.Entry<byte[], byte[]> pollLastEntry() {
        return doRemoveLastEntry();
    }


    /* ---------------- Iterators -------------- */
    /**
     * Base of iterator classes:
     */
    abstract class Iter<T> implements Iterator<T> {

        /** the last node returned by next() */
        Node lastReturned;
        /** the next node to return from next(); */
        Node next;
        /** Cache of next value field to maintain weak consistency */
        byte[] nextValue;

        /** Initializes ascending iterator for entire range. */
        Iter() {
            while ((next = findFirst()) != null) {
                long x = next.value;
                if (x != NIL && x != SELF) {
                    nextValue = comparator.bytes(x);
                    break;
                }
            }
        }

        public final boolean hasNext() {
            return next != null;
        }

        /** Advances next to higher entry. */
        final void advance() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            lastReturned = next;
            while ((next = next.next) != null) {
                long x = next.value;
                if (x != NIL && x != SELF) {
                    nextValue = comparator.bytes(x);
                    break;
                }
            }
        }

        public void remove() {
            Node l = lastReturned;
            if (l == null) {
                throw new IllegalStateException();
            }
            // It would not be worth all of the overhead to directly
            // unlink from here. Using remove is fast enough.
            LABConcurrentSkipListMap.this.remove(comparator.bytes(l.key));
            lastReturned = null;
        }

    }

    final class FreeIterator extends Iter<byte[]> {

        @Override
        public byte[] next() {
            Node n = next;
            advance();
            byte[] freeing = comparator.bytes(n.key);
            comparator.free(n.key);
            comparator.free(n.value);
            return freeing;
        }
    }

    final class ValueIterator extends Iter<byte[]> {

        public byte[] next() {
            byte[] v = nextValue;
            advance();
            return v;
        }
    }

    final class KeyIterator extends Iter<byte[]> {

        public byte[] next() {
            Node n = next;
            advance();
            return comparator.bytes(n.key);
        }
    }

    final class EntryIterator extends Iter<Map.Entry<byte[], byte[]>> {

        public Map.Entry<byte[], byte[]> next() {
            Node n = next;
            byte[] v = nextValue;
            advance();
            return new AbstractMap.SimpleImmutableEntry<byte[], byte[]>(comparator.bytes(n.key), v);
        }
    }

    // Factory methods for iterators needed by ConcurrentSkipListSet etc
    Iterator<byte[]> keyIterator() {
        return new KeyIterator();
    }

    Iterator<byte[]> valueIterator() {
        return new ValueIterator();
    }

    Iterator<Map.Entry<byte[], byte[]>> entryIterator() {
        return new EntryIterator();
    }

    /* ---------------- View Classes -------------- */

 /*
     * View classes are static, delegating to a ConcurrentNavigableMap
     * to allow use by SubMaps, which outweighs the ugliness of
     * needing type-tests for Iterator methods.
     */
    static final <E> List<E> toList(Collection<E> c) {
        // Using size() here would be a pessimization.
        ArrayList<E> list = new ArrayList<>();
        for (E e : c) {
            list.add(e);
        }
        return list;
    }

    static final class KeySet extends AbstractSet<byte[]> implements NavigableSet<byte[]> {

        final ConcurrentNavigableMap<byte[], ?> m;

        KeySet(ConcurrentNavigableMap<byte[], ?> map) {
            m = map;
        }

        @Override
        public int size() {
            return m.size();
        }

        @Override
        public boolean isEmpty() {
            return m.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return m.containsKey(o);
        }

        @Override
        public boolean remove(Object o) {
            return m.remove(o) != null;
        }

        @Override
        public void clear() {
            m.clear();
        }

        @Override
        public byte[] lower(byte[] e) {
            return m.lowerKey(e);
        }

        @Override
        public byte[] floor(byte[] e) {
            return m.floorKey(e);
        }

        public byte[] ceiling(byte[] e) {
            return m.ceilingKey(e);
        }

        public byte[] higher(byte[] e) {
            return m.higherKey(e);
        }

        public Comparator<? super byte[]> comparator() {
            return m.comparator();
        }

        public byte[] first() {
            return m.firstKey();
        }

        public byte[] last() {
            return m.lastKey();
        }

        public byte[] pollFirst() {
            Map.Entry<byte[], ?> e = m.pollFirstEntry();
            return (e == null) ? null : e.getKey();
        }

        public byte[] pollLast() {
            Map.Entry<byte[], ?> e = m.pollLastEntry();
            return (e == null) ? null : e.getKey();
        }

        @SuppressWarnings("unchecked")
        public Iterator<byte[]> iterator() {
            if (m instanceof LABConcurrentSkipListMap) {
                return ((LABConcurrentSkipListMap) m).keyIterator();
            } else {
                return ((LABConcurrentSkipListMap.SubMap) m).keyIterator();
            }
        }

        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (!(o instanceof Set)) {
                return false;
            }
            Collection<?> c = (Collection<?>) o;
            try {
                return containsAll(c) && c.containsAll(this);
            } catch (ClassCastException unused) {
                return false;
            } catch (NullPointerException unused) {
                return false;
            }
        }

        public Object[] toArray() {
            return toList(this).toArray();
        }

        public <T> T[] toArray(T[] a) {
            return toList(this).toArray(a);
        }

        public Iterator<byte[]> descendingIterator() {
            return descendingSet().iterator();
        }

        public NavigableSet<byte[]> subSet(byte[] fromElement,
            boolean fromInclusive,
            byte[] toElement,
            boolean toInclusive) {
            return new KeySet(m.subMap(fromElement, fromInclusive,
                toElement, toInclusive));
        }

        public NavigableSet<byte[]> headSet(byte[] toElement, boolean inclusive) {
            return new KeySet(m.headMap(toElement, inclusive));
        }

        public NavigableSet<byte[]> tailSet(byte[] fromElement, boolean inclusive) {
            return new KeySet(m.tailMap(fromElement, inclusive));
        }

        public NavigableSet<byte[]> subSet(byte[] fromElement, byte[] toElement) {
            return subSet(fromElement, true, toElement, false);
        }

        public NavigableSet<byte[]> headSet(byte[] toElement) {
            return headSet(toElement, false);
        }

        public NavigableSet<byte[]> tailSet(byte[] fromElement) {
            return tailSet(fromElement, true);
        }

        public NavigableSet<byte[]> descendingSet() {
            return new KeySet(m.descendingMap());
        }

        @SuppressWarnings("unchecked")
        public Spliterator<byte[]> spliterator() {
            if (m instanceof LABConcurrentSkipListMap) {
                return ((LABConcurrentSkipListMap) m).keySpliterator();
            } else {
                return (Spliterator<byte[]>) ((SubMap) m).keyIterator();
            }
        }
    }

    static final class Values extends AbstractCollection<byte[]> {

        final ConcurrentNavigableMap<?, byte[]> m;

        Values(ConcurrentNavigableMap<?, byte[]> map) {
            m = map;
        }

        @SuppressWarnings("unchecked")
        public Iterator<byte[]> iterator() {
            if (m instanceof LABConcurrentSkipListMap) {
                return ((LABConcurrentSkipListMap) m).valueIterator();
            } else {
                return ((SubMap) m).valueIterator();
            }
        }

        public boolean isEmpty() {
            return m.isEmpty();
        }

        public int size() {
            return m.size();
        }

        public boolean contains(Object o) {
            return m.containsValue(o);
        }

        public void clear() {
            m.clear();
        }

        public Object[] toArray() {
            return toList(this).toArray();
        }

        public <T> T[] toArray(T[] a) {
            return toList(this).toArray(a);
        }

        @SuppressWarnings("unchecked")
        public Spliterator<byte[]> spliterator() {
            if (m instanceof LABConcurrentSkipListMap) {
                return ((LABConcurrentSkipListMap) m).valueSpliterator();
            } else {
                return (Spliterator<byte[]>) ((SubMap) m).valueIterator();
            }
        }
    }

    static final class EntrySet extends AbstractSet<Map.Entry<byte[], byte[]>> {

        final ConcurrentNavigableMap<byte[], byte[]> m;

        EntrySet(ConcurrentNavigableMap<byte[], byte[]> map) {
            m = map;
        }

        @SuppressWarnings("unchecked")
        public Iterator<Map.Entry<byte[], byte[]>> iterator() {
            if (m instanceof LABConcurrentSkipListMap) {
                return ((LABConcurrentSkipListMap) m).entryIterator();
            } else {
                return ((SubMap) m).entryIterator();
            }
        }

        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            byte[] v = m.get(e.getKey());
            return v != null && v.equals(e.getValue());
        }

        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            return m.remove(e.getKey(),
                e.getValue());
        }

        public boolean isEmpty() {
            return m.isEmpty();
        }

        public int size() {
            return m.size();
        }

        public void clear() {
            m.clear();
        }

        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (!(o instanceof Set)) {
                return false;
            }
            Collection<?> c = (Collection<?>) o;
            try {
                return containsAll(c) && c.containsAll(this);
            } catch (ClassCastException unused) {
                return false;
            } catch (NullPointerException unused) {
                return false;
            }
        }

        public Object[] toArray() {
            return toList(this).toArray();
        }

        public <T> T[] toArray(T[] a) {
            return toList(this).toArray(a);
        }

        @SuppressWarnings("unchecked")
        public Spliterator<Map.Entry<byte[], byte[]>> spliterator() {
            if (m instanceof LABConcurrentSkipListMap) {
                return ((LABConcurrentSkipListMap) m).entrySpliterator();
            } else {
                return (Spliterator<Map.Entry<byte[], byte[]>>) ((SubMap) m).entryIterator();
            }
        }
    }

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
    static final class SubMap extends AbstractMap<byte[], byte[]>
        implements ConcurrentNavigableMap<byte[], byte[]>, Cloneable, Serializable {

        private static final long serialVersionUID = -7647078645895051609L;

        /** Underlying map */
        private final LABConcurrentSkipListMap m;
        /** lower bound key, or null if from start */
        private final byte[] lo;
        /** upper bound key, or null if to end */
        private final byte[] hi;
        /** inclusion flag for lo */
        private final boolean loInclusive;
        /** inclusion flag for hi */
        private final boolean hiInclusive;
        /** direction */
        private final boolean isDescending;

        // Lazily initialized view holders
        private transient KeySet keySetView;
        private transient Set<Map.Entry<byte[], byte[]>> entrySetView;
        private transient Collection<byte[]> valuesView;

        /**
         * Creates a new submap, initializing all fields.
         */
        SubMap(LABConcurrentSkipListMap map,
            byte[] fromKey, boolean fromInclusive,
            byte[] toKey, boolean toInclusive,
            boolean isDescending) {
            Comparator<? super Long> cmp = map.comparator;
            if (fromKey != null && toKey != null) {
                if (map.comparator.bytesCompartor().compare(fromKey, toKey) > 0) {
                    throw new IllegalArgumentException("inconsistent range");
                }
            }
            this.m = map;
            this.lo = fromKey;
            this.hi = toKey;
            this.loInclusive = fromInclusive;
            this.hiInclusive = toInclusive;
            this.isDescending = isDescending;
        }

        /* ----------------  Utilities -------------- */
        boolean tooLow(byte[] key, LABIndexableMemory cmp) {
            int c;

            if (lo != null) {
                return ((c = cprbb(cmp, key, lo)) < 0 || (c == 0 && !loInclusive));

            } else {
                return false;
            }

        }

        boolean tooHigh(byte[] key, LABIndexableMemory cmp) {
            int c;
            if (hi != null) {
                return ((c = cprbb(cmp, key, hi)) > 0 || (c == 0 && !hiInclusive));
            } else {
                return false;
            }

        }

        boolean inBounds(byte[] key, LABIndexableMemory cmp) {
            return !tooLow(key, cmp) && !tooHigh(key, cmp);
        }

        void checkKeyBounds(byte[] keyBytes, LABIndexableMemory cmp) {
            if (keyBytes == null) {
                throw new NullPointerException();
            }
            if (!inBounds(keyBytes, cmp)) {
                throw new IllegalArgumentException("key out of range");
            }
        }

        /**
         * Returns true if node key is less than upper bound of range.
         */
        boolean isBeforeEnd(LABConcurrentSkipListMap.Node n,
            LABIndexableMemory cmp) {
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
            int c = cprlb(cmp, k, hi);
            if (c > 0 || (c == 0 && !hiInclusive)) {
                return false;
            }
            return true;

        }

        /**
         * Returns lowest node. This node might not be in range, so
         * most usages need to check bounds.
         */
        LABConcurrentSkipListMap.Node loNode(LABIndexableMemory cmp) {
            if (lo == null) {
                return m.findFirst();
            } else if (loInclusive) {
                return m.findNear(lo, GT | EQ, cmp);

            } else {
                return m.findNear(lo, GT, cmp);
            }
        }

        /**
         * Returns highest node. This node might not be in range, so
         * most usages need to check bounds.
         */
        LABConcurrentSkipListMap.Node hiNode(LABIndexableMemory cmp) {
            if (hi == null) {
                return m.findLast();
            } else if (hiInclusive) {
                return m.findNear(hi, LT | EQ, cmp);

            } else {
                return m.findNear(hi, LT, cmp);

            }
        }

        /**
         * Returns lowest absolute key (ignoring directonality).
         */
        byte[] lowestKey() {
            LABConcurrentSkipListMap.Node n = loNode(m.comparator);
            if (isBeforeEnd(n, m.comparator)) {
                return m.comparator.bytes(n.key);
            } else {
                throw new NoSuchElementException();
            }
        }

        /**
         * Returns highest absolute key (ignoring directonality).
         */
        byte[] highestKey() {
            LABConcurrentSkipListMap.Node n = hiNode(m.comparator);
            if (n != null) {
                byte[] last = m.comparator.bytes(n.key);
                if (inBounds(last, m.comparator)) {
                    return last;
                }
            }
            throw new NoSuchElementException();
        }

        Map.Entry<byte[], byte[]> lowestEntry() {
            for (;;) {
                LABConcurrentSkipListMap.Node n = loNode(m.comparator);
                if (!isBeforeEnd(n, m.comparator)) {
                    return null;
                }
                Map.Entry<byte[], byte[]> e = n.createSnapshot(m.comparator);
                if (e != null) {
                    return e;
                }
            }
        }

        Map.Entry<byte[], byte[]> highestEntry() {
            for (;;) {
                LABConcurrentSkipListMap.Node n = hiNode(m.comparator);
                if (n == null || !inBounds(m.comparator.bytes(n.key), m.comparator)) {
                    return null;
                }
                Map.Entry<byte[], byte[]> e = n.createSnapshot(m.comparator);
                if (e != null) {
                    return e;
                }
            }
        }

        Map.Entry<byte[], byte[]> removeLowest() {
            for (;;) {
                Node n = loNode(m.comparator);
                if (n == null) {
                    return null;
                }
                byte[] k = m.comparator.bytes(n.key);
                if (!inBounds(k, m.comparator)) {
                    return null;
                }
                byte[] v = m.doRemove(k, null);
                if (v != null) {
                    return new AbstractMap.SimpleImmutableEntry<byte[], byte[]>(k, v);
                }
            }
        }

        Map.Entry<byte[], byte[]> removeHighest() {
            for (;;) {
                Node n = hiNode(m.comparator);
                if (n == null) {
                    return null;
                }
                byte[] k = m.comparator.bytes(n.key);
                if (!inBounds(k, m.comparator)) {
                    return null;
                }
                byte[] v = m.doRemove(k, null);
                if (v != null) {
                    return new AbstractMap.SimpleImmutableEntry<byte[], byte[]>(k, v);
                }
            }
        }

        /**
         * Submap version of LABConcurrentSkipListMap.getNearEntry
         */
        Map.Entry<byte[], byte[]> getNearEntry(byte[] key, int rel) {
            if (isDescending) { // adjust relation for direction
                if ((rel & LT) == 0) {
                    rel |= LT;
                } else {
                    rel &= ~LT;
                }
            }
            if (tooLow(key, m.comparator)) {
                return ((rel & LT) != 0) ? null : lowestEntry();
            }
            if (tooHigh(key, m.comparator)) {
                return ((rel & LT) != 0) ? highestEntry() : null;
            }
            for (;;) {
                Node n = m.findNear(key, rel, m.comparator);
                if (n == null || !inBounds(m.comparator.bytes(n.key), m.comparator)) {
                    return null;
                }
                long k = n.key;
                byte[] v = n.getValidValue(m.comparator);
                if (v != null) {
                    return new AbstractMap.SimpleImmutableEntry<byte[], byte[]>(m.comparator.bytes(k), v);
                }
            }
        }

        // Almost the same as getNearEntry, except for keys
        byte[] getNearKey(byte[] key, int rel) {
            if (isDescending) { // adjust relation for direction
                if ((rel & LT) == 0) {
                    rel |= LT;
                } else {
                    rel &= ~LT;
                }
            }
            if (tooLow(key, m.comparator)) {
                if ((rel & LT) == 0) {
                    LABConcurrentSkipListMap.Node n = loNode(m.comparator);
                    if (isBeforeEnd(n, m.comparator)) {
                        return m.comparator.bytes(n.key);
                    }
                }
                return null;
            }
            if (tooHigh(key, m.comparator)) {
                if ((rel & LT) != 0) {
                    LABConcurrentSkipListMap.Node n = hiNode(m.comparator);
                    if (n != null) {
                        byte[] last = m.comparator.bytes(n.key);
                        if (inBounds(last, m.comparator)) {
                            return last;
                        }
                    }
                }
                return null;
            }
            for (;;) {
                Node n = m.findNear(key, rel, m.comparator);
                if (n == null || !inBounds(m.comparator.bytes(n.key), m.comparator)) {
                    return null;
                }
                long k = n.key;
                byte[] v = n.getValidValue(m.comparator);
                if (v != null) {
                    return m.comparator.bytes(k);
                }
            }
        }

        /* ----------------  Map API methods -------------- */
        public boolean containsKey(Object keyBytes) {
            if (keyBytes == null) {
                throw new NullPointerException();
            }
            return inBounds((byte[]) keyBytes, m.comparator) && m.containsKey(keyBytes);
        }

        public byte[] get(Object keyBytes) {
            if (keyBytes == null) {
                throw new NullPointerException();
            }
            return (!inBounds((byte[]) keyBytes, m.comparator)) ? null : m.get(keyBytes);

        }

        public byte[] put(byte[] key, byte[] value) {
            checkKeyBounds(key, m.comparator);
            return m.put(key, value);
        }

        public byte[] remove(Object keyBytes) {
            return (!inBounds((byte[]) keyBytes, m.comparator)) ? null : m.remove(keyBytes);

        }

        public int size() {
            long count = 0;
            for (LABConcurrentSkipListMap.Node n = loNode(m.comparator);
                isBeforeEnd(n, m.comparator);
                n = n.next) {
                if (n.getValidValue(m.comparator) != null) {
                    ++count;
                }
            }
            return count >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) count;
        }

        public boolean isEmpty() {
            return !isBeforeEnd(loNode(m.comparator), m.comparator);
        }

        public boolean containsValue(Object value) {
            if (value == null) {
                throw new NullPointerException();
            }
            for (LABConcurrentSkipListMap.Node n = loNode(m.comparator);
                isBeforeEnd(n, m.comparator);
                n = n.next) {
                byte[] v = n.getValidValue(m.comparator);
                if (v != null && Arrays.equals((byte[]) value, v)) {
                    return true;
                }
            }
            return false;
        }

        public void clear() {
            for (LABConcurrentSkipListMap.Node n = loNode(m.comparator);
                isBeforeEnd(n, m.comparator);
                n = n.next) {
                if (n.getValidValue(m.comparator) != null) {
                    m.remove(n.key);
                }
            }
        }

        /* ----------------  ConcurrentMap API methods -------------- */
        public byte[] putIfAbsent(byte[] key, byte[] value) {
            checkKeyBounds(key, m.comparator);
            return m.putIfAbsent(key, value);
        }

        public boolean remove(Object keyBytes, Object value) {
            return inBounds((byte[]) keyBytes, m.comparator) && m.remove(keyBytes, value);
        }

        public boolean replace(byte[] key, byte[] oldValue, byte[] newValue) {
            checkKeyBounds(key, m.comparator);
            return m.replace(key, oldValue, newValue);
        }

        public byte[] replace(byte[] key, byte[] value) {
            checkKeyBounds(key, m.comparator);
            return m.replace(key, value);
        }

        /* ----------------  SortedMap API methods -------------- */
        public Comparator<? super byte[]> comparator() {
            Comparator<? super byte[]> cmp = m.comparator();
            if (isDescending) {
                return Collections.reverseOrder(cmp);
            } else {
                return cmp;
            }
        }

        /**
         * Utility to create submaps, where given bounds override
         * unbounded(null) ones and/or are checked against bounded ones.
         */
        SubMap newSubMap(byte[] fromKey, boolean fromInclusive,
            byte[] toKey, boolean toInclusive) {

            if (isDescending) { // flip senses
                byte[] tk = fromKey;
                fromKey = toKey;
                toKey = tk;
                boolean ti = fromInclusive;
                fromInclusive = toInclusive;
                toInclusive = ti;
            }
            if (lo != null) {
                if (fromKey == null) {
                    fromKey = lo;
                    fromInclusive = loInclusive;
                } else {
                    int c = cprbb(m.comparator, fromKey, lo);
                    if (c < 0 || (c == 0 && !loInclusive && fromInclusive)) {
                        throw new IllegalArgumentException("key out of range");
                    }
                }
            }
            if (hi != null) {
                if (toKey == null) {
                    toKey = hi;
                    toInclusive = hiInclusive;
                } else {
                    int c = cprbb(m.comparator, toKey, hi);
                    if (c > 0 || (c == 0 && !hiInclusive && toInclusive)) {
                        throw new IllegalArgumentException("key out of range");
                    }
                }
            }
            return new SubMap(m, fromKey, fromInclusive,
                toKey, toInclusive, isDescending);

        }

        public SubMap subMap(byte[] fromKey, boolean fromInclusive,
            byte[] toKey, boolean toInclusive) {
            if (fromKey == null || toKey == null) {
                throw new NullPointerException();
            }
            return newSubMap(fromKey, fromInclusive, toKey, toInclusive);
        }

        public SubMap headMap(byte[] toKey, boolean inclusive) {
            if (toKey == null) {
                throw new NullPointerException();
            }
            return newSubMap(null, false, toKey, inclusive);
        }

        public SubMap tailMap(byte[] fromKey, boolean inclusive) {
            if (fromKey == null) {
                throw new NullPointerException();
            }
            return newSubMap(fromKey, inclusive, null, false);
        }

        public SubMap subMap(byte[] fromKey, byte[] toKey) {
            return subMap(fromKey, true, toKey, false);
        }

        public SubMap headMap(byte[] toKey) {
            return headMap(toKey, false);
        }

        public SubMap tailMap(byte[] fromKey) {
            return tailMap(fromKey, true);
        }

        public SubMap descendingMap() {
            return new SubMap(m, lo, loInclusive,
                hi, hiInclusive, !isDescending);
        }

        /* ----------------  Relational methods -------------- */
        public Map.Entry<byte[], byte[]> ceilingEntry(byte[] keyBytes) {
            return getNearEntry(keyBytes, GT | EQ);

        }

        public byte[] ceilingKey(byte[] keyBytes) {
            return getNearKey(keyBytes, GT | EQ);
        }

        public Map.Entry<byte[], byte[]> lowerEntry(byte[] keyBytes) {
            return getNearEntry(keyBytes, LT);
        }

        public byte[] lowerKey(byte[] keyBytes) {
            return getNearKey(keyBytes, LT);
        }

        public Map.Entry<byte[], byte[]> floorEntry(byte[] keyBytes) {
            return getNearEntry(keyBytes, LT | EQ);
        }

        public byte[] floorKey(byte[] keyBytes) {
            return getNearKey(keyBytes, LT | EQ);
        }

        public Map.Entry<byte[], byte[]> higherEntry(byte[] keyBytes) {
            return getNearEntry(keyBytes, GT);

        }

        public byte[] higherKey(byte[] keyBytes) {
            return getNearKey(keyBytes, GT);
        }

        public byte[] firstKey() {
            return isDescending ? highestKey() : lowestKey();
        }

        public byte[] lastKey() {
            return isDescending ? lowestKey() : highestKey();
        }

        public Map.Entry<byte[], byte[]> firstEntry() {
            return isDescending ? highestEntry() : lowestEntry();
        }

        public Map.Entry<byte[], byte[]> lastEntry() {
            return isDescending ? lowestEntry() : highestEntry();
        }

        public Map.Entry<byte[], byte[]> pollFirstEntry() {
            return isDescending ? removeHighest() : removeLowest();
        }

        public Map.Entry<byte[], byte[]> pollLastEntry() {
            return isDescending ? removeLowest() : removeHighest();
        }

        /* ---------------- Submap Views -------------- */
        public NavigableSet<byte[]> keySet() {
            KeySet ks = keySetView;
            return (ks != null) ? ks : (keySetView = new KeySet(this));
        }

        public NavigableSet<byte[]> navigableKeySet() {
            KeySet ks = keySetView;
            return (ks != null) ? ks : (keySetView = new KeySet(this));
        }

        public Collection<byte[]> values() {
            Collection<byte[]> vs = valuesView;
            return (vs != null) ? vs : (valuesView = new Values(this));
        }

        public Set<Map.Entry<byte[], byte[]>> entrySet() {
            Set<Map.Entry<byte[], byte[]>> es = entrySetView;
            return (es != null) ? es : (entrySetView = new EntrySet(this));
        }

        public NavigableSet<byte[]> descendingKeySet() {
            return descendingMap().navigableKeySet();
        }

        Iterator<byte[]> keyIterator() {
            return new SubMapKeyIterator();
        }

        Iterator<byte[]> valueIterator() {
            return new SubMapValueIterator();
        }

        Iterator<Map.Entry<byte[], byte[]>> entryIterator() {
            return new SubMapEntryIterator();
        }

        /**
         * Variant of main Iter class to traverse through submaps.
         * Also serves as back-up Spliterator for views
         */
        abstract class SubMapIter<T> implements Iterator<T>, Spliterator<T> {

            /** the last node returned by next() */
            Node lastReturned;
            /** the next node to return from next(); */
            Node next;
            /** Cache of next value field to maintain weak consistency */
            byte[] nextValue;

            SubMapIter() {
                for (;;) {
                    next = isDescending ? hiNode(m.comparator) : loNode(m.comparator);
                    if (next == null) {
                        break;
                    }
                    long x = next.value;
                    if (x != NIL && x != SELF) {
                        if (!inBounds(m.comparator.bytes(next.key), m.comparator)) {
                            next = null;
                        } else {
                            nextValue = m.comparator.bytes(x);
                        }
                        break;
                    }
                }
            }

            public final boolean hasNext() {
                return next != null;
            }

            final void advance() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                lastReturned = next;
                if (isDescending) {
                    descend();
                } else {
                    ascend();
                }
            }

            private void ascend() {
                for (;;) {
                    next = next.next;
                    if (next == null) {
                        break;
                    }
                    long x = next.value;
                    if (x != NIL && x != SELF) {
                        if (tooHigh(m.comparator.bytes(next.key), m.comparator)) {
                            next = null;
                        } else {
                            nextValue = m.comparator.bytes(x);
                        }
                        break;
                    }
                }
            }

            private void descend() {
                for (;;) {
                    next = m.findNear(m.comparator.bytes(lastReturned.key), LT, m.comparator);
                    if (next == null) {
                        break;
                    }
                    long x = next.value;
                    if (x != NIL && x != SELF) {
                        if (tooLow(m.comparator.bytes(next.key), m.comparator)) {
                            next = null;
                        } else {
                            nextValue = m.comparator.bytes(x);
                        }
                        break;
                    }
                }
            }

            public void remove() {
                Node l = lastReturned;
                if (l == null) {
                    throw new IllegalStateException();
                }
                m.remove(m.comparator.bytes(l.key));
                lastReturned = null;
            }

            public Spliterator<T> trySplit() {
                return null;
            }

            public boolean tryAdvance(Consumer<? super T> action) {
                if (hasNext()) {
                    action.accept(next());
                    return true;
                }
                return false;
            }

            public void forEachRemaining(Consumer<? super T> action) {
                while (hasNext()) {
                    action.accept(next());
                }
            }

            public long estimateSize() {
                return Long.MAX_VALUE;
            }

        }

        final class SubMapValueIterator extends SubMapIter<byte[]> {

            public byte[] next() {
                byte[] v = nextValue;
                advance();
                return v;
            }

            public int characteristics() {
                return 0;
            }
        }

        final class SubMapKeyIterator extends SubMapIter<byte[]> {

            public byte[] next() {
                Node n = next;
                advance();
                return m.comparator.bytes(n.key);
            }

            public int characteristics() {
                return Spliterator.DISTINCT | Spliterator.ORDERED
                    | Spliterator.SORTED;
            }

            public final Comparator<? super byte[]> getComparator() {
                return SubMap.this.comparator();
            }
        }

        final class SubMapEntryIterator extends SubMapIter<Map.Entry<byte[], byte[]>> {

            public Map.Entry<byte[], byte[]> next() {
                Node n = next;
                byte[] v = nextValue;
                advance();
                return new AbstractMap.SimpleImmutableEntry<byte[], byte[]>(m.comparator.bytes(n.key), v);
            }

            public int characteristics() {
                return Spliterator.DISTINCT;
            }
        }
    }

    // default Map method overrides
    public void forEach(BiConsumer<? super byte[], ? super byte[]> action) {
        if (action == null) {
            throw new NullPointerException();
        }
        byte[] v;
        for (Node n = findFirst(); n != null; n = n.next) {
            if ((v = n.getValidValue(comparator)) != null) {
                action.accept(comparator.bytes(n.key), v);
            }
        }
    }

    public void replaceAll(BiFunction<? super byte[], ? super byte[], ? extends byte[]> function) {
        if (function == null) {
            throw new NullPointerException();
        }
        byte[] value;
        for (Node n = findFirst(); n != null; n = n.next) {
            while ((value = n.getValidValue(comparator)) != null) {
                byte[] r = function.apply(comparator.bytes(n.key), value);
                if (r == null) {
                    throw new NullPointerException();
                }
                long rid = comparator.allocate(r);
                long v = n.value;
                if (n.casValue(v, rid)) {
                    comparator.free(v);
                    break;
                } else {
                    comparator.free(rid);
                }
            }
        }
    }

    /**
     * Base class providing common structure for Spliterators.
     * (Although not all that much common functionality; as usual for
     * view classes, details annoyingly vary in key, value, and entry
     * subclasses in ways that are not worth abstracting out for
     * internal classes.)
     *
     * The basic split strategy is to recursively descend from top
     * level, row by row, descending to next row when either split
     * off, or the end of row is encountered. Control of the number of
     * splits relies on some statistical estimation: The expected
     * remaining number of elements of a skip list when advancing
     * either across or down decreases by about 25%. To make this
     * observation useful, we need to know initial size, which we
     * don't. But we can just use Integer.MAX_VALUE so that we
     * don't prematurely zero out while splitting.
     */
    abstract static class CSLMSpliterator {

        final LABIndexableMemory comparator;
        final Long fence;     // exclusive upper bound for keys, or null if to end
        Index row;    // the level to split out
        Node current; // current traversal node; initialize at origin
        int est;           // pseudo-size estimate

        CSLMSpliterator(LABIndexableMemory comparator,
            Index row,
            Node origin,
            Long fence,
            int est) {
            this.comparator = comparator;
            this.row = row;
            this.current = origin;
            this.fence = fence;
            this.est = est;
        }

        public final long estimateSize() {
            return est;
        }
    }

    static final class KeySpliterator extends CSLMSpliterator implements Spliterator<byte[]> {

        KeySpliterator(LABIndexableMemory comparator, Index row,
            Node origin, long fence, int est) {
            super(comparator, row, origin, fence, est);
        }

        public Spliterator<byte[]> trySplit() {
            Node e;
            long ek;
            LABIndexableMemory cmp = comparator;
            long f = fence;
            if ((e = current) != null && (ek = e.key) != NIL) {
                for (Index q = row; q != null; q = row = q.down) {
                    Index s;
                    Node b, n;
                    long sk;
                    if ((s = q.right) != null && (b = s.node) != null
                        && (n = b.next) != null && n.value != NIL
                        && (sk = n.key) != NIL && cprll(cmp, sk, ek) > 0
                        && (f == NIL || cprll(cmp, sk, f) < 0)) {
                        current = n;
                        Index r = q.down;
                        row = (s.right != null) ? s : s.down;
                        est -= est >>> 2;
                        return new KeySpliterator(cmp, r, e, sk, est);
                    }
                }
            }
            return null;
        }

        public void forEachRemaining(Consumer<? super byte[]> action) {
            if (action == null) {
                throw new NullPointerException();
            }
            long f = fence;
            Node e = current;
            current = null;
            for (; e != null; e = e.next) {
                long k;
                long v;
                if ((k = e.key) != NIL && f != NIL && cprll(comparator, f, k) <= 0) {
                    break;
                }
                if ((v = e.value) != NIL && v != SELF) {
                    action.accept(comparator.bytes(k));
                }
            }
        }

        public boolean tryAdvance(Consumer<? super byte[]> action) {
            if (action == null) {
                throw new NullPointerException();
            }
            long f = fence;
            Node e = current;
            for (; e != null; e = e.next) {
                long k;
                long v;
                if ((k = e.key) != NIL && f != NIL && cprll(comparator, f, k) <= 0) {
                    e = null;
                    break;
                }
                if ((v = e.value) != NIL && v != SELF) {
                    current = e.next;
                    action.accept(comparator.bytes(k));
                    return true;
                }
            }
            current = e;
            return false;
        }

        public int characteristics() {
            return Spliterator.DISTINCT | Spliterator.SORTED
                | Spliterator.ORDERED | Spliterator.CONCURRENT
                | Spliterator.NONNULL;
        }

        public final Comparator<? super byte[]> getComparator() {
            return comparator.bytesCompartor();
        }
    }

    // factory method for KeySpliterator
    final KeySpliterator keySpliterator() {
        LABIndexableMemory cmp = comparator;
        for (;;) { // ensure h corresponds to origin p
            HeadIndex h;
            Node p;
            Node b = (h = head).node;
            if ((p = b.next) == null || p.value != NIL) {
                return new KeySpliterator(cmp, h, p, NIL, (p == null)
                    ? 0 : Integer.MAX_VALUE);
            }
            p.helpDelete(b, p.next);
        }
    }

    static final class ValueSpliterator extends CSLMSpliterator
        implements Spliterator<byte[]> {

        ValueSpliterator(LABIndexableMemory comparator, Index row,
            Node origin, long fence, int est) {
            super(comparator, row, origin, fence, est);
        }

        public Spliterator<byte[]> trySplit() {
            Node e;
            long ek;
            LABIndexableMemory cmp = comparator;
            long f = fence;
            if ((e = current) != null && (ek = e.key) != NIL) {
                for (Index q = row; q != null; q = row = q.down) {
                    Index s;
                    Node b, n;
                    long sk;
                    if ((s = q.right) != null && (b = s.node) != null
                        && (n = b.next) != null && n.value != NIL
                        && (sk = n.key) != NIL && cprll(cmp, sk, ek) > 0
                        && (f == NIL || cprll(cmp, sk, f) < 0)) {
                        current = n;
                        Index r = q.down;
                        row = (s.right != null) ? s : s.down;
                        est -= est >>> 2;
                        return new ValueSpliterator(cmp, r, e, sk, est);
                    }
                }
            }
            return null;
        }

        public void forEachRemaining(Consumer<? super byte[]> action) {
            if (action == null) {
                throw new NullPointerException();
            }
            long f = fence;
            Node e = current;
            current = null;
            for (; e != null; e = e.next) {
                long k;
                long v;
                if ((k = e.key) != NIL && f != NIL && cprll(comparator, f, k) <= 0) {
                    break;
                }
                if ((v = e.value) != NIL && v != SELF) {
                    action.accept(comparator.bytes(v));
                }
            }
        }

        public boolean tryAdvance(Consumer<? super byte[]> action) {
            if (action == null) {
                throw new NullPointerException();
            }
            long f = fence;
            Node e = current;
            for (; e != null; e = e.next) {
                long k;
                long v;
                if ((k = e.key) != NIL && f != NIL && cprll(comparator, f, k) <= 0) {
                    e = null;
                    break;
                }
                if ((v = e.value) != NIL && v != SELF) {
                    current = e.next;
                    action.accept(comparator.bytes(v));
                    return true;
                }
            }
            current = e;
            return false;
        }

        public int characteristics() {
            return Spliterator.CONCURRENT | Spliterator.ORDERED
                | Spliterator.NONNULL;
        }
    }

    // Almost the same as keySpliterator()
    final ValueSpliterator valueSpliterator() {
        LABIndexableMemory cmp = comparator;
        for (;;) {
            HeadIndex h;
            Node p;
            Node b = (h = head).node;
            if ((p = b.next) == null || p.value != NIL) {
                return new ValueSpliterator(cmp, h, p, NIL, (p == null)
                    ? 0 : Integer.MAX_VALUE);
            }
            p.helpDelete(b, p.next);
        }
    }

    static final class EntrySpliterator extends LABConcurrentSkipListMap.CSLMSpliterator
        implements Spliterator<Map.Entry<byte[], byte[]>> {

        EntrySpliterator(LABIndexableMemory comparator, LABConcurrentSkipListMap.Index row,
            LABConcurrentSkipListMap.Node origin, long fence, int est) {
            super(comparator, row, origin, fence, est);
        }

        public Spliterator<Map.Entry<byte[], byte[]>> trySplit() {
            Node e;
            long ek;
            LABIndexableMemory cmp = comparator;
            long f = fence;
            if ((e = current) != null && (ek = e.key) != NIL) {
                for (Index q = row; q != null; q = row = q.down) {
                    Index s;
                    Node b, n;
                    long sk;
                    if ((s = q.right) != null && (b = s.node) != null
                        && (n = b.next) != null && n.value != NIL
                        && (sk = n.key) != NIL && cprll(cmp, sk, ek) > 0
                        && (f == NIL || cprll(cmp, sk, f) < 0)) {
                        current = n;
                        Index r = q.down;
                        row = (s.right != null) ? s : s.down;
                        est -= est >>> 2;
                        return new EntrySpliterator(cmp, r, e, sk, est);
                    }
                }
            }
            return null;
        }

        public void forEachRemaining(Consumer<? super Map.Entry<byte[], byte[]>> action) {
            if (action == null) {
                throw new NullPointerException();
            }
            long f = fence;
            Node e = current;
            current = null;
            for (; e != null; e = e.next) {
                long k;
                long v;
                if ((k = e.key) != NIL && f != NIL && cprll(comparator, f, k) <= 0) {
                    break;
                }
                if ((v = e.value) != NIL && v != SELF) {
                    action.accept(new AbstractMap.SimpleImmutableEntry<>(comparator.bytes(k), comparator.bytes(v)));
                }
            }
        }

        public boolean tryAdvance(Consumer<? super Map.Entry<byte[], byte[]>> action) {
            if (action == null) {
                throw new NullPointerException();
            }
            long f = fence;
            Node e = current;
            for (; e != null; e = e.next) {
                long k;
                long v;
                if ((k = e.key) != NIL && f != NIL && cprll(comparator, f, k) <= 0) {
                    e = null;
                    break;
                }
                if ((v = e.value) != NIL && v != SELF) {
                    current = e.next;
                    action.accept(new AbstractMap.SimpleImmutableEntry<>(comparator.bytes(k), comparator.bytes(v)));
                    return true;
                }
            }
            current = e;
            return false;
        }

        public int characteristics() {
            return Spliterator.DISTINCT | Spliterator.SORTED
                | Spliterator.ORDERED | Spliterator.CONCURRENT
                | Spliterator.NONNULL;
        }

        public final Comparator<Map.Entry<byte[], byte[]>> getComparator() {
            return Map.Entry.comparingByKey(comparator.bytesCompartor());
        }
    }

    // Almost the same as keySpliterator()
    final EntrySpliterator entrySpliterator() {
        LABIndexableMemory cmp = comparator;
        for (;;) { // almost same as key version
            HeadIndex h;
            Node p;
            Node b = (h = head).node;
            if ((p = b.next) == null || p.value != NIL) {
                return new EntrySpliterator(cmp, h, p, NIL, (p == null)
                    ? 0 : Integer.MAX_VALUE);
            }
            p.helpDelete(b, p.next);
        }
    }

}
