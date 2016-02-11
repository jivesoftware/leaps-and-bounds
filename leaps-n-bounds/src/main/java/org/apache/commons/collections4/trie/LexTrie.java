package org.apache.commons.collections4.trie;

/**
 *
 * @author jonathan.colt
 */
public class LexTrie<E> extends AbstractPatriciaTrie<LexKey, E> {


    public LexTrie() {
        super(new LexKeyAnalyzer());
    }
}
