package org.apache.commons.collections4.trie;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LexKeyAnalyzerNGTest {

    @Test
    public void testIsPrefix() {

        int sizeInBits = 8;
        LexKeyAnalyzer analyzer = new LexKeyAnalyzer();
        Assert.assertTrue(analyzer.isPrefix(new LexKey(new byte[]{1, 2, 3}), 0, 3 * sizeInBits, new LexKey(new byte[]{1, 2, 3, 4, 5})));
        Assert.assertFalse(analyzer.isPrefix(new LexKey(new byte[]{1, 2, 3}), 0, 3 * sizeInBits, new LexKey(new byte[]{1, 2})));
        Assert.assertTrue(analyzer.isPrefix(new LexKey(new byte[]{1, 2, 3}), sizeInBits, 2 * sizeInBits, new LexKey(new byte[]{2, 3, 4, 5})));

        Balls b = new Balls();
        sizeInBits = 16;
        Assert.assertTrue(b.isPrefix("123", 0, 3 * sizeInBits, "12345"));
        Assert.assertFalse(b.isPrefix("123", 0, 3 * sizeInBits, "12"));
        Assert.assertTrue(b.isPrefix("123", sizeInBits, 2 * sizeInBits, "2345"));

    }

}
