package org.apache.commons.collections4.trie;

class LexKeyAnalyzer extends KeyAnalyzer<LexKey> {

    public static final LexKeyAnalyzer INSTANCE = new LexKeyAnalyzer();
    /** The number of bits per byte. */
    public static final int LENGTH = 8;
    /** A bit mask where the first bit is 1 and the others are zero. */
    private static final int MSB = 0x80;

    /** Returns a bit mask where the given bit is set. */
    private static int mask(final int bit) {
        return MSB >>> bit;
    }

    @Override
    public int bitsPerElement() {
        return LENGTH;
    }

    @Override
    public int lengthInBits(final LexKey key) {
        return key != null ? key.bytes.length * LENGTH : 0;
    }

    @Override
    public int bitIndex(final LexKey key, final int offsetInBits, final int lengthInBits,
        final LexKey other, final int otherOffsetInBits, final int otherLengthInBits) {

        boolean allNull = true;

        if (offsetInBits % LENGTH != 0 || otherOffsetInBits % LENGTH != 0
            || lengthInBits % LENGTH != 0 || otherLengthInBits % LENGTH != 0) {
            throw new IllegalArgumentException("The offsets and lengths must be at Character boundaries");
        }

        final int beginIndex1 = offsetInBits / LENGTH;
        final int beginIndex2 = otherOffsetInBits / LENGTH;

        final int endIndex1 = beginIndex1 + lengthInBits / LENGTH;
        final int endIndex2 = beginIndex2 + otherLengthInBits / LENGTH;

        final int length = Math.max(endIndex1, endIndex2);

        // Look at each character, and if they're different
        // then figure out which bit makes the difference
        // and return it.
        byte k = 0, f = 0;
        for (int i = 0; i < length; i++) {
            final int index1 = beginIndex1 + i;
            final int index2 = beginIndex2 + i;

            if (index1 >= endIndex1) {
                k = 0;
            } else {
                k = key.bytes[index1];
            }

            if (other == null || index2 >= endIndex2) {
                f = 0;
            } else {
                f = other.bytes[index2];
            }

            if (k != f) {
                final int x = k ^ f;
                return i * LENGTH + Integer.numberOfLeadingZeros(x) - LENGTH;
            }

            if (k != 0) {
                allNull = false;
            }
        }

        // All bits are 0
        if (allNull) {
            return KeyAnalyzer.NULL_BIT_KEY;
        }

        // Both keys are equal
        return KeyAnalyzer.EQUAL_BIT_KEY;
    }

    @Override
    public boolean isBitSet(final LexKey key, final int bitIndex, final int lengthInBits) {
        if (key == null || bitIndex >= lengthInBits) {
            return false;
        }

        final int index = bitIndex / LENGTH;
        final int bit = bitIndex % LENGTH;

        return (key.bytes[index] & mask(bit)) != 0;
    }

    @Override
    public boolean isPrefix(final LexKey prefix, final int offsetInBits,
        final int lengthInBits, final LexKey key) {
        if (offsetInBits % LENGTH != 0 || lengthInBits % LENGTH != 0) {
            throw new IllegalArgumentException(
                "Cannot determine prefix outside of Character boundaries");
        }
        return startsWith(key, prefix, offsetInBits / LENGTH, lengthInBits / LENGTH);
    }

    private static boolean startsWith(LexKey source, LexKey prefix, int offset, int length) {
        int to = 0;
        if (source.bytes.length < length) {
            return false;
        }
        while (--length >= 0) {
            if (source.bytes[to++] != prefix.bytes[offset++]) {
                return false;
            }
        }
        return true;
    }

}
