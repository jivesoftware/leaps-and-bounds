package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public class RawEntryFormat {

    public static final RawEntryFormat MEMORY = new RawEntryFormat(0, 0);

    final long keyFormat;
    final long valueFormat;

    public RawEntryFormat(long keyFormat, long valueFormat) {
        this.keyFormat = keyFormat;
        this.valueFormat = valueFormat;
    }

    public long getKeyFormat() {
        return keyFormat;
    }

    public long getValueFormat() {
        return valueFormat;
    }

    @Override
    public String toString() {
        return "RawEntryFormat{" + "keyFormat=" + keyFormat + ", valueFormat=" + valueFormat + '}';
    }

}
