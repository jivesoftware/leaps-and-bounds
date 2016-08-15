package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public class MemoryRawEntryFormat extends RawEntryFormat {

    public static String NAME = "memoryRawEntryFormat";
    public static MemoryRawEntryFormat SINGLETON = new MemoryRawEntryFormat();

    private MemoryRawEntryFormat() {
        super(0, 0);
    }

}
