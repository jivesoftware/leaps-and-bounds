package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.api.MergeRawEntry;
import com.jivesoftware.os.lab.io.api.UIO;

/**
 *
 * @author jonathan.colt
 */
public class LABValueMerger implements MergeRawEntry {

    @Override
    public byte[] merge(byte[] current, byte[] adding) {
        int currentKeyLength = UIO.bytesInt(current);
        int addingKeyLength = UIO.bytesInt(adding);

        long currentsTimestamp = UIO.bytesLong(current, 4 + currentKeyLength);
        long currentsVersion = UIO.bytesLong(current, 4 + currentKeyLength + 8 + 1);

        long addingsTimestamp = UIO.bytesLong(adding, 4 + addingKeyLength);
        long addingsVersion = UIO.bytesLong(adding, 4 + addingKeyLength + 8 + 1);

        return (currentsTimestamp > addingsTimestamp) || (currentsTimestamp == addingsTimestamp && currentsVersion > addingsVersion) ? current : adding;
    }
}
