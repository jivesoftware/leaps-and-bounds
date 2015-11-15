package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface MergeRawEntry {

    byte[] merge(byte[] current, byte[] adding);
}
