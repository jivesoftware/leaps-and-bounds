package com.jivesoftware.os.lab.guts.allocators;

import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.api.Rawhide;

/**
 *
 * @author jonathan.colt
 */
public interface LABMemoryAllocator {

   boolean acquireBytes(long address, BolBuffer bolBuffer) throws Exception;

    byte[] bytes(long address) throws InterruptedException;

    long allocate(byte[] bytes, int offset, int length) throws Exception;

    void release(long address) throws Exception;

    int compare(Rawhide rawhide, long leftAddress, long rightAddress);

    int compare(Rawhide rawhide, long leftAddress, byte[] right, int rightOffset, int rightLength);

    int compare(Rawhide rawhide, byte[] left, int leftOffset, int leftLength, long rightAddress);

    int compare(Rawhide rawhide, byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength);

    long sizeInBytes();

}
