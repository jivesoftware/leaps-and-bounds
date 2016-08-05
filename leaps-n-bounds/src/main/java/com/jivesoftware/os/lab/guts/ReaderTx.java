package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.guts.api.ReadIndex;

/**
 *
 * @author jonathan.colt
 */
public interface ReaderTx {

    boolean tx(int index, byte[] fromKey, byte[] toKey, ReadIndex[] readIndexs, boolean hydrateValues) throws Exception;

}
