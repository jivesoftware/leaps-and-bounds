package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.guts.api.ReadIndex;

/**
 *
 * @author jonathan.colt
 */
public interface ReaderTx<R> {

    R tx(ReadIndex[] readIndexs) throws Exception;

}
