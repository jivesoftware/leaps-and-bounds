package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.rawhide.Rawhide;

/**
 * @author jonathan.colt
 */
public interface LABIndexProvider<E, B> {

    LABIndex<E, B> create(Rawhide rawhide, int poweredUpToHint) throws Exception;
}
