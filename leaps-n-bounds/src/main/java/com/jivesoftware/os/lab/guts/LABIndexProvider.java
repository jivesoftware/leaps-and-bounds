package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.rawhide.Rawhide;

/**
 *
 * @author jonathan.colt
 */
public interface LABIndexProvider {

    LABIndex create(Rawhide rawhide, int poweredUpToHint) throws Exception;
}
