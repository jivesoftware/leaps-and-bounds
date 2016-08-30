package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.Rawhide;

/**
 *
 * @author jonathan.colt
 */
public interface LABIndexProvider {

    LABIndex create(Rawhide rawhide) throws Exception;
}
