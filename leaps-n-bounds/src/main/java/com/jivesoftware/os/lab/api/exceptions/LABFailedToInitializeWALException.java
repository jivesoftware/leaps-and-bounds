package com.jivesoftware.os.lab.api.exceptions;

/**
 *
 * @author jonathan.colt
 */
public class LABFailedToInitializeWALException extends Exception {

    public LABFailedToInitializeWALException(String string, Exception x) {
        super(string, x);
    }

}
