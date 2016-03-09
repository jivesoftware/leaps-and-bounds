package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public class LABIndexCorruptedException extends Exception {

    public LABIndexCorruptedException() {
    }

    public LABIndexCorruptedException(String message) {
        super(message);
    }

    public LABIndexCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
