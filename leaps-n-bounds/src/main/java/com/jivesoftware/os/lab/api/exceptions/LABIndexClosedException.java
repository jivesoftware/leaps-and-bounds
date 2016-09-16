package com.jivesoftware.os.lab.api.exceptions;

/**
 *
 * @author jonathan.colt
 */
public class LABIndexClosedException extends Exception {

    public LABIndexClosedException() {
    }

    public LABIndexClosedException(String message) {
        super(message);
    }

    public LABIndexClosedException(String message, Throwable cause) {
        super(message, cause);
    }
}
