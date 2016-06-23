package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface FormatTransformer {

    public static final FormatTransformer NO_OP = new FormatTransformer() {
        @Override
        public byte[] transform(byte[] key) throws Exception {
            return key;
        }

        @Override
        public byte[][] transform(byte[][] keys) throws Exception {
            return keys;
        }
    };

    byte[] transform(byte[] key) throws Exception;

    byte[][] transform(byte[][] keys) throws Exception;
}
