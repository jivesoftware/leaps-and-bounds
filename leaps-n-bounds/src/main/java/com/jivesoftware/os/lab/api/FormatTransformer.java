package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface FormatTransformer {

    public static final FormatTransformer NO_OP = new FormatTransformer() {
        @Override
        public byte[] transform(byte[] key) {
            return key;
        }

        @Override
        public byte[][] transform(byte[][] keys) {
            return keys;
        }
    };

    byte[] transform(byte[] key);

    byte[][] transform(byte[][] keys);
}
