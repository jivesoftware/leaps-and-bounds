package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface FormatTransformer {

    public static final FormatTransformer NO_OP = new FormatTransformer() {
        @Override
        public byte[] transform(byte[] bytes) {
            return bytes;
        }

        @Override
        public byte[][] transform(byte[][] bytes) {
            return bytes;
        }
    };

    byte[] transform(byte[] bytes);

    byte[][] transform(byte[][] bytes);
}
