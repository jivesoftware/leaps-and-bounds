package com.jivesoftware.os.lab.api;

import java.nio.ByteBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface FormatTransformer {

    public static final FormatTransformer NO_OP = new FormatTransformer() {
        @Override
        public ByteBuffer transform(ByteBuffer bytes) {
            return bytes;
        }

        @Override
        public ByteBuffer[] transform(ByteBuffer[] bytes) {
            return bytes;
        }
    };

    ByteBuffer transform(ByteBuffer bytes);

    ByteBuffer[] transform(ByteBuffer[] bytes);
}
