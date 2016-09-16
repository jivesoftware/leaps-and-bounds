package com.jivesoftware.os.lab.api;

import com.jivesoftware.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface FormatTransformer {

    public static final FormatTransformer NO_OP = new FormatTransformer() {
        @Override
        public BolBuffer transform(BolBuffer bytes) {
            return bytes;
        }

        @Override
        public BolBuffer[] transform(BolBuffer[] bytes) {
            return bytes;
        }
    };

    BolBuffer transform(BolBuffer bytes);

    BolBuffer[] transform(BolBuffer[] bytes);
}
