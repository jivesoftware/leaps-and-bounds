package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface FormatTransformerProvider {

    public static final FormatTransformerProvider NO_OP = new FormatTransformerProvider() {
        @Override
        public FormatTransformer read(long format) throws Exception {
            return FormatTransformer.NO_OP;
        }

        @Override
        public FormatTransformer write(long format) throws Exception {
            return FormatTransformer.NO_OP;
        }
    };

    FormatTransformer read(long format) throws Exception;

    FormatTransformer write(long format) throws Exception;

}
