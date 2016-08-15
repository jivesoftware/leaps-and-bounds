package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public class NoOpFormatTransformerProvider implements FormatTransformerProvider {

    public static String NAME = "noOpFormatTransformerProvider";

    public static NoOpFormatTransformerProvider NO_OP = new NoOpFormatTransformerProvider();

    private NoOpFormatTransformerProvider() {
    }

    @Override
    public FormatTransformer read(long format) throws Exception {
        return FormatTransformer.NO_OP;
    }

    @Override
    public FormatTransformer write(long format) throws Exception {
        return FormatTransformer.NO_OP;
    }
}
