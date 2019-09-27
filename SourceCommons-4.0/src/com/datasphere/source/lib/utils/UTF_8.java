package com.datasphere.source.lib.utils;

import java.io.*;

class UTF_8 extends StandardCharset
{
    @Override
    public boolean skipBOM(final InputStream in) throws IOException {
        final byte[] bom = new byte[3];
        in.read(bom, 0, bom.length);
        return bom[0] == -17 && bom[1] == -69 && bom[2] == -65;
    }
}
