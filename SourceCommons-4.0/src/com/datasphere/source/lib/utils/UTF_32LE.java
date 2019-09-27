package com.datasphere.source.lib.utils;

import java.io.*;

class UTF_32LE extends StandardCharset
{
    @Override
    public boolean skipBOM(final InputStream in) throws IOException {
        final byte[] bom = new byte[4];
        in.read(bom, 0, bom.length);
        return bom[0] == -1 && bom[1] == -2 && bom[2] == 0 && bom[3] == 0;
    }
}
