package com.datasphere.source.lib.reader;

class UTF32LEBom extends BOM
{
    public UTF32LEBom() {
        final byte[] tmp = { -1, -2, 0, 0 };
        this.pattern = tmp;
    }
}
