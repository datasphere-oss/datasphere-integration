package com.datasphere.source.lib.reader;

class UTF16LEBom extends BOM
{
    public UTF16LEBom() {
        final byte[] tmp = { -1, -2 };
        this.pattern = tmp;
    }
}
