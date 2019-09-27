package com.datasphere.source.lib.reader;

class UTF16BEBom extends BOM
{
    public UTF16BEBom() {
        final byte[] tmp = { -2, -1 };
        this.pattern = tmp;
    }
}
