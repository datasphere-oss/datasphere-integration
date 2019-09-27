package com.datasphere.source.lib.reader;

class UTF32BEBom extends BOM
{
    public UTF32BEBom() {
        final byte[] tmp = { 0, 0, -2, -1 };
        this.pattern = tmp;
    }
}
