package com.datasphere.source.lib.reader;

class UTF8Bom extends BOM
{
    public UTF8Bom() {
        final byte[] tmp = { -17, -69, -65 };
        this.pattern = tmp;
    }
}
