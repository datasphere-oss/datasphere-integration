package com.datasphere.uuid;

class MACAddressParser
{
    static String parse(final String in) {
        String out = in;
        final int hexStart = out.indexOf("0x");
        if (hexStart != -1 && out.indexOf("ETHER") != -1) {
            final int hexEnd = out.indexOf(32, hexStart);
            if (hexEnd > hexStart + 2) {
                out = out.substring(hexStart, hexEnd);
            }
        }
        else {
            int octets = 0;
            if (out.indexOf(45) > -1) {
                out = out.replace('-', ':');
            }
            int lastIndex = out.lastIndexOf(58);
            if (lastIndex > out.length() - 2) {
                out = null;
            }
            else {
                final int end = Math.min(out.length(), lastIndex + 3);
                ++octets;
                int old = lastIndex;
                while (octets != 5 && lastIndex != -1 && lastIndex > 1) {
                    lastIndex = out.lastIndexOf(58, --lastIndex);
                    if (old - lastIndex == 3 || old - lastIndex == 2) {
                        ++octets;
                        old = lastIndex;
                    }
                }
                if (octets == 5 && lastIndex > 1) {
                    out = out.substring(lastIndex - 2, end).trim();
                }
                else {
                    out = null;
                }
            }
        }
        if (out != null && out.startsWith("0x")) {
            out = out.substring(2);
        }
        return out;
    }
}
