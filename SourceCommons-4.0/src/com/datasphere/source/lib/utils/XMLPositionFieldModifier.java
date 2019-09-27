package com.datasphere.source.lib.utils;

import org.apache.commons.codec.binary.Base64;

import com.datasphere.ser.KryoSingleton;
import com.datasphere.recovery.Position;

class XMLPositionFieldModifier extends FieldModifier
{
    @Override
    public String modifyFieldValue(final Object fieldValue, final Object event) {
        final Position position = (Position)fieldValue;
        final byte[] pos = KryoSingleton.write((Object)position, false);
        final String posstring = new String(Base64.encodeBase64(pos));
        final String xml = "<__DataExchangeMetadata>\n" + "<position>" + posstring + "</position>" + "</__DataExchangeMetadata>";
        return xml;
    }
}
