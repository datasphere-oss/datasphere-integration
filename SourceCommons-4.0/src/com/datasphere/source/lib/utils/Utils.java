package com.datasphere.source.lib.utils;

import org.apache.log4j.*;

import java.nio.*;
import java.net.*;
import com.datasphere.uuid.*;
import com.datasphere.source.lib.constant.*;
import com.datasphere.security.*;
import com.datasphere.metaRepository.*;
import com.datasphere.runtime.meta.*;

public class Utils
{
    static String intToString;
    static byte[] stringToBytes;
    static int stringLength;
    static byte[] arrayOfString;
    static int stringToInt;
    private static Logger logger;
    
    public static byte[] convertIntegerToStringBytes(final int integer) {
        Utils.intToString = Integer.toString(integer);
        final int byteArrayLength = (Utils.intToString + Constant.STRING_LENGTH_SENTINEL).getBytes().length;
        Utils.stringToBytes = new byte[byteArrayLength];
        return Utils.stringToBytes = (Utils.intToString + Constant.STRING_LENGTH_SENTINEL).getBytes();
    }
    
    public static int convertStringBytesToInteger(final byte[] byteArray) {
        final int arrayLength = byteArray.length;
        int strLen = 0;
        for (int i = 0; i < arrayLength; ++i) {
            if (byteArray[i] == Constant.STRING_LENGTH_SENTINEL) {
                Utils.stringLength = strLen;
                break;
            }
            ++strLen;
        }
        System.arraycopy(byteArray, 0, Utils.arrayOfString = new byte[Utils.stringLength], 0, Utils.stringLength);
        final String byteToString = new String(Utils.arrayOfString);
        return Utils.stringToInt = Integer.parseInt(byteToString);
    }
    
    public static int getSizeLength() {
        return Utils.stringLength + 1;
    }
    
    public static short convertToShort(final ByteBuffer buf) {
        final byte byte0 = buf.get();
        final byte byte2 = buf.get();
        int iLow;
        int iHigh;
        if (isBIGEndian()) {
            iLow = byte2;
            iHigh = byte0;
        }
        else {
            iLow = byte0;
            iHigh = byte2;
        }
        final int retval = iHigh << 8 | (0xFF & iLow);
        return (short)retval;
    }
    
    public static String convertToString(final byte[] rowdata, final int offset, final int length) {
        final byte[] destination = new byte[length];
        System.arraycopy(rowdata, offset, destination, 0, length);
        return new String(destination);
    }
    
    public static int convertToInteger(final byte[] rowdata, final int offset, final int length) {
        if (rowdata.length < offset + 4) {
            return -1;
        }
        long accum = 0L;
        int i = 0;
        for (int shiftBy = 0; shiftBy < 32; shiftBy += 8) {
            accum |= (rowdata[offset + i] & 0xFF) << shiftBy;
            ++i;
        }
        return (int)accum;
    }
    
    public static double convertToDouble(final byte[] rowdata, final int offset, final int length) {
        long accum = 0L;
        int i = 0;
        for (int shiftBy = 0; shiftBy < 64; shiftBy += 8) {
            accum |= (rowdata[offset + i] & 0xFF) << shiftBy;
            ++i;
        }
        final double retval = Double.longBitsToDouble(accum);
        return retval;
    }
    
    static boolean isBIGEndian() {
        final ByteOrder b = ByteOrder.nativeOrder();
        return b.equals(ByteOrder.BIG_ENDIAN);
    }
    
    public static boolean isBufferEmpty(final Buffer buffer) {
        return buffer.position() == 0 && buffer.limit() == 0;
    }
    
    public static String getDBType(final String dbURL) {
        String schema = "";
        try {
            URI jURI;
            if (dbURL.startsWith("jdbc:")) {
                jURI = new URI(dbURL.substring(5));
            }
            else {
                jURI = new URI(dbURL);
            }
            schema = jURI.getScheme();
        }
        catch (URISyntaxException e) {
            final String errMsg = "Invalid URL {" + dbURL + "}";
            Utils.logger.error((Object)errMsg, (Throwable)e);
        }
        return (schema != null) ? schema : "";
    }
    
    public static String getFullyQualifiedIdentifier(final UUID uuid) {
        String targetId = null;
        final MetadataRepository metadataRepository = MetadataRepository.getINSTANCE();
        try {
            final MetaInfo.MetaObject object = metadataRepository.getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
            targetId = object.getFullName();
        }
        catch (MetaDataRepositoryException e) {
            Utils.logger.error((Object)("Exception while trying to get application name of {" + uuid.toString() + "}"));
        }
        return targetId;
    }
    
    static {
        Utils.intToString = null;
        Utils.stringToBytes = null;
        Utils.stringLength = 0;
        Utils.arrayOfString = null;
        Utils.logger = Logger.getLogger((Class)Utils.class);
    }
}
