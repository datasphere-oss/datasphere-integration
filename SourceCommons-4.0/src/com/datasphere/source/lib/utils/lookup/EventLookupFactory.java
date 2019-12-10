package com.datasphere.source.lib.utils.lookup;

import java.lang.reflect.*;
import com.datasphere.common.exc.*;
import java.util.*;

public class EventLookupFactory
{
    private static final String METADATA = "metadata";
    private static final String USERDATA = "userdata";
    
    public static EventLookup getEventLookup(final Field[] fields, final String... lookupTokens) throws Exception {
        if (lookupTokens == null || lookupTokens.length <= 0) {
            throw new IllegalArgumentException("Invalid lookup token(s) " + Arrays.toString(lookupTokens) + " is specifed. Please specify a valid value.");
        }
        final String lookupToken = lookupTokens[0];
        if (lookupTokens.length == 1) {
            if (lookupToken.startsWith("@")) {
                return validateAndInitializeMetadataLookup(fields, lookupToken);
            }
            return validateAndInitializeTypedEventLookup(fields, lookupToken);
        }
        else {
            if (lookupToken.startsWith("@")) {
                throw new AdapterException("Looking up metadata or userdata of HDEvent is supported for a single token, specifying for multiple tokens is not supported");
            }
            return validateAndInitializeTypedEventLookup(fields, lookupTokens);
        }
    }
    
    private static EventLookup validateAndInitializeMetadataLookup(final Field[] fields, final String lookupToken) throws Exception {
        final String incomingType = fields[0].getDeclaringClass().getSimpleName();
        if (!incomingType.equalsIgnoreCase("event")) {
            throw new AdapterException("Failure in initializing writer. Metadata/Userdata lookup is supported for stream of type HDEvent and not supported for the type " + incomingType);
        }
        if (!lookupToken.contains("(") || !lookupToken.contains(")")) {
            throw new IllegalArgumentException("Invalid value " + lookupToken + " specified. Valid lookup token is of the @metdata(metadatakey) or @userdata(userdatakey)");
        }
        final String lookupOption = lookupToken.substring(1, lookupToken.indexOf("("));
        if (lookupOption.trim().isEmpty()) {
            throw new IllegalArgumentException("Specified lookup option is emtpy. Valid lookup options are metdata or userdata");
        }
        if (lookupOption.equalsIgnoreCase("metadata")) {
            return new HDEventMetadataLookup(getLookupKey(lookupToken));
        }
        if (lookupOption.equalsIgnoreCase("userdata")) {
            return new HDEventUserdataLookup(getLookupKey(lookupToken));
        }
        throw new IllegalArgumentException("Unsupported lookup option " + lookupOption + " is sepcified. Supported lookup options are metadata and userdata");
    }
    
    private static EventLookup validateAndInitializeTypedEventLookup(final Field[] fields, final String... lookupTokens) throws Exception {
        final String incomingType = fields[0].getDeclaringClass().getSimpleName();
        final Map<String, Field> tokenNameFieldMap = new LinkedHashMap<String, Field>();
        for (final String lookupToken : lookupTokens) {
            final Field associatedField = getAssociatedFieldOfToken(lookupToken, fields);
            if (associatedField == null) {
                throw new IllegalArgumentException("Look up token(s) " + Arrays.toString(lookupTokens) + " has to match a field name in the type " + incomingType + " of this Target Adapter's incoming stream.");
            }
            tokenNameFieldMap.put(lookupToken, associatedField);
        }
        return new TypedEventLookup(tokenNameFieldMap);
    }
    
    private static Field getAssociatedFieldOfToken(final String fieldName, final Field[] fields) {
        for (final Field field : fields) {
            if (field.getName().equalsIgnoreCase(fieldName)) {
                return field;
            }
        }
        return null;
    }
    
    private static String getLookupKey(final String lookupToken) throws Exception {
        final String lookupKey = lookupToken.substring(lookupToken.indexOf(40) + 1, lookupToken.indexOf(41));
        if (lookupKey.trim().isEmpty()) {
            throw new IllegalArgumentException("Keyname to be looked up using metadata or userdata is empty. Please specify a valid value of the form @metdata(tablename) or @userdata(region)");
        }
        return lookupKey;
    }
}
