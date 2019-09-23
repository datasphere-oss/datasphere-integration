package com.datasphere.runtime;

import com.datasphere.runtime.dataObfuscation.*;
import com.datasphere.event.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;

public class DataObfuscationLayer
{
    public static final String EMAILADDRESS = "EMAILADDRESS";
    public static final String SSN = "SSN";
    public static final String PHONENUMBER = "PHONENUMBER";
    public static final String CREDITCARDNUMBER = "CREDITCARDNUMBER";
    public static final String OTHER = "OTHER";
    public static final String ANONYMIZE_PARTIALLY = "ANONYMIZE_PARTIALLY";
    public static final String ANONYMIZE_COMPLETELY = "ANONYMIZE_COMPLETELY";
    public static final String PARTIALLY = "Partially";
    public static final String COMPLETELY = "Completely";
    public static final String MASK = "mask";
    public static final String CHILDREN = "children";
    public static final String ID = "id";
    public static final String TEXT = "text";
    public static final String DESCRIPTION = "description";
    public static final String MASKING_FUNCTION = "maskingFunction";
    public static final String PARAMS = "params";
    
    public static String other_func(final MaskFunctionType functionType, final String value) {
        final String name = functionType.name();
        switch (name) {
            case "ANONYMIZE_PARTIALLY": {
                return null;
            }
            case "ANONYMIZE_COMPLETELY": {
                return null;
            }
            default: {
                return null;
            }
        }
    }
    
    public static String phone_func(final MaskFunctionType functionType, final String value) {
        final String name = functionType.name();
        switch (name) {
            case "ANONYMIZE_PARTIALLY": {
                return PhoneNumberObfuscation.maskPhoneNumberPartially(value);
            }
            case "ANONYMIZE_COMPLETELY": {
                return PhoneNumberObfuscation.maskPhoneNumberComplete(value);
            }
            default: {
                return null;
            }
        }
    }
    
    public static String phone_func(final String mask, final String value) {
        return PhoneNumberObfuscation.maskPhoneNumberBasedOnPattern(value, mask);
    }
    
    public static String phone_func_regex(final String phoneNumber, final String regex, final int groupNumber) {
        return PhoneNumberObfuscation.maskPhoneNumberBasedOnRegex(phoneNumber, regex, groupNumber);
    }
    
    public static String phone_func_regex(final String phoneNumber, final String regex, final Object... groupNumbers) {
        return PhoneNumberObfuscation.maskPhoneNumberBasedOnRegex(phoneNumber, regex, groupNumbers);
    }
    
    public static String ssn_func(final MaskFunctionType functionType, final String value) {
        final String name = functionType.name();
        switch (name) {
            case "ANONYMIZE_PARTIALLY": {
                return SSNObfuscation.maskSSNPartially(value);
            }
            case "ANONYMIZE_COMPLETELY": {
                return SSNObfuscation.maskSSNCompletely(value);
            }
            default: {
                return null;
            }
        }
    }
    
    public static String ssn_func(final String mask, final String value) {
        return SSNObfuscation.maskSSNBasedOnPattern(value, mask);
    }
    
    public static String email_func(final MaskFunctionType functionType, final String value) {
        final String name = functionType.name();
        switch (name) {
            case "ANONYMIZE_PARTIALLY": {
                return EmailAddressObfuscation.maskEmailAddressExcludingDomain(value);
            }
            case "ANONYMIZE_COMPLETELY": {
                return EmailAddressObfuscation.maskEmailAddressIncludingDomain(value);
            }
            default: {
                return null;
            }
        }
    }
    
    public static String email_func(final String mask, final String value) {
        return EmailAddressObfuscation.maskEmailAddressBasedOnPattern(value, mask);
    }
    
    public static String creditCard_func(final MaskFunctionType functionType, final String value) {
        final String name = functionType.name();
        switch (name) {
            case "ANONYMIZE_PARTIALLY": {
                return CreditCardObfuscation.maskCreditCardNumberPartially(value);
            }
            case "ANONYMIZE_COMPLETELY": {
                return CreditCardObfuscation.maskCreditCardNumberCompletely(value);
            }
            default: {
                return null;
            }
        }
    }
    
    public static String creditCard_func(final String mask, final String value) {
        return CreditCardObfuscation.maskCreditCardNumberBasedOnPattern(value, mask);
    }
    
    public static String generic_func(final MaskFunctionType functionType, final String value) {
        final String name = functionType.name();
        switch (name) {
            case "ANONYMIZE_PARTIALLY": {
                return NumericDataObfuscation.maskNumberPartially(value);
            }
            case "ANONYMIZE_COMPLETELY": {
                return NumericDataObfuscation.maskNumberCompletely(value);
            }
            default: {
                return NumericDataObfuscation.maskNumber(value, functionType.name());
            }
        }
    }
    
    public static String generic_func(final String mask, final String value) {
        return NumericDataObfuscation.maskNumber(value, mask);
    }
    
    public static String generic_func(final String value, final String regex, final Object... groupNumbers) {
        return PhoneNumberObfuscation.maskPhoneNumberBasedOnRegex(value, regex, groupNumbers);
    }
    
    public static ArrayNode getMaskingTemplate() {
        final ArrayNode result = ObjectMapperFactory.getInstance().createArrayNode();
        result.add((JsonNode)prepareCreditCardObject());
        result.add((JsonNode)preparePhoneNumberObject());
        result.add((JsonNode)prepareEmailAddressObject());
        result.add((JsonNode)prepareSSNObject());
        return result;
    }
    
    private static ObjectNode prepareCreditCardObject() {
        final ArrayNode arrayNode = ObjectMapperFactory.getInstance().createArrayNode();
        ObjectNode obj = ObjectMapperFactory.getInstance().createObjectNode();
        obj.put("id", "maskCreditCardNumber_partially");
        obj.put("text", "Partially");
        obj.put("description", "XXXXXXXXX1234");
        obj.put("maskingFunction", "maskCreditCardNumber");
        obj.put("params", "ANONYMIZE_PARTIALLY");
        arrayNode.add((JsonNode)obj);
        obj = ObjectMapperFactory.getInstance().createObjectNode();
        obj.put("id", "maskCreditCardNumber_completely");
        obj.put("text", "Completely");
        obj.put("description", "XXXXXXXXXXXXX");
        obj.put("maskingFunction", "maskCreditCardNumber");
        obj.put("params", "ANONYMIZE_COMPLETELY");
        arrayNode.add((JsonNode)obj);
        obj = ObjectMapperFactory.getInstance().createObjectNode();
        obj.put("text", "CreditCard");
        obj.put("children", (JsonNode)arrayNode);
        return obj;
    }
    
    private static ObjectNode preparePhoneNumberObject() {
        final ArrayNode arrayNode = ObjectMapperFactory.getInstance().createArrayNode();
        final ObjectNode areaCodeOnly = ObjectMapperFactory.getInstance().createObjectNode();
        areaCodeOnly.put("id", "maskPhoneNumber_area_code_only");
        areaCodeOnly.put("text", "Partially");
        areaCodeOnly.put("description", "(xxx) xxx 4567");
        areaCodeOnly.put("maskingFunction", "maskPhoneNumber");
        areaCodeOnly.put("params", "ANONYMIZE_PARTIALLY");
        arrayNode.add((JsonNode)areaCodeOnly);
        final ObjectNode grouppedDigits = ObjectMapperFactory.getInstance().createObjectNode();
        grouppedDigits.put("id", "maskPhoneNumber_first_digit_group");
        grouppedDigits.put("text", "Partially (Regex)");
        grouppedDigits.put("description", "43xxxxxxxxx");
        grouppedDigits.put("maskingFunction", "maskPhoneNumber");
        final ArrayNode grouppedDigitsParams = ObjectMapperFactory.getInstance().createArrayNode();
        grouppedDigitsParams.add("(\\d{0,4}\\s)(\\d{0,4}\\s)([0-9 ]+)");
        grouppedDigitsParams.add("1");
        grouppedDigits.put("params", (JsonNode)grouppedDigitsParams);
        arrayNode.add((JsonNode)grouppedDigits);
        final ObjectNode onlyDigits = ObjectMapperFactory.getInstance().createObjectNode();
        onlyDigits.put("id", "maskPhoneNumber_digits_only");
        onlyDigits.put("text", "Partially (Regex)");
        onlyDigits.put("description", "011434xxxxxxxxxxx");
        onlyDigits.put("maskingFunction", "maskPhoneNumber");
        final ArrayNode onlyDigitsParams = ObjectMapperFactory.getInstance().createArrayNode();
        onlyDigitsParams.add("(^\\s{0,2}\\d{6})(\\d{0,15})");
        onlyDigitsParams.add("1");
        onlyDigits.put("params", (JsonNode)onlyDigitsParams);
        arrayNode.add((JsonNode)onlyDigits);
        final ObjectNode completely = ObjectMapperFactory.getInstance().createObjectNode();
        completely.put("id", "maskPhoneNumber_completely");
        completely.put("text", "Completely");
        completely.put("description", "XXXXXXXXXX");
        completely.put("maskingFunction", "maskPhoneNumber");
        completely.put("params", "ANONYMIZE_COMPLETELY");
        arrayNode.add((JsonNode)completely);
        final ObjectNode phoneNumber = ObjectMapperFactory.getInstance().createObjectNode();
        phoneNumber.put("text", "PhoneNumber");
        phoneNumber.put("children", (JsonNode)arrayNode);
        return phoneNumber;
    }
    
    private static ObjectNode prepareEmailAddressObject() {
        final ArrayNode arrayNode = ObjectMapperFactory.getInstance().createArrayNode();
        ObjectNode obj = ObjectMapperFactory.getInstance().createObjectNode();
        obj.put("id", "maskEmailAddress_partially");
        obj.put("text", "Partially");
        obj.put("description", "axxxx@striim.com");
        obj.put("maskingFunction", "maskEmailAddress");
        obj.put("params", "ANONYMIZE_PARTIALLY");
        arrayNode.add((JsonNode)obj);
        obj = ObjectMapperFactory.getInstance().createObjectNode();
        obj.put("id", "maskEmailAddress_completely");
        obj.put("text", "Completely");
        obj.put("description", "axxx@xxxx");
        obj.put("maskingFunction", "maskEmailAddress");
        obj.put("params", "ANONYMIZE_COMPLETELY");
        arrayNode.add((JsonNode)obj);
        obj = ObjectMapperFactory.getInstance().createObjectNode();
        obj.put("text", "EmailAddress");
        obj.put("children", (JsonNode)arrayNode);
        return obj;
    }
    
    private static ObjectNode prepareSSNObject() {
        final ArrayNode arrayNode = ObjectMapperFactory.getInstance().createArrayNode();
        ObjectNode obj = ObjectMapperFactory.getInstance().createObjectNode();
        obj.put("id", "maskSSN_partially");
        obj.put("text", "Partially");
        obj.put("description", "XXX-XX-3456");
        obj.put("maskingFunction", "maskSSN");
        obj.put("params", "ANONYMIZE_PARTIALLY");
        arrayNode.add((JsonNode)obj);
        obj = ObjectMapperFactory.getInstance().createObjectNode();
        obj.put("id", "maskSSN_completely");
        obj.put("text", "Completely");
        obj.put("description", "XXX-XX-XXXX");
        obj.put("maskingFunction", "maskSSN");
        obj.put("params", "ANONYMIZE_COMPLETELY");
        arrayNode.add((JsonNode)obj);
        obj = ObjectMapperFactory.getInstance().createObjectNode();
        obj.put("text", "SSN");
        obj.put("children", (JsonNode)arrayNode);
        return obj;
    }
    
    public static boolean contains(final String test) {
        for (final MaskFunctionType c : MaskFunctionType.values()) {
            if (c.name().equalsIgnoreCase(test)) {
                return true;
            }
        }
        return false;
    }
    
    public enum MaskDataType
    {
        PHONENUMBER, 
        SSN, 
        EMAILADDRESS, 
        CREDITCARDNUMBER;
    }
    
    public enum MaskFunctionType
    {
        ANONYMIZE_PARTIALLY, 
        ANONYMIZE_COMPLETELY;
    }
}
