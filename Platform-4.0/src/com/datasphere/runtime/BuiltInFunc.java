package com.datasphere.runtime;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.Period;
import org.joda.time.ReadableInstant;
import org.joda.time.ReadablePartial;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimePrinter;
import org.joda.time.format.ISODateTimeFormat;

import com.datasphere.NSKUtilities.TxnUtilities;
import com.datasphere.common.constants.Constant;
import com.datasphere.event.Event;
import com.datasphere.event.SimpleEvent;
import com.datasphere.exception.SecurityException;
import com.datasphere.geo.GeoSearchCoverTree;
import com.datasphere.geo.Point;
import com.datasphere.geo.Polygon;
import com.datasphere.iplookup.IPLookup2;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.proc.events.AvroEvent;
import com.datasphere.proc.events.JsonNodeEvent;
import com.datasphere.proc.events.OPCUADataChangeEvent;
import com.datasphere.proc.events.StringArrayEvent;
import com.datasphere.proc.records.Record;
import com.datasphere.runtime.compiler.custom.AcceptWildcard;
import com.datasphere.runtime.compiler.custom.AccessPrevEventInBuffer;
import com.datasphere.runtime.compiler.custom.AggHandlerDesc;
import com.datasphere.runtime.compiler.custom.AnyAttrLike;
import com.datasphere.runtime.compiler.custom.CustomFunction;
import com.datasphere.runtime.compiler.custom.DataSetItertorTrans;
import com.datasphere.runtime.compiler.custom.EventListGetter;
import com.datasphere.runtime.compiler.custom.StreamGeneratorDef;
import com.datasphere.runtime.compiler.custom.matchTranslator;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.HeartBeatGenerator;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitorBatchEvent;
import com.datasphere.runtime.monitor.MonitorModel;
import com.datasphere.runtime.utils.DateParser;
import com.datasphere.runtime.utils.RecordUtil;
import com.datasphere.security.HSecurityManager;
import com.datasphere.utility.SnmpPayload;
import com.datasphere.uuid.UUID;
import com.datasphere.hd.HD;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import cern.colt.list.DoubleArrayList;

public abstract class BuiltInFunc
{
    private static Logger logger;
    private static DateTimeFormatter defaultDateTimeFormatter;
    private static Map<String, DateTimeFormatter> dtfs;
    private static Map<String, DateParser> dps;
    static Map<Object, Object> PREVs;
    private static HashMap<UUID, Field[]> typeUUIDCache;
    private static HashMap<String, Utf8> stringToUtf8Mapper;
    static ObjectMapper mapper;
    static Map<String, Pattern> patterns;
    static HashMap<String, HashMap<String, Pattern>> all_patterns;
    static Map<String, Map<String, Integer>> uidSets;
    public static Map<String, GeoSearchCoverTree<Event>> spatialIndices;
    
    public static StreamGeneratorDef heartbeat(final ClassLoader c, final long interval) {
        return new StreamGeneratorDef(HeartBeatGenerator.HeartBeatEvent.uuid, HeartBeatGenerator.HeartBeatEvent.class, new Object[] { interval }, HeartBeatGenerator.class.getName());
    }
    
    public static StreamGeneratorDef heartbeat(final ClassLoader c, final long interval, final int times) {
        return new StreamGeneratorDef(HeartBeatGenerator.HeartBeatEvent.uuid, HeartBeatGenerator.HeartBeatEvent.class, new Object[] { interval, times }, HeartBeatGenerator.class.getName());
    }
    
    public static StreamGeneratorDef heartbeat(final ClassLoader c, final long interval, final int times, final int batchSize) {
        return new StreamGeneratorDef(HeartBeatGenerator.HeartBeatEvent.uuid, HeartBeatGenerator.HeartBeatEvent.class, new Object[] { interval, times, batchSize }, HeartBeatGenerator.class.getName());
    }
    
    public static StreamGeneratorDef heartbeat(final ClassLoader c, final long interval, final int times, final int batchSize, final int seqsize) {
        return new StreamGeneratorDef(HeartBeatGenerator.HeartBeatEvent.uuid, HeartBeatGenerator.HeartBeatEvent.class, new Object[] { interval, times, batchSize, seqsize }, HeartBeatGenerator.class.getName());
    }
    
    public static String maskSSN(final String value, final String functionType) {
        if (functionType == null || value == null) {
            return null;
        }
        if (DataObfuscationLayer.contains(functionType)) {
            final DataObfuscationLayer.MaskFunctionType functionType2 = DataObfuscationLayer.MaskFunctionType.valueOf(functionType);
            return DataObfuscationLayer.ssn_func(functionType2, value);
        }
        return DataObfuscationLayer.ssn_func(functionType, value);
    }
    
    public static String maskPhoneNumber(final String value, final String functionType) {
        if (functionType == null || value == null) {
            return null;
        }
        if (DataObfuscationLayer.contains(functionType)) {
            final DataObfuscationLayer.MaskFunctionType functionType2 = DataObfuscationLayer.MaskFunctionType.valueOf(functionType);
            return DataObfuscationLayer.phone_func(functionType2, value);
        }
        return DataObfuscationLayer.phone_func(functionType, value);
    }
    
    public static String maskPhoneNumber(final String phoneNumber, final String regex, final int groupNumber) {
        if (phoneNumber == null || regex == null || groupNumber <= 0) {
            return null;
        }
        return DataObfuscationLayer.phone_func_regex(phoneNumber, regex, groupNumber);
    }
    
    public static String maskPhoneNumber(final String phoneNumber, final String regex, final Object... groupNumbers) {
        if (phoneNumber == null || regex == null || groupNumbers.length == 0) {
            return null;
        }
        return DataObfuscationLayer.phone_func_regex(phoneNumber, regex, groupNumbers);
    }
    
    public static String maskEmailAddress(final String value, final String functionType) {
        if (functionType == null || value == null) {
            return null;
        }
        final DataObfuscationLayer.MaskFunctionType functionType2 = DataObfuscationLayer.MaskFunctionType.valueOf(functionType);
        return DataObfuscationLayer.email_func(functionType2, value);
    }
    
    public static String maskCreditCardNumber(final String value, final String functionType) {
        if (functionType == null || value == null) {
            return null;
        }
        if (DataObfuscationLayer.contains(functionType)) {
            final DataObfuscationLayer.MaskFunctionType functionType2 = DataObfuscationLayer.MaskFunctionType.valueOf(functionType);
            return DataObfuscationLayer.creditCard_func(functionType2, value);
        }
        return DataObfuscationLayer.creditCard_func(functionType, value);
    }
    
    public static String maskGeneric(final String value, final String functionType) {
        if (functionType == null || value == null) {
            return null;
        }
        final DataObfuscationLayer.MaskFunctionType functionType2 = DataObfuscationLayer.MaskFunctionType.valueOf(functionType);
        return DataObfuscationLayer.generic_func(functionType2, value);
    }
    
    public static String mask(final String value, final String regex, final Object... groupNumbers) {
        if (value == null || regex == null || groupNumbers.length == 0) {
            return null;
        }
        return DataObfuscationLayer.generic_func(value, regex, groupNumbers);
    }
    
    private static DateTimeFormatter makeDefaultFormatter() {
        final DateTimeParser tp = ISODateTimeFormat.timeElementParser().getParser();
        final DateTimeParser otp = new DateTimeFormatterBuilder().appendLiteral(' ').appendOptional(tp).toParser();
        final DateTimeParser[] parsers = { new DateTimeFormatterBuilder().append(ISODateTimeFormat.dateElementParser().getParser()).appendOptional(otp).toParser(), new DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("yyyy/MM/dd")).appendOptional(otp).toParser(), new DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("yyyy-MMM-dd")).appendOptional(otp).toParser(), ISODateTimeFormat.dateOptionalTimeParser().getParser(), tp };
        final DateTimePrinter dp = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss.SSS").getPrinter();
        final DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(dp, parsers).toFormatter();
        return formatter;
    }
    
    public static Object[] CLONE(final Object[] data) {
        return data.clone();
    }
    
    public static Object TO_OBJ(final int size) {
        return size;
    }
    
    public static Object TO_OBJ(final float size) {
        return size;
    }
    
    public static Object TO_OBJ(final boolean size) {
        return size;
    }
    
    public static Object TO_OBJ(final long size) {
        return size;
    }
    
    public static Object TO_OBJ(final double size) {
        return size;
    }
    
    public static Object TO_OBJ(final Object size) {
        return size;
    }
    
    public static DateTime TO_DATE(final Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof DateTime) {
            return (DateTime)obj;
        }
        if (obj instanceof Long) {
            final Long val = (Long)obj;
            return new DateTime((long)val);
        }
        if (obj instanceof Date) {
            final Date val2 = (Date)obj;
            return new DateTime((Object)val2);
        }
        if (obj instanceof LocalDate) {
            return ((LocalDate)obj).toDateTimeAtStartOfDay();
        }
        if (obj instanceof String) {
            final String val3 = (String)obj;
            try {
                return DateTime.parse(val3, BuiltInFunc.defaultDateTimeFormatter);
            }
            catch (IllegalArgumentException iae) {
                final Long l = Long.parseLong(val3);
                return new DateTime((Object)l);
            }
        }
        if (obj instanceof SnmpPayload) {
            return getSnmpDateTime(obj);
        }
        throw new IllegalArgumentException("Passed Parameter of type " + obj.getClass().getName() + " cannot be converted to DateTime");
    }
    
    public static Date TO_DATE_JAVA_DATE(final Object obj) {
        final DateTime dateTime = TO_DATE(obj);
        if (dateTime == null) {
            return null;
        }
        return dateTime.toDate();
    }
    
    public static DateTime TO_DATE(final long val) {
        return new DateTime(val);
    }
    
    public static DateTime TO_DATE(final Object obj, final String format) {
        String value;
        if (obj instanceof String) {
            value = (String)obj;
        }
        else {
            value = obj.toString();
        }
        DateTimeFormatter dtf = BuiltInFunc.dtfs.get(format);
        if (dtf == null) {
            dtf = DateTimeFormat.forPattern(format);
            BuiltInFunc.dtfs.put(format, dtf);
        }
        return dtf.parseDateTime(value);
    }
    
    public static DateTime TO_DATEF(final Object obj, final String format) {
        String value;
        if (obj instanceof String) {
            value = (String)obj;
        }
        else {
            value = obj.toString();
        }
        DateParser dp = BuiltInFunc.dps.get(format);
        if (dp == null) {
            dp = new DateParser(format);
            BuiltInFunc.dps.put(format, dp);
        }
        return dp.parse(value);
    }
    
    public static String TO_STRING(final DateTime value, final String format) {
        DateTimeFormatter dtf = BuiltInFunc.dtfs.get(format);
        if (dtf == null) {
            dtf = DateTimeFormat.forPattern(format);
            BuiltInFunc.dtfs.put(format, dtf);
        }
        return dtf.print((ReadableInstant)value);
    }
    
    public static DateTime DNOW() {
        return DateTime.now();
    }
    
    public static int DYEARS(final Object date) {
        if (date instanceof DateTime) {
            return ((DateTime)date).year().get();
        }
        if (date instanceof LocalDate) {
            return ((LocalDate)date).year().get();
        }
        throw new IllegalArgumentException("Passed Parameter of type " + date.getClass().getName() + " cannot be converted to DateTime or LocalDate");
    }
    
    public static int DMONTHS(final Object date) {
        if (date instanceof DateTime) {
            return ((DateTime)date).monthOfYear().get();
        }
        if (date instanceof LocalDate) {
            return ((LocalDate)date).monthOfYear().get();
        }
        throw new IllegalArgumentException("Passed Parameter of type " + date.getClass().getName() + " cannot be converted to DateTime or LocalDate");
    }
    
    public static int DDAYS(final Object date) {
        if (date instanceof DateTime) {
            return ((DateTime)date).dayOfMonth().get();
        }
        if (date instanceof LocalDate) {
            return ((LocalDate)date).dayOfMonth().get();
        }
        throw new IllegalArgumentException("Passed Parameter of type " + date.getClass().getName() + " cannot be converted to DateTime or LocalDate");
    }
    
    public static int DHOURS(final DateTime date) {
        return date.hourOfDay().get();
    }
    
    public static int DMINS(final DateTime date) {
        return date.minuteOfHour().get();
    }
    
    public static int DSECS(final DateTime date) {
        return date.secondOfMinute().get();
    }
    
    public static int DMILLIS(final DateTime date) {
        return date.getMillisOfSecond();
    }
    
    public static Period DYEARS(final int num) {
        return Period.years(num);
    }
    
    public static Period DMONTHS(final int num) {
        return Period.months(num);
    }
    
    public static Period DDAYS(final int num) {
        return Period.days(num);
    }
    
    public static Period DHOURS(final int num) {
        return Period.hours(num);
    }
    
    public static Period DMINS(final int num) {
        return Period.minutes(num);
    }
    
    public static Period DSECS(final int num) {
        return Period.seconds(num);
    }
    
    public static Period DMILLIS(final int num) {
        return Period.millis(num);
    }
    
    public static boolean DBEFORE(final Object first, final Object second) {
        if (first instanceof DateTime && second instanceof DateTime) {
            return ((DateTime)second).isBefore((ReadableInstant)first);
        }
        if (first instanceof LocalDate && second instanceof LocalDate) {
            return ((LocalDate)second).isBefore((ReadablePartial)second);
        }
        throw new IllegalArgumentException("Passed Parameters of type " + first.getClass().getName() + " / " + second.getClass().getName() + " cannot be converted to DateTime or LocalDate");
    }
    
    public static boolean DAFTER(final Object first, final Object second) {
        if (first instanceof DateTime && second instanceof DateTime) {
            return ((DateTime)second).isAfter((ReadableInstant)first);
        }
        if (first instanceof LocalDate && second instanceof LocalDate) {
            return ((LocalDate)second).isAfter((ReadablePartial)second);
        }
        throw new IllegalArgumentException("Passed Parameters of type " + first.getClass().getName() + " / " + second.getClass().getName() + " cannot be converted to DateTime or LocalDate");
    }
    
    public static boolean DBETWEEN(final Object first, final Object second, final Object third) {
        if (first instanceof DateTime && second instanceof DateTime) {
            return ((DateTime)first).isAfter((ReadableInstant)second) && ((DateTime)first).isBefore((ReadableInstant)third);
        }
        if (first instanceof LocalDate && second instanceof LocalDate) {
            return ((LocalDate)first).isAfter((ReadablePartial)second) && ((LocalDate)first).isBefore((ReadablePartial)third);
        }
        throw new IllegalArgumentException("Passed Parameters of type " + first.getClass().getName() + " / " + second.getClass().getName() + " cannot be converted to DateTime or LocalDate");
    }
    
    public static DateTime DADD(final DateTime date, final Period after) {
        return date.plus((ReadablePeriod)after);
    }
    
    public static LocalDate DADD(final LocalDate date, final ReadablePeriod after) {
        return date.plus(after);
    }
    
    public static DateTime DSUBTRACT(final DateTime date, final Period before) {
        return date.minus((ReadablePeriod)before);
    }
    
    public static LocalDate DSUBTRACT(final LocalDate date, final ReadablePeriod before) {
        return date.minus(before);
    }
    
    public static Period DDIFF(final DateTime first, final DateTime second) {
        final long firstM = first.getMillis();
        final long secondM = second.getMillis();
        final long diff = (firstM > secondM) ? (firstM - secondM) : (secondM - firstM);
        return new Period(diff);
    }
    
    public static long DDIFF_LONG(final DateTime first, final DateTime second) {
        final long firstM = first.getMillis();
        final long secondM = second.getMillis();
        final long diff = (firstM > secondM) ? (firstM - secondM) : (secondM - firstM);
        return diff;
    }
    
    public static int DDIFF(final LocalDate first, final LocalDate second) {
        return Days.daysBetween((ReadablePartial)first, (ReadablePartial)second).getDays();
    }
    
    public static Short TO_SHORT(final Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Number) {
            return ((Number)obj).shortValue();
        }
        if (obj instanceof String) {
            return Short.parseShort((String)obj);
        }
        throw new IllegalArgumentException("Passed Parameter of type " + obj.getClass().getName() + " cannot be converted to short");
    }
    
    public static short TO_SHORT_PV(final Object obj) {
        final Short ss = TO_SHORT(obj);
        if (ss == null) {
            return 0;
        }
        return ss;
    }
    
    public static Integer TO_INT(final Object obj, final Integer radix) {
        if (obj == null || radix == null || radix < 2 || radix > 36) {
            return null;
        }
        if (obj instanceof Number) {
            return ((Number)obj).intValue();
        }
        if (obj instanceof String) {
            return Integer.parseInt((String)obj, radix);
        }
        throw new IllegalArgumentException("Passed Parameter of type " + obj.getClass().getName() + " cannot be converted to int");
    }
    
    public static int TO_INT_PV(final Object obj) {
        final Integer ii = TO_INT(obj);
        if (ii == null) {
            return 0;
        }
        return ii;
    }
    
    public static Integer TO_INT(final Object obj) {
        return TO_INT(obj, 10);
    }
    
    public static Long TO_LONG(final Object obj, final Integer radix) {
        if (obj == null || radix == null || radix < 2 || radix > 36) {
            return null;
        }
        if (obj instanceof Number) {
            return ((Number)obj).longValue();
        }
        if (obj instanceof String) {
            return Long.parseLong((String)obj, radix);
        }
        if (obj instanceof DateTime) {
            return ((DateTime)obj).getMillis();
        }
        if (obj instanceof Period) {
            return (long)((Period)obj).getMillis();
        }
        if (obj instanceof Date) {
            return ((Date)obj).getTime();
        }
        throw new IllegalArgumentException("Passed Parameter of type " + obj.getClass().getName() + " cannot be converted to long");
    }
    
    public static Long TO_LONG(final Object obj) {
        return TO_LONG(obj, 10);
    }
    
    public static long TO_LONG_PV(final Object obj) {
        final Long ll = TO_LONG(obj, 10);
        if (ll == null) {
            return 0L;
        }
        return ll;
    }
    
    public static Float TO_FLOAT(final Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Number) {
            return ((Number)obj).floatValue();
        }
        if (obj instanceof String) {
            return Float.parseFloat((String)obj);
        }
        throw new IllegalArgumentException("Passed Parameter of type " + obj.getClass().getName() + " cannot be converted to float");
    }
    
    public static float TO_FLOAT_PV(final Object obj) {
        final Float ff = TO_FLOAT(obj);
        if (ff == null) {
            return 0.0f;
        }
        return ff;
    }
    
    public static Double TO_DOUBLE(final Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Number) {
            return ((Number)obj).doubleValue();
        }
        if (obj instanceof String) {
            return Double.parseDouble((String)obj);
        }
        throw new IllegalArgumentException("Passed Parameter of type " + obj.getClass().getName() + " cannot be converted to double");
    }
    
    public static double TO_DOUBLE_PV(final Object obj) {
        final Double dd = TO_DOUBLE(obj);
        if (dd == null) {
            return 0.0;
        }
        return dd;
    }
    
    public static double ROUND_DOUBLE(final Object obj, final Object places) {
        final int pos = TO_INT(places);
        if (pos < 0) {
            throw new IllegalArgumentException("Passed Parameter of type " + places.getClass().getName() + " cannot be converted to int");
        }
        final long factor = (long)Math.pow(10.0, pos);
        double d = TO_DOUBLE(obj);
        d = Math.round(d * factor);
        return d / factor;
    }
    
    public static float ROUND_FLOAT(final Object obj, final Object places) {
        final int pos = TO_INT(places);
        if (pos < 0) {
            throw new IllegalArgumentException("Passed Parameter of type " + places.getClass().getName() + " cannot be converted to int");
        }
        final long factor = (long)Math.pow(10.0, pos);
        float f = TO_FLOAT(obj);
        f = Math.round(f * factor);
        return f / factor;
    }
    
    public static Boolean TO_BOOLEAN(final Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Boolean) {
            return (boolean)obj;
        }
        if (obj instanceof String) {
            return Boolean.parseBoolean((String)obj);
        }
        throw new IllegalArgumentException("Passed Parameter of type " + obj.getClass().getName() + " cannot be converted to boolean");
    }
    
    public static Boolean TO_BOOLEAN_PV(final Object obj) {
        final Boolean bool = TO_BOOLEAN(obj);
        if (bool == null) {
            return false;
        }
        return bool;
    }
    
    public static String TO_STRING(final Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof String) {
            return (String)obj;
        }
        if (obj instanceof DateTime) {
            return BuiltInFunc.defaultDateTimeFormatter.print((ReadableInstant)obj);
        }
        if (obj instanceof SnmpPayload) {
            return ((SnmpPayload)obj).getSnmpStringValue();
        }
        return obj.toString();
    }
    
    public static String TO_STRING(final Object o, final String encoding) {
        if (o == null) {
            return null;
        }
        try {
            final String strVal = new String((byte[])o, encoding);
            return strVal;
        }
        catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Parameter of type " + o.getClass().getName() + " cannot be converted to String");
        }
    }
    
    public static byte[] hexToRaw(final String hexString) {
        byte[] buffer = null;
        final char[] hexData = hexString.toCharArray();
        buffer = new byte[hexData.length / 2];
        int byteIdx = 0;
        byte byteVal = 0;
        int intValue = 0;
        for (int itr = 0; itr < hexData.length; ++itr) {
            switch (hexData[itr]) {
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9': {
                    intValue = hexData[itr] - '0';
                    break;
                }
                case 'a':
                case 'b':
                case 'c':
                case 'd':
                case 'e':
                case 'f': {
                    intValue = hexData[itr] - 'a' + '\n';
                    break;
                }
                case 'A':
                case 'B':
                case 'C':
                case 'D':
                case 'E':
                case 'F': {
                    intValue = hexData[itr] - 'A' + '\n';
                    break;
                }
                default: {
                    intValue = hexData[itr];
                    break;
                }
            }
            if (itr % 2 != 0) {
                byteVal <<= 4;
                byteVal |= (byte)(intValue & 0xF);
                buffer[byteIdx++] = byteVal;
            }
            else {
                byteVal = 0;
                byteVal |= (byte)(intValue & 0xF);
            }
        }
        return buffer;
    }
    
    public static char TO_CHAR(final Object obj) {
        char c = '\0';
        if (obj == null) {
            return c;
        }
        try {
            if (obj instanceof String) {
                c = ((String)obj).charAt(0);
            }
            else if (obj instanceof Character) {
                c = (char)obj;
            }
            else {
                c = (char)obj;
            }
        }
        catch (Exception ex) {
            throw new IllegalArgumentException("Parameter of type " + obj.getClass().getName() + " cannot be converted to char");
        }
        return c;
    }
    
    public static byte[] TO_BYTE_ARRAY(final Object obj) {
        byte[] bytes = null;
        if (obj == null) {
            return bytes;
        }
        try {
            if (obj instanceof byte[]) {
                bytes = (byte[])obj;
            }
            else if (obj instanceof String) {
                bytes = ((String)obj).getBytes();
            }
            else {
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                final ObjectOutputStream os = new ObjectOutputStream(out);
                os.writeObject(obj);
                bytes = out.toByteArray();
            }
        }
        catch (IOException iox) {
            throw new IllegalArgumentException("Parameter of type " + obj.getClass().getName() + " cannot be converted to byte array.");
        }
        return bytes;
    }
    
    public static String TO_HEX(final Object o) {
        if (o == null) {
            return null;
        }
        return Hex.encodeHexString((byte[])o);
    }
    
    public static String SRIGHT(final Object obj, final int from) {
        if (obj == null) {
            return null;
        }
        final String val = TO_STRING(obj);
        return val.substring(from);
    }
    
    public static String SLEFT(final Object obj, final int to) {
        if (obj == null) {
            return null;
        }
        final String val = TO_STRING(obj);
        return val.substring(0, to);
    }
    
    public static Double ABS(final Double num) {
        return (num >= 0.0) ? num : (-num);
    }
    
    public static Float ABS(final Float num) {
        return (num >= 0.0) ? num : (-num);
    }
    
    public static Long ABS(final Long num) {
        return (num >= 0L) ? num : (-num);
    }
    
    public static Integer ABS(final Integer num) {
        return (num >= 0) ? num : (-num);
    }
    
    public static Short ABS(final Short num) {
        return (num >= 0) ? ((short)num) : (short)(-num);
    }
    
    public static Object NVL(final Object obj, final Object nvl) {
        return (obj == null) ? nvl : obj;
    }
    
    public static boolean IS_NULL(final Object data) {
        return data == null;
    }
    
    public static Object PREV(final Object key, final Object val) {
        final Object prevval = BuiltInFunc.PREVs.get(key);
        BuiltInFunc.PREVs.put(key, val);
        return (prevval == null) ? val : prevval;
    }
    
    public static DateTime getSnmpDateTime(final Object o) {
        final byte[] bytes = ((SnmpPayload)o).getSnmpByteValue();
        long year = 0L;
        int i = 1;
        for (int shiftBy = 0; shiftBy < 16; shiftBy += 8) {
            year |= (bytes[0 + i] & 0xFF) << shiftBy;
            --i;
        }
        final byte[] direction = { 0 };
        System.arraycopy(bytes, 8, direction, 0, 1);
        final String hrOffsetStr = new String(direction) + bytes[9];
        final int hoursOffset = Integer.parseInt(hrOffsetStr);
        return new DateTime((int)year, (int)bytes[2], (int)bytes[3], (int)bytes[4], (int)bytes[5], (int)bytes[6], (int)bytes[7], DateTimeZone.forOffsetHoursMinutes(hoursOffset, (int)bytes[10]));
    }
    
    public static Object VALUE(final Record obj, final String key) {
        final Object val = ((Map)obj.data[0]).get(key);
        return val;
    }
    
    public static Object VALUE(final Record evt, final int n) {
        if (n < evt.data.length) {
            return evt.data[n];
        }
        return null;
    }
    
    public static String TO_MACID(final Object o, final String del) {
        String str = "";
        if (o == null) {
            return str;
        }
        final byte[] tmp = ((SnmpPayload)o).getSnmpByteValue();
        for (int itr = 0; itr < tmp.length; ++itr) {
            if (itr != 0) {
                str = str + del + String.format("%02X", tmp[itr] & 0xFF);
            }
            else {
                str += String.format("%02X", tmp[itr] & 0xFF);
            }
        }
        return str;
    }
    
    public static Object META(final Record e, final String key) {
        final Object o = e.metadata.get(key);
        final Object ret = (o == null) ? "" : o;
        return ret;
    }
    
    public static Object META(final OPCUADataChangeEvent e, final String key) {
        final Object o = e.metadata.get(key);
        final Object ret = (o == null) ? "" : o;
        return ret;
    }
    
    public static Object META(final JsonNodeEvent jsonNodeEvent, final String key) {
        Object value = jsonNodeEvent.metadata.get(key);
        value = ((value == null) ? "" : value);
        return value;
    }
    
    public static int FIELDCOUNT(final Record Record) {
        return Record.data.length;
    }
    
    public static boolean IS_PRESENT(final Record e, final Object[] p_array, final int index) {
        final int pos = index / 7;
        final int offset = index % 7;
        int val = 0;
        final byte b = (byte)(1 << offset);
        if (p_array == e.data) {
            val = (e.dataPresenceBitMap[pos] & b) >> offset;
        }
        else if (p_array == e.before) {
            val = (e.beforePresenceBitMap[pos] & b) >> offset;
        }
        return val == 1;
    }
    
    public static HashMap<String, Object> DATA(final Record event) {
        if (event.data != null) {
            return getDataOrBeforeArrayAsMap(event, event.data);
        }
        return null;
    }
    
    public static Object GETDATA(final Record event, final String colname) {
        final int index = getIndexOfColumn(event, colname);
        return event.data[index];
    }
    
    public static Record convertTypedeventToRecord(final Event event, String typeName) throws SecurityException, MetaDataRepositoryException {
        if (event == null) {
            return null;
        }
        if (typeName == null || typeName.isEmpty()) {
            return null;
        }
        Record Record = null;
        typeName = typeName.trim();
        if (typeName.indexOf(46) == -1) {
            throw new RuntimeException("Provide fully qualified name of type. e.g. ns1.type1 ");
        }
        if (event.getClass().getCanonicalName() != "com.datasphere.proc.events.Record") {
            final String ns = typeName.substring(0, typeName.indexOf("."));
            final String name = typeName.substring(typeName.indexOf(46) + 1);
            final MetaInfo.Type typeDef = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.TYPE, ns, name, null, HSecurityManager.TOKEN);
            if (typeDef == null) {
                throw new RuntimeException("Failed to retrieve type information for: " + typeName);
            }
            final Object[] payload = event.getPayload();
            if (payload != null) {
                final int payloadLength = payload.length;
                Record = new Record(payloadLength, (UUID)null);
                final HashMap<String, Object> metadata = new HashMap<String, Object>();
                metadata.put("TableName", typeDef.getFullName());
                metadata.put("OperationName", "INSERT");
                Record.metadata = metadata;
                Record.data = new Object[payloadLength];
                Record.typeUUID = typeDef.getUuid();
                int i = 0;
                for (final Object o : payload) {
                    Record.setData(i++, o);
                }
            }
        }
        return Record;
    }
    
    public static Record convertAvroEventToRecord(final Event event) {
        if (!(event instanceof AvroEvent)) {
            throw new RuntimeException("this method can't convert NON-Avro events. ");
        }
        Record Record = null;
        final Object[] payload = event.getPayload();
        if (payload != null) {
            final int payloadLength = payload.length;
            Record = new Record(payloadLength, (UUID)null);
            final HashMap<String, Object> metadata = new HashMap<String, Object>();
            metadata.put("OperationName", "INSERT");
            Record.metadata = metadata;
            Record.data = new Object[payloadLength];
            int i = 0;
            for (final Object o : payload) {
                Record.setData(i++, o);
            }
        }
        return Record;
    }
    
    public static HashMap<String, Object> BEFORE(final Record event) {
        if (event.before != null) {
            return getDataOrBeforeArrayAsMap(event, event.before);
        }
        return null;
    }
    
    public static Object VALUE(final Object object, final String keyString) {
        if (!(object instanceof HashMap)) {
            return null;
        }
        final HashMap<Utf8, ?> mp = (HashMap<Utf8, ?>)object;
        if (BuiltInFunc.stringToUtf8Mapper.containsKey(keyString)) {
            return mp.get(BuiltInFunc.stringToUtf8Mapper.get(keyString));
        }
        final Utf8 Utf8String = new Utf8(keyString);
        BuiltInFunc.stringToUtf8Mapper.put(keyString, Utf8String);
        return mp.get(Utf8String);
    }
    
    public static JsonNode TO_JSON_NODE(final Object obj) throws IOException {
        final String content = BuiltInFunc.mapper.writeValueAsString(obj);
        return BuiltInFunc.mapper.readTree(content);
    }
    
    private static HashMap<String, Object> getDataOrBeforeArrayAsMap(final Record event, final Object[] dataOrBeforeArray) {
        final Field[] fieldsOfThisTable = getFieldsArray(event);
        final HashMap<String, Object> dataOrBeforeArrayMap = new HashMap<String, Object>();
        for (Integer i = 0; i < dataOrBeforeArray.length; ++i) {
            final boolean isPresent = IS_PRESENT(event, dataOrBeforeArray, i);
            if (isPresent) {
                final String operationName = (String)META(event, Constant.OPERATION_TYPE);
                String columnName;
                if (operationName != null && operationName.equalsIgnoreCase(Constant.DDL_OPERATION)) {
                    columnName = "DDLCommand";
                }
                else {
                    columnName = fieldsOfThisTable[i].getName();
                }
                dataOrBeforeArrayMap.put(columnName, (dataOrBeforeArray[i] != null) ? dataOrBeforeArray[i] : null);
            }
        }
        return dataOrBeforeArrayMap;
    }
    
    public static String IP_COUNTRY(final String ip) {
        return IPLookup2.lookupCountry(ip);
    }
    
    public static String IP_CITY(final String ip) {
        return IPLookup2.lookupCity(ip);
    }
    
    public static double IP_LAT(final String ip) {
        final String strVal = IPLookup2.lookupLat(ip);
        if (strVal == null) {
            return 0.0;
        }
        return Double.parseDouble(strVal);
    }
    
    public static double IP_LON(final String ip) {
        final String strVal = IPLookup2.lookupLon(ip);
        if (strVal == null) {
            return 0.0;
        }
        return Double.parseDouble(strVal);
    }
    
    public static String NSK_TXN_STRING(final String TxnID, final String SystemName) {
        return TxnUtilities.formatNSKTxnID(TxnID, SystemName);
    }
    
    public static boolean NSK_TXNS_ARE_SAME(final String TxnID1, final String TxnID2) {
        return TxnUtilities.NSKTxnIDsAreSame(TxnID1, TxnID2);
    }
    
    public static String NSK_CNVT_TXNID_TO_UNSIGNED(final String TxnID) {
        return TxnUtilities.convertNSKTxnIDToUnsigned(TxnID);
    }
    
    public static Pattern compile__like__pattern(final String pat) {
        String re = pat.replace(".", "\\.");
        re = re.replace("*", "\\*");
        re = re.replace("?", "\\?");
        re = re.replace("(\\?", "(?");
        re = re.replace("^", "\\^");
        re = re.replace("$", "\\$");
        re = re.replace("%", ".*");
        re = re.replace("_", ".");
        final Pattern p = Pattern.compile(re);
        return p;
    }
    
    public static Timestamp toTimestamp(final long millisec, final int nanosec) {
        final Timestamp t = new Timestamp(millisec);
        t.setNanos(nanosec);
        return t;
    }
    
    public static DateTime addInterval(final DateTime t, final long interval) {
        return t.plus(interval / 1000L);
    }
    
    public static String ar(final String[] data, final int index) {
        try {
            return data[index];
        }
        catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }
    
    public static int arlen(final Object[] data) {
        return data.length;
    }
    
    public static StringArrayEvent func(final SimpleEvent e) {
        return new StringArrayEvent(System.currentTimeMillis());
    }
    
    public static int castToInt(final String s) {
        final int x = Integer.parseInt(s);
        return x;
    }
    
    public static double castToDouble(final String s) {
        final double x = Double.parseDouble(s);
        return x;
    }
    
    public static int strIndexOf(final String s, final String search) {
        final int x = s.indexOf(search);
        return x;
    }
    
    public static int strLastIndexOf(final String s, final String search) {
        final int x = s.lastIndexOf(search);
        return x;
    }
    
    public static int strLength(final String s) {
        final int x = s.length();
        return x;
    }
    
    public static String trimStr(final String s) {
        if (s == null) {
            return s;
        }
        return s.trim();
    }
    
    public static String substr(final String s, final int start, final int count) {
        return s.substring(start, count);
    }
    
    public static String castToString(final int s) {
        return Integer.toString(s);
    }
    
    public static String castToString(final Object s) {
        return (String)s;
    }
    
    public static StringArrayEvent castToStringArray(final SimpleEvent inpEvent) {
        final StringArrayEvent evt = new StringArrayEvent(System.currentTimeMillis());
        final Object[] objarr = inpEvent.getPayload();
        String[] strdata = null;
        final String[] dataArr = (String[])objarr[0];
        strdata = new String[dataArr.length];
        for (int i = 0; i < dataArr.length; ++i) {
            strdata[i] = dataArr[i];
        }
        evt.setData(strdata);
        return evt;
    }
    
    public static String[] getEventData(final SimpleEvent ev) {
        return getStringArrayData(castToStringArray(ev));
    }
    
    public static String[] getStringArrayData(final StringArrayEvent str) {
        return str.getData();
    }
    
    public static long castToLong(final String s) {
        return Long.parseLong(s);
    }
    
    public static String textFromFields(final String text, final SimpleEvent event) {
        final Object[] objarr = event.getPayload();
        String ret = text;
        for (int i = objarr.length - 1; i >= 0; --i) {
            if (ret.indexOf("$" + (i + 1)) != -1) {
                final String value = objarr[i].toString();
                final String numVal = "" + (i + 1);
                String numReg = "";
                for (int j = 0; j < numVal.length(); ++j) {
                    numReg = numReg + "[" + numVal.charAt(j) + "]";
                }
                ret = ret.replaceAll("[$]" + numReg, value);
            }
        }
        return ret;
    }
    
    @CustomFunction(translator = matchTranslator.class)
    public abstract boolean match2(final String p0, final String p1);
    
    public static boolean match2impl(final String a, final String regex) {
        final Pattern p = Pattern.compile(regex);
        final Matcher m = p.matcher(a);
        return m.find();
    }
    
    public static boolean match2impl(final String a, final Pattern pat) {
        final Matcher m = pat.matcher(a);
        return m.find();
    }
    
    public static String matchUser(final String s) {
        final String regex = "user\\W+(\\w+)";
        final Pattern p = Pattern.compile(regex);
        final Matcher m = p.matcher(s);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }
    
    public static String matchPort(final String s) {
        final String regex = "port\\W+(\\w+)";
        final Pattern p = Pattern.compile(regex);
        final Matcher m = p.matcher(s);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }
    
    public static String match(final String s, final String regex) {
        return match(s, regex, 1);
    }
    
    public static String match(final String s, final String regex, final int groupNumber) {
        Pattern p = BuiltInFunc.patterns.get(regex);
        if (p == null) {
            p = Pattern.compile(regex);
            BuiltInFunc.patterns.put(regex, p);
        }
        String ret = null;
        try {
            final Matcher m = p.matcher(s);
            if (m.find()) {
                ret = m.group(groupNumber);
            }
        }
        catch (Exception e) {
            System.err.println(e);
        }
        return ret;
    }
    
    public static boolean MATCH_ANY_ADD_PATTERN(final String group, final String pat) {
        HashMap<String, Pattern> group_patterns = BuiltInFunc.all_patterns.get(group);
        if (group_patterns == null) {
            group_patterns = new HashMap<String, Pattern>();
            BuiltInFunc.all_patterns.put(group, group_patterns);
        }
        if (group_patterns.containsKey(pat)) {
            return false;
        }
        if (pat.equals("<RESET>")) {
            group_patterns.clear();
            return false;
        }
        final Pattern p = Pattern.compile(pat);
        group_patterns.put(pat, p);
        return true;
    }
    
    public static String MATCH_ANY_FIND_PATTERN(final String group, final String to_match) {
        String ret = null;
        final HashMap<String, Pattern> group_patterns = BuiltInFunc.all_patterns.get(group);
        if (group_patterns == null) {
            return ret;
        }
        for (final Map.Entry<String, Pattern> entry : group_patterns.entrySet()) {
            if (entry.getValue().matcher(to_match).matches()) {
                ret = entry.getKey();
                break;
            }
        }
        return ret;
    }
    
    public static String MATCH_ANY_EXACT_PATTERN(final String group, final String to_match) {
        String ret = null;
        final HashMap<String, Pattern> group_patterns = BuiltInFunc.all_patterns.get(group);
        if (group_patterns == null) {
            return ret;
        }
        for (final Map.Entry<String, Pattern> entry : group_patterns.entrySet()) {
            if (entry.getKey().equals(to_match)) {
                ret = entry.getKey();
                break;
            }
        }
        return ret;
    }
    
    public static int uid(final String set, final String name) {
        Map<String, Integer> uids = BuiltInFunc.uidSets.get(set);
        if (uids == null) {
            uids = new HashMap<String, Integer>();
            BuiltInFunc.uidSets.put(set, uids);
        }
        Integer val = uids.get(name);
        if (val == null) {
            val = uids.size() + 1;
            uids.put(name, val);
        }
        return val;
    }
    
    public static String matchIP(final String s) {
        final Pattern p = Pattern.compile("([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})");
        final Matcher m = p.matcher(s);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }
    
    public static String parseMonthToNum(final String month) {
        if (month.equalsIgnoreCase("jan")) {
            return new String("01");
        }
        if (month.equalsIgnoreCase("feb")) {
            return new String("02");
        }
        if (month.equalsIgnoreCase("mar")) {
            return new String("03");
        }
        if (month.equalsIgnoreCase("apr")) {
            return new String("04");
        }
        if (month.equalsIgnoreCase("may")) {
            return new String("05");
        }
        if (month.equalsIgnoreCase("jun")) {
            return new String("06");
        }
        if (month.equalsIgnoreCase("jul")) {
            return new String("07");
        }
        if (month.equalsIgnoreCase("aug")) {
            return new String("08");
        }
        if (month.equalsIgnoreCase("sep")) {
            return new String("09");
        }
        if (month.equalsIgnoreCase("oct")) {
            return new String("10");
        }
        if (month.equalsIgnoreCase("nov")) {
            return new String("11");
        }
        if (month.equalsIgnoreCase("dec")) {
            return new String("12");
        }
        BuiltInFunc.logger.warn((Object)"Invalid month");
        return null;
    }
    
    @CustomFunction(translator = EventListGetter.class)
    public static List<SimpleEvent> eventList(final Object o) {
        final HD wa = (HD)o;
        return wa.getEvents();
    }
    
    public static ArrayNode eventList(final JsonNodeEvent event) {
        final ArrayNode ret = JsonNodeFactory.instance.arrayNode();
        final ObjectNode x = (ObjectNode)event.data;
        final Iterator<Map.Entry<String, JsonNode>> it = (Iterator<Map.Entry<String, JsonNode>>)x.fields();
        while (it.hasNext()) {
            final Map.Entry<String, JsonNode> e = it.next();
            final String name = e.getKey();
            final JsonNode n = e.getValue();
            if (n.isArray() && name.contains(":")) {
                final Iterator<JsonNode> ait = (Iterator<JsonNode>)((ArrayNode)n).elements();
                while (ait.hasNext()) {
                    ret.add((JsonNode)ait.next());
                }
            }
        }
        return ret;
    }
    
    public static MonitorBatchEvent processBatch(final MonitorBatchEvent event) {
        try {
            return MonitorModel.processBatch(event);
        }
        catch (Exception e) {
            if (e instanceof SearchPhaseExecutionException) {
                BuiltInFunc.logger.warn((Object)("Failed to process MonitorBatchEvent with exception " + e.getMessage()));
            }
            else {
                BuiltInFunc.logger.error((Object)"Failed to process MonitorBatchEvent with exception ", (Throwable)e);
            }
            return null;
        }
    }
    
    @CustomFunction(translator = AccessPrevEventInBuffer.class)
    public abstract Object prev();
    
    @CustomFunction(translator = AccessPrevEventInBuffer.class)
    public abstract Object prev(final int p0);
    
    @CustomFunction(translator = AnyAttrLike.class)
    public abstract boolean anyattrlike(final Object p0, final String p1);
    
    @CustomFunction(translator = DataSetItertorTrans.class)
    public abstract Iterator<SimpleEvent> it(final Object p0);
    
    public static String windump(final Iterator<SimpleEvent> it) {
        final StringBuilder sb = new StringBuilder();
        sb.append("windump {\n");
        while (it.hasNext()) {
            final SimpleEvent e = it.next();
            sb.append(e + "\n");
        }
        sb.append("}\n");
        return sb.toString();
    }
    
    public static JsonNode makeJSON(final String jsonText) {
        try {
            return new ObjectMapper().readTree(jsonText);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static List<?> makeList(final Object... objs) {
        return new ArrayList<Object>(Arrays.asList(objs));
    }
    
    public static SimpleEvent makeSimpleEvent(final Object... objs) {
        final SimpleEvent e = new SimpleEvent(System.currentTimeMillis());
        e.payload = objs;
        return e;
    }
    
    public static StringArrayEvent makeStringArrayEvent(final Object... objs) {
        final StringArrayEvent e = new StringArrayEvent(System.currentTimeMillis());
        final String[] data = new String[objs.length];
        int i = 0;
        for (final Object o : objs) {
            data[i++] = o.toString();
        }
        e.setData(data);
        return e;
    }
    
    public static Record makeRecord(final Object... objs) {
        final Record e = new Record();
        e.data = new Object[objs.length];
        e.dataPresenceBitMap = new byte[(objs.length + 7) / 8];
        int i = 0;
        for (final Object o : objs) {
            e.setData(i++, o);
        }
        return e;
    }
    
    public static Record replaceData(final Record event, final String colname, final Object newValue) {
        final int index = getIndexOfColumn(event, colname);
        return replaceData(event, index, newValue);
    }
    
    public static Record replaceData(final Record event, final int index, final Object newValue) {
        event.setData(index, newValue);
        return event;
    }
    
    public static Record replaceBefore(final Record event, final String colname, final Object newValue) {
        final int index = getIndexOfColumn(event, colname);
        return replaceBefore(event, index, newValue);
    }
    
    public static Record replaceBefore(final Record event, final int index, final Object newValue) {
        event.setBefore(index, newValue);
        return event;
    }
    
    private static int getIndexOfColumn(final Record event, final String colname) {
        final Field[] fieldsOfThisTable = getFieldsArray(event);
        for (int ik = 0; ik < fieldsOfThisTable.length; ++ik) {
            if (fieldsOfThisTable[ik].getName().equalsIgnoreCase(colname)) {
                return ik;
            }
        }
        BuiltInFunc.logger.error((Object)("No column with name: " + colname + " exists in table : " + META(event, "TableName")));
        return -1;
    }
    
    private static Field[] getFieldsArray(final Record event) {
        Field[] fieldsOfThisTable = null;
        if (event.typeUUID != null) {
            if (BuiltInFunc.typeUUIDCache.containsKey(event.typeUUID)) {
                fieldsOfThisTable = BuiltInFunc.typeUUIDCache.get(event.typeUUID);
            }
            else {
                try {
                    final MetaInfo.Type dataType = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(event.typeUUID, HSecurityManager.TOKEN);
                    final Class<?> typeClass = ClassLoader.getSystemClassLoader().loadClass(dataType.className);
                    fieldsOfThisTable = typeClass.getDeclaredFields();
                    BuiltInFunc.typeUUIDCache.put(event.typeUUID, fieldsOfThisTable);
                }
                catch (MetaDataRepositoryException | ClassNotFoundException ex2) {
                    BuiltInFunc.logger.warn((Object)("Unable to fetch the type for table " + event.metadata.get("TableName") + ex2));
                }
            }
        }
        return fieldsOfThisTable;
    }
    
    public static Record putUserData(final Record event, final Object... values) {
        if (values == null || values.length == 0) {
            return event;
        }
        if (values.length % 2 == 1) {
            throw new RuntimeException("When using 'putUserData' function, every key must have a value, i.e array length must be even number.");
        }
        final Record newEvent = Record.makeCopy(event);
        for (int ik = 0; ik < values.length; ik += 2) {
            newEvent.putUserdata((String)values[ik], values[ik + 1]);
        }
        return newEvent;
    }
    
    public static Object USERDATA(final Record event, final String key) {
        if (event.userdata != null) {
            final Record newEvent = Record.makeCopy(event);
            return newEvent.userdata.get(key);
        }
        return null;
    }
    
    public static Record removeUserData(final Record event, final Object... keys) {
        if (keys == null || keys.length == 0) {
            return event;
        }
        if (event.userdata == null || event.userdata.isEmpty()) {
            return event;
        }
        final Record newEvent = Record.makeCopy(event);
        for (final Object val : keys) {
            final String key = (String)val;
            newEvent.removeUserData(key);
        }
        return newEvent;
    }
    
    public static Record clearUserData(final Record event) {
        if (event.userdata == null || event.userdata.isEmpty()) {
            return event;
        }
        final Record newEvent = Record.makeCopy(event);
        newEvent.userdata.clear();
        return newEvent;
    }
    
    public static boolean Pause(final Long period, final SimpleEvent e) {
        LockSupport.parkNanos(period * 1000L);
        return true;
    }
    
    public static JsonNode JSONFrom(final Object value) {
        return RecordUtil.JSONFrom(value);
    }
    
    public static JsonNode JSONNew() {
        return RecordUtil.JSONNew();
    }
    
    public static JsonNode JSONPut(final JsonNode node, final String field, final Object value) {
        return RecordUtil.JSONPut(node, field, value);
    }
    
    public static JsonNode JSONArrayAdd(final JsonNode node, final Object value) {
        return RecordUtil.JSONArrayAdd(node, value);
    }
    
    public static String JSONGetString(final JsonNode node, final String field) {
        return RecordUtil.JSONGetString(node, field);
    }
    
    public static Boolean JSONGetBoolean(final JsonNode node, final String field) {
        return RecordUtil.JSONGetBoolean(node, field);
    }
    
    public static Double JSONGetDouble(final JsonNode node, final String field) {
        return RecordUtil.JSONGetDouble(node, field);
    }
    
    public static Integer JSONGetInteger(final JsonNode node, final String field) {
        return RecordUtil.JSONGetInteger(node, field);
    }
    
    public static GenericRecord AvroPut(final GenericRecord parent, final String name, final Object value) {
        return RecordUtil.AvroPut(parent, name, value);
    }
    
    public static Object AvroGet(final GenericRecord parent, final String name) {
        return RecordUtil.AvroGet(parent, name);
    }
    
    public static JsonNode AvroToJson(final Object datum, final boolean ignoreNulls) {
        return RecordUtil.AvroToJson(datum, ignoreNulls);
    }
    
    public static JsonNode AvroToJson(final Object datum) {
        return RecordUtil.AvroToJson(datum, false);
    }
    
    @AggHandlerDesc(handler = SumInt.class)
    public abstract Long sum(final Integer p0);
    
    @AggHandlerDesc(handler = SumLong.class)
    public abstract Long sum(final Long p0);
    
    @AggHandlerDesc(handler = SumFloat.class)
    public abstract Float sum(final Float p0);
    
    @AggHandlerDesc(handler = SumDouble.class)
    public abstract Double sum(final Double p0);
    
    @AggHandlerDesc(handler = SumBigDecimal.class)
    public abstract BigDecimal sum(final BigDecimal p0);
    
    @AggHandlerDesc(handler = AvgInt.class)
    public abstract Float avg(final Integer p0);
    
    @AggHandlerDesc(handler = AvgLong.class)
    public abstract Float avg(final Long p0);
    
    @AggHandlerDesc(handler = AvgFloat.class)
    public abstract Float avg(final Float p0);
    
    @AggHandlerDesc(handler = AvgDouble.class)
    public abstract Double avg(final Double p0);
    
    @AggHandlerDesc(handler = AvgNumber.class)
    public abstract Double avgD(final Number p0);
    
    @AggHandlerDesc(handler = StdDevPNumber.class)
    public abstract Double StdDevP(final Number p0);
    
    @AggHandlerDesc(handler = StdDevSNumber.class)
    public abstract Double StdDevS(final Number p0);
    
    @AggHandlerDesc(handler = CountNotNull.class, distinctHandler = CountNotNullDistinct.class)
    public abstract long count(final Object p0);
    
    @AggHandlerDesc(handler = CountAny.class)
    public abstract long count();
    
    @AggHandlerDesc(handler = FirstAny.class)
    public abstract Object first(final Object p0);
    
    @AcceptWildcard
    @AggHandlerDesc(handler = LastAny.class)
    public abstract Object last(final Object p0);
    
    @AggHandlerDesc(handler = ListByKey.class)
    public abstract List listByKey(final String p0, final Object p1);
    
    @AggHandlerDesc(handler = MinInt.class)
    public abstract Integer min(final Integer p0);
    
    @AggHandlerDesc(handler = MinLong.class)
    public abstract Long min(final Long p0);
    
    @AggHandlerDesc(handler = MinFloat.class)
    public abstract Float min(final Float p0);
    
    @AggHandlerDesc(handler = MinDouble.class)
    public abstract Double min(final Double p0);
    
    @AggHandlerDesc(handler = MinString.class)
    public abstract String min(final String p0);
    
    @AggHandlerDesc(handler = LastInt.class)
    public abstract Integer last(final Integer p0);
    
    @AggHandlerDesc(handler = LastLong.class)
    public abstract Long last(final Long p0);
    
    @AggHandlerDesc(handler = LastFloat.class)
    public abstract Float last(final Float p0);
    
    @AggHandlerDesc(handler = LastDouble.class)
    public abstract Double last(final Double p0);
    
    @AggHandlerDesc(handler = LastString.class)
    public abstract String last(final String p0);
    
    @AggHandlerDesc(handler = LastDateTime.class)
    public abstract DateTime last(final DateTime p0);
    
    @AggHandlerDesc(handler = FirstInt.class)
    public abstract Integer first(final Integer p0);
    
    @AggHandlerDesc(handler = FirstLong.class)
    public abstract Long first(final Long p0);
    
    @AggHandlerDesc(handler = FirstFloat.class)
    public abstract Float first(final Float p0);
    
    @AggHandlerDesc(handler = FirstDouble.class)
    public abstract Double first(final Double p0);
    
    @AggHandlerDesc(handler = FirstString.class)
    public abstract String first(final String p0);
    
    @AggHandlerDesc(handler = FirstDateTime.class)
    public abstract DateTime first(final DateTime p0);
    
    @AggHandlerDesc(handler = MaxInt.class)
    public abstract Integer max(final Integer p0);
    
    @AggHandlerDesc(handler = MaxLong.class)
    public abstract Long max(final Long p0);
    
    @AggHandlerDesc(handler = MaxFloat.class)
    public abstract Float max(final Float p0);
    
    @AggHandlerDesc(handler = MaxDouble.class)
    public abstract Double max(final Double p0);
    
    @AggHandlerDesc(handler = MaxString.class)
    public abstract String max(final String p0);
    
    @AggHandlerDesc(handler = MaxOccursString.class)
    public abstract String maxOccurs(final String p0);
    
    @AggHandlerDesc(handler = DblArray.class)
    public abstract DoubleArrayList dblarray(final double p0);
    
    @AggHandlerDesc(handler = GenList.class)
    public abstract List list(final Object... p0);
    
    @AggHandlerDesc(handler = GeometryConv.class)
    public abstract LocationArrayList locInfoList(final double p0, final double p1, final String p2, final String p3, final String p4);
    
    @AggHandlerDesc(handler = Spatial.class)
    public abstract GeoSearchCoverTree<Event> SPATIAL_INDEX(final String p0, final Event p1, final double p2, final double p3);
    
    public static List<Event> SPATIAL_FIND_IN_RADIUS(final String indexName, final double lat, final double lon, final double radiusInM) {
        final GeoSearchCoverTree<Event> search = BuiltInFunc.spatialIndices.get(indexName);
        if (search == null) {
            return (List<Event>)Collections.EMPTY_LIST;
        }
        return search.queryRadius(lat, lon, radiusInM);
    }
    
    public static double SPATIAL_DISTANCE_M(final double lat1, final double lon1, final double lat2, final double lon2) {
        return GeoSearchCoverTree.distFrom(lat1, lon1, lat2, lon2);
    }
    
    public static double SPATIAL_DISTANCE_MILES(final double lat1, final double lon1, final double lat2, final double lon2) {
        return 6.21371E-4 * GeoSearchCoverTree.distFrom(lat1, lon1, lat2, lon2);
    }
    
    public static Point SPATIAL_POINT(final double lat, final double lon) {
        return new Point(lat, lon);
    }
    
    public static Point SPATIAL_POINT(final Object obj) {
        if (obj instanceof String) {
            return new Point((String)obj);
        }
        if (obj instanceof JsonNode) {
            return new Point((JsonNode)obj);
        }
        if (obj instanceof double[]) {
            return new Point((double[])obj);
        }
        throw new RuntimeException("Can only convert String, JsonNode or double[] to Point, got " + obj.getClass());
    }
    
    public static Polygon SPATIAL_POLYGON(final Object obj) {
        if (obj instanceof String) {
            return new Polygon((String)obj);
        }
        if (obj instanceof JsonNode) {
            return new Polygon((JsonNode)obj);
        }
        if (obj instanceof double[]) {
            return new Polygon((double[])obj);
        }
        if (obj instanceof double[][]) {
            return new Polygon((double[][])obj);
        }
        throw new RuntimeException("Can only convert String, JsonNode, double[] or double[][] to Polygon, got " + obj.getClass());
    }
    
    public static boolean SPATIAL_POINT_IN_POLYGON(final Point point, final Polygon polygon) {
        return polygon != null && point != null && polygon.isInside(point);
    }
    
    public static Object ListFirst(final List list) {
        if (list != null && list.size() > 0) {
            return list.get(0);
        }
        return null;
    }
    
    public static Object ListLast(final List list) {
        if (list != null && list.size() > 0) {
            return list.get(list.size() - 1);
        }
        return null;
    }
    
    static {
        BuiltInFunc.logger = Logger.getLogger((Class)BuiltInFunc.class);
        BuiltInFunc.defaultDateTimeFormatter = makeDefaultFormatter();
        BuiltInFunc.dtfs = new ConcurrentHashMap<String, DateTimeFormatter>();
        BuiltInFunc.dps = new ConcurrentHashMap<String, DateParser>();
        BuiltInFunc.PREVs = new ConcurrentHashMap<Object, Object>();
        BuiltInFunc.typeUUIDCache = new HashMap<UUID, Field[]>();
        BuiltInFunc.stringToUtf8Mapper = new HashMap<String, Utf8>();
        BuiltInFunc.mapper = new ObjectMapper();
        BuiltInFunc.patterns = new HashMap<String, Pattern>();
        BuiltInFunc.all_patterns = new HashMap<String, HashMap<String, Pattern>>();
        BuiltInFunc.uidSets = new HashMap<String, Map<String, Integer>>();
        BuiltInFunc.spatialIndices = new HashMap<String, GeoSearchCoverTree<Event>>();
    }
    
    public static class Spatial
    {
        private GeoSearchCoverTree<Event> search;
        
        public Spatial() {
            this.search = null;
        }
        
        public GeoSearchCoverTree<Event> getAggValue() {
            return this.search;
        }
        
        public void incAggValue(final String indexName, final Event e, final double lat, final double lon) {
            if (this.search == null) {
                this.search = BuiltInFunc.spatialIndices.get(indexName);
                if (this.search == null) {
                    this.search = new GeoSearchCoverTree<Event>();
                    BuiltInFunc.spatialIndices.put(indexName, this.search);
                }
            }
            this.search.add(e, lat, lon);
        }
        
        public void decAggValue(final String indexName, final Event e, final double lat, final double lon) {
            if (this.search == null) {
                this.search = BuiltInFunc.spatialIndices.get(indexName);
                if (this.search == null) {
                    return;
                }
            }
            this.search.remove(e, lat, lon);
        }
    }
    
    public static class GeometryConv
    {
        LocationArrayList value;
        
        public GeometryConv() {
            this.value = new LocationArrayList();
        }
        
        public LocationArrayList getAggValue() {
            synchronized (this.value) {
                return new LocationArrayList(this.value);
            }
        }
        
        public void incAggValue(final double latVal, final double longVal, final String city, final String zip, final String companyName) {
            synchronized (this.value) {
                this.value.add(new LocationInfo(latVal, longVal, city, zip, companyName));
            }
        }
        
        public void decAggValue(final double latVal, final double longVal, final String city, final String zip, final String companyName) {
            synchronized (this.value) {
                this.value.remove(new LocationInfo(latVal, longVal, city, zip, companyName));
            }
        }
    }
    
    public static class DblArray
    {
        DoubleArrayList value;
        
        public DblArray() {
            this.value = new DoubleArrayList();
        }
        
        public DoubleArrayList getAggValue() {
            synchronized (this.value) {
                return this.value.copy();
            }
        }
        
        public void incAggValue(final double arg) {
            synchronized (this.value) {
                this.value.add(arg);
            }
        }
        
        public void decAggValue(final double arg) {
            synchronized (this.value) {
                assert !this.value.isEmpty();
                assert this.value.getQuick(0) == arg;
                this.value.remove(0);
            }
        }
    }
    
    public static class GenList
    {
        List value;
        boolean allEvents;
        
        public GenList() {
            this.value = new CopyOnWriteArrayList();
            this.allEvents = true;
        }
        
        public List getAggValue() {
            if (this.allEvents) {
                final Event[] valArr = (Event[])this.value.toArray(new Event[this.value.size()]);
                Arrays.sort(valArr, new Comparator<Event>() {
                    @Override
                    public int compare(final Event o1, final Event o2) {
                        if (o1 == null && o2 == null) {
                            return 0;
                        }
                        if (o1 != null && o2 == null) {
                            return 1;
                        }
                        if (o1 == null && o2 != null) {
                            return -1;
                        }
                        final UUID u1 = o1.get_da_SimpleEvent_ID();
                        final UUID u2 = o2.get_da_SimpleEvent_ID();
                        if (u1 == null && u2 == null) {
                            return 0;
                        }
                        if (u1 != null && u2 == null) {
                            return 1;
                        }
                        if (u1 == null && u2 != null) {
                            return -1;
                        }
                        return u1.compareTo(u2);
                    }
                });
                for (int i = 0; i < valArr.length; ++i) {
                    this.value.set(i, valArr[i]);
                }
            }
            return this.value;
        }
        
        public void incAggValue(final Object... arg) {
            for (final Object a : arg) {
                if (a != null) {
                    this.value.add(a);
                    if (!(a instanceof Event)) {
                        this.allEvents = false;
                    }
                }
            }
        }
        
        public void decAggValue(final Object... arg) {
            for (final Object a : arg) {
                if (a != null) {
                    this.value.remove(a);
                }
            }
        }
    }
    
    public static class SumInt
    {
        int count;
        long value;
        
        public SumInt() {
            this.count = 0;
            this.value = 0L;
        }
        
        public Long getAggValue() {
            return (this.count == 0) ? 0L : this.value;
        }
        
        public void incAggValue(final Integer arg) {
            if (arg != null) {
                ++this.count;
                this.value += arg;
            }
        }
        
        public void decAggValue(final Integer arg) {
            if (arg != null) {
                --this.count;
                this.value -= arg;
            }
        }
    }
    
    public static class SumLong
    {
        long count;
        long value;
        
        public SumLong() {
            this.count = 0L;
            this.value = 0L;
        }
        
        public Long getAggValue() {
            return (this.count == 0L) ? 0L : this.value;
        }
        
        public void incAggValue(final Long arg) {
            if (arg != null) {
                ++this.count;
                this.value += arg;
            }
        }
        
        public void decAggValue(final Long arg) {
            if (arg != null) {
                --this.count;
                this.value -= arg;
            }
        }
    }
    
    public static class SumFloat
    {
        long count;
        float value;
        
        public SumFloat() {
            this.count = 0L;
            this.value = 0.0f;
        }
        
        public Float getAggValue() {
            return (this.count == 0L) ? 0.0f : this.value;
        }
        
        public void incAggValue(final Float arg) {
            if (arg != null) {
                ++this.count;
                this.value += arg;
            }
        }
        
        public void decAggValue(final Float arg) {
            if (arg != null) {
                --this.count;
                this.value -= arg;
            }
        }
    }
    
    public static class SumDouble
    {
        long count;
        double value;
        
        public SumDouble() {
            this.count = 0L;
            this.value = 0.0;
        }
        
        public Double getAggValue() {
            return (this.count == 0L) ? 0.0 : this.value;
        }
        
        public void incAggValue(final Double arg) {
            if (arg != null) {
                ++this.count;
                this.value += arg;
            }
        }
        
        public void decAggValue(final Double arg) {
            if (arg != null) {
                --this.count;
                this.value -= arg;
            }
        }
    }
    
    public static class SumBigDecimal
    {
        long count;
        BigDecimal value;
        
        public SumBigDecimal() {
            this.count = 0L;
            this.value = BigDecimal.ZERO;
        }
        
        public BigDecimal getAggValue() {
            return (this.count == 0L) ? BigDecimal.ZERO : this.value;
        }
        
        public void incAggValue(final BigDecimal arg) {
            if (arg != null) {
                ++this.count;
                this.value = this.value.add(arg);
            }
        }
        
        public void decAggValue(final BigDecimal arg) {
            if (arg != null) {
                --this.count;
                this.value = this.value.subtract(arg);
            }
        }
    }
    
    public static class AvgInt
    {
        long value;
        int count;
        
        public AvgInt() {
            this.value = 0L;
            this.count = 0;
        }
        
        public Float getAggValue() {
            final float value_f = this.value;
            return (this.count == 0) ? 0.0f : (value_f / this.count);
        }
        
        public void incAggValue(final Integer arg) {
            if (arg != null) {
                ++this.count;
                this.value += arg;
            }
        }
        
        public void decAggValue(final Integer arg) {
            if (arg != null) {
                --this.count;
                this.value -= arg;
            }
        }
    }
    
    public static class LagRateAverageCalculator
    {
        long value;
        int count;
        
        public LagRateAverageCalculator() {
            this.value = 0L;
            this.count = 0;
        }
        
        public long getAggValue() {
            final long value_f = this.value;
            return (this.count == 0) ? 0L : (value_f / this.count);
        }
        
        public void incAggValue(final Long arg) {
            if (arg != null) {
                ++this.count;
                this.value += arg;
            }
        }
        
        public void decAggValue(final Long arg) {
            if (arg != null) {
                --this.count;
                this.value -= arg;
            }
        }
    }
    
    public static class AvgLong
    {
        long value;
        int count;
        
        public AvgLong() {
            this.value = 0L;
            this.count = 0;
        }
        
        public Float getAggValue() {
            final float value_f = this.value;
            return (this.count == 0) ? 0.0f : (value_f / this.count);
        }
        
        public void incAggValue(final Long arg) {
            if (arg != null) {
                ++this.count;
                this.value += arg;
            }
        }
        
        public void decAggValue(final Long arg) {
            if (arg != null) {
                --this.count;
                this.value -= arg;
            }
        }
    }
    
    public static class AvgFloat
    {
        float value;
        long count;
        
        public AvgFloat() {
            this.value = 0.0f;
            this.count = 0L;
        }
        
        public Float getAggValue() {
            return (this.count == 0L) ? 0.0f : (this.value / this.count);
        }
        
        public void incAggValue(final Float arg) {
            if (arg != null) {
                ++this.count;
                this.value += arg;
            }
        }
        
        public void decAggValue(final Float arg) {
            if (arg != null) {
                --this.count;
                this.value -= arg;
            }
        }
    }
    
    public static class AvgDouble
    {
        double value;
        long count;
        
        public AvgDouble() {
            this.value = 0.0;
            this.count = 0L;
        }
        
        public Double getAggValue() {
            return (this.count == 0L) ? 0.0 : (this.value / this.count);
        }
        
        public void incAggValue(final Double arg) {
            if (arg != null) {
                ++this.count;
                this.value += arg;
            }
        }
        
        public void decAggValue(final Double arg) {
            if (arg != null) {
                --this.count;
                this.value -= arg;
            }
        }
    }
    
    public static class AvgNumber
    {
        double value;
        long count;
        
        public AvgNumber() {
            this.value = 0.0;
            this.count = 0L;
        }
        
        public Double getAggValue() {
            return (this.count == 0L) ? 0.0 : (this.value / this.count);
        }
        
        public void incAggValue(final Number arg) {
            if (arg != null) {
                ++this.count;
                this.value += arg.doubleValue();
            }
        }
        
        public void decAggValue(final Number arg) {
            if (arg != null) {
                --this.count;
                this.value -= arg.doubleValue();
            }
        }
    }
    
    public static class StdDevPNumber
    {
        Double m;
        Double S;
        long count;
        
        public StdDevPNumber() {
            this.m = 0.0;
            this.S = 0.0;
            this.count = 0L;
        }
        
        public Double getAggValue() {
            final double stddev = (this.count == 0L) ? 0.0 : Math.sqrt(this.S / this.count);
            return stddev;
        }
        
        public void incAggValue(final Number arg) {
            if (arg != null) {
                final Double prev_mean = this.m;
                final double argd = arg.doubleValue();
                ++this.count;
                this.m += (argd - this.m) / this.count;
                this.S += (argd - this.m) * (argd - prev_mean);
                this.S = ((this.S < 0.0) ? 0.0 : this.S);
            }
        }
        
        public void decAggValue(final Number arg) {
            if (arg != null) {
                final Double prev_mean = this.m;
                final double argd = arg.doubleValue();
                --this.count;
                if (this.count == 0L) {
                    this.m = 0.0;
                    this.S = 0.0;
                }
                else {
                    this.m -= (argd - this.m) / this.count;
                    this.S -= (argd - this.m) * (argd - prev_mean);
                    this.S = ((this.S < 0.0) ? 0.0 : this.S);
                }
            }
        }
    }
    
    public static class StdDevSNumber
    {
        Double m;
        Double S;
        long count;
        
        public StdDevSNumber() {
            this.m = 0.0;
            this.S = 0.0;
            this.count = 0L;
        }
        
        public Double getAggValue() {
            return (this.count <= 1L) ? 0.0 : Math.sqrt(this.S / (this.count - 1L));
        }
        
        public void incAggValue(final Number arg) {
            if (arg != null) {
                final Double prev_mean = this.m;
                final double argd = arg.doubleValue();
                ++this.count;
                this.m += (argd - this.m) / this.count;
                this.S += (argd - this.m) * (argd - prev_mean);
                this.S = ((this.S < 0.0) ? 0.0 : this.S);
            }
        }
        
        public void decAggValue(final Number arg) {
            if (arg != null) {
                final Double prev_mean = this.m;
                final double argd = arg.doubleValue();
                --this.count;
                if (this.count == 0L) {
                    this.m = 0.0;
                    this.S = 0.0;
                }
                else {
                    this.m -= (argd - this.m) / this.count;
                    this.S -= (argd - this.m) * (argd - prev_mean);
                    this.S = ((this.S < 0.0) ? 0.0 : this.S);
                }
            }
        }
    }
    
    public static class CountNotNull
    {
        long count;
        
        public CountNotNull() {
            this.count = 0L;
        }
        
        public long getAggValue() {
            return this.count;
        }
        
        public void incAggValue(final Object arg) {
            if (arg != null) {
                ++this.count;
            }
        }
        
        public void decAggValue(final Object arg) {
            if (arg != null) {
                --this.count;
            }
        }
    }
    
    public static class CountNotNullDistinct
    {
        Map<Object, Integer> map;
        
        public CountNotNullDistinct() {
            this.map = new HashMap<Object, Integer>();
        }
        
        public int getAggValue() {
            return this.map.size();
        }
        
        public void incAggValue(final Object arg) {
            if (arg != null) {
                final Integer i = this.map.get(arg);
                if (i == null) {
                    this.map.put(arg, 1);
                }
                else {
                    this.map.put(arg, i + 1);
                }
            }
        }
        
        public void decAggValue(final Object arg) {
            if (arg != null) {
                final Integer i = this.map.get(arg);
                if (i == 1) {
                    this.map.remove(arg);
                }
                else {
                    this.map.put(arg, i - 1);
                }
            }
        }
    }
    
    public static class CountAny
    {
        long count;
        
        public CountAny() {
            this.count = 0L;
        }
        
        public long getAggValue() {
            return this.count;
        }
        
        public void incAggValue() {
            ++this.count;
        }
        
        public void decAggValue() {
            --this.count;
        }
    }
    
    public static class FirstAny
    {
        LinkedList<Object> list;
        
        public FirstAny() {
            this.list = new LinkedList<Object>();
        }
        
        public Object getAggValue() {
            if (this.list.isEmpty()) {
                return null;
            }
            return this.list.getFirst();
        }
        
        public void incAggValue(final Object arg) {
            this.list.addLast(arg);
        }
        
        public void decAggValue(final Object arg) {
            if (!this.list.isEmpty()) {
                this.list.removeFirst();
            }
        }
    }
    
    public static class FirstInt
    {
        LinkedList<Integer> list;
        
        public FirstInt() {
            this.list = new LinkedList<Integer>();
        }
        
        public Integer getAggValue() {
            if (this.list.isEmpty()) {
                return null;
            }
            return this.list.getFirst();
        }
        
        public void incAggValue(final Integer arg) {
            this.list.addLast(arg);
        }
        
        public void decAggValue(final Integer arg) {
            if (!this.list.isEmpty()) {
                this.list.removeFirst();
            }
        }
    }
    
    public static class FirstLong
    {
        LinkedList<Long> list;
        
        public FirstLong() {
            this.list = new LinkedList<Long>();
        }
        
        public Long getAggValue() {
            if (this.list.isEmpty()) {
                return null;
            }
            return this.list.getFirst();
        }
        
        public void incAggValue(final Long arg) {
            this.list.addLast(arg);
        }
        
        public void decAggValue(final Long arg) {
            if (!this.list.isEmpty()) {
                this.list.removeFirst();
            }
        }
    }
    
    public static class FirstFloat
    {
        LinkedList<Float> list;
        
        public FirstFloat() {
            this.list = new LinkedList<Float>();
        }
        
        public Float getAggValue() {
            if (this.list.isEmpty()) {
                return null;
            }
            return this.list.getFirst();
        }
        
        public void incAggValue(final Float arg) {
            this.list.addLast(arg);
        }
        
        public void decAggValue(final Float arg) {
            if (!this.list.isEmpty()) {
                this.list.removeFirst();
            }
        }
    }
    
    public static class FirstDouble
    {
        LinkedList<Double> list;
        
        public FirstDouble() {
            this.list = new LinkedList<Double>();
        }
        
        public Double getAggValue() {
            if (this.list.isEmpty()) {
                return null;
            }
            return this.list.getFirst();
        }
        
        public void incAggValue(final Double arg) {
            this.list.addLast(arg);
        }
        
        public void decAggValue(final Double arg) {
            if (!this.list.isEmpty()) {
                this.list.removeFirst();
            }
        }
    }
    
    public static class FirstString
    {
        LinkedList<String> list;
        
        public FirstString() {
            this.list = new LinkedList<String>();
        }
        
        public String getAggValue() {
            if (this.list.isEmpty()) {
                return null;
            }
            return this.list.getFirst();
        }
        
        public void incAggValue(final String arg) {
            this.list.addLast(arg);
        }
        
        public void decAggValue(final String arg) {
            if (!this.list.isEmpty()) {
                this.list.removeFirst();
            }
        }
    }
    
    public static class FirstDateTime
    {
        LinkedList<DateTime> list;
        
        public FirstDateTime() {
            this.list = new LinkedList<DateTime>();
        }
        
        public DateTime getAggValue() {
            if (this.list.isEmpty()) {
                return null;
            }
            return this.list.getFirst();
        }
        
        public void incAggValue(final DateTime arg) {
            this.list.addLast(arg);
        }
        
        public void decAggValue(final DateTime arg) {
            if (!this.list.isEmpty()) {
                this.list.removeFirst();
            }
        }
    }
    
    public static class LastAny
    {
        Object last;
        
        public Object getAggValue() {
            return this.last;
        }
        
        public void incAggValue(final Object arg) {
            this.last = arg;
        }
        
        public void decAggValue(final Object arg) {
        }
    }
    
    public static class ListByKey
    {
        String lastKey;
        List items;
        
        public ListByKey() {
            this.lastKey = null;
            this.items = null;
        }
        
        public List getAggValue() {
            return this.items;
        }
        
        public void incAggValue(final String key, final Object item) {
            if (key == null || key.trim().length() == 0) {
                this.items = null;
                this.lastKey = null;
                return;
            }
            if (!key.equals(this.lastKey)) {
                this.items = null;
            }
            if (this.items == null) {
                this.items = new CopyOnWriteArrayList();
            }
            this.items.add(item);
            this.lastKey = key;
        }
        
        public void decAggValue(final String key, final Object item) {
            if (this.items != null) {
                this.items.remove(item);
            }
        }
    }
    
    public static class LastInt
    {
        Integer last;
        
        public Integer getAggValue() {
            return this.last;
        }
        
        public void incAggValue(final Integer arg) {
            this.last = arg;
        }
        
        public void decAggValue(final Integer arg) {
        }
    }
    
    public static class LastLong
    {
        Long last;
        
        public Long getAggValue() {
            return this.last;
        }
        
        public void incAggValue(final Long arg) {
            this.last = arg;
        }
        
        public void decAggValue(final Long arg) {
        }
    }
    
    public static class LastFloat
    {
        Float last;
        
        public Float getAggValue() {
            return this.last;
        }
        
        public void incAggValue(final Float arg) {
            this.last = arg;
        }
        
        public void decAggValue(final Float arg) {
        }
    }
    
    public static class LastDouble
    {
        Double last;
        
        public Double getAggValue() {
            return this.last;
        }
        
        public void incAggValue(final Double arg) {
            this.last = arg;
        }
        
        public void decAggValue(final Double arg) {
        }
    }
    
    public static class LastString
    {
        String last;
        
        public String getAggValue() {
            return this.last;
        }
        
        public void incAggValue(final String arg) {
            this.last = arg;
        }
        
        public void decAggValue(final String arg) {
        }
    }
    
    public static class LastDateTime
    {
        DateTime last;
        
        public DateTime getAggValue() {
            return this.last;
        }
        
        public void incAggValue(final DateTime arg) {
            this.last = arg;
        }
        
        public void decAggValue(final DateTime arg) {
        }
    }
    
    public static class MinInt
    {
        MinMaxTree<Integer> tree;
        
        public MinInt() {
            this.tree = new MinMaxTree<Integer>();
        }
        
        public Integer getAggValue() {
            return this.tree.getMin();
        }
        
        public void incAggValue(final Integer arg) {
            this.tree.addKey(arg);
        }
        
        public void decAggValue(final Integer arg) {
            this.tree.removeKey(arg);
        }
    }
    
    public static class MinLong
    {
        MinMaxTree<Long> tree;
        
        public MinLong() {
            this.tree = new MinMaxTree<Long>();
        }
        
        public Long getAggValue() {
            return this.tree.getMin();
        }
        
        public void incAggValue(final Long arg) {
            this.tree.addKey(arg);
        }
        
        public void decAggValue(final Long arg) {
            this.tree.removeKey(arg);
        }
    }
    
    public static class MinFloat
    {
        MinMaxTree<Float> tree;
        
        public MinFloat() {
            this.tree = new MinMaxTree<Float>();
        }
        
        public Float getAggValue() {
            return this.tree.getMin();
        }
        
        public void incAggValue(final Float arg) {
            this.tree.addKey(arg);
        }
        
        public void decAggValue(final Float arg) {
            this.tree.removeKey(arg);
        }
    }
    
    public static class MinDouble
    {
        MinMaxTree<Double> tree;
        
        public MinDouble() {
            this.tree = new MinMaxTree<Double>();
        }
        
        public Double getAggValue() {
            return this.tree.getMin();
        }
        
        public void incAggValue(final Double arg) {
            this.tree.addKey(arg);
        }
        
        public void decAggValue(final Double arg) {
            this.tree.removeKey(arg);
        }
    }
    
    public static class MinString
    {
        MinMaxTree<String> tree;
        
        public MinString() {
            this.tree = new MinMaxTree<String>();
        }
        
        public String getAggValue() {
            return this.tree.getMin();
        }
        
        public void incAggValue(final String arg) {
            this.tree.addKey(arg);
        }
        
        public void decAggValue(final String arg) {
            this.tree.removeKey(arg);
        }
    }
    
    public static class MaxInt
    {
        MinMaxTree<Integer> tree;
        
        public MaxInt() {
            this.tree = new MinMaxTree<Integer>();
        }
        
        public Integer getAggValue() {
            return this.tree.getMax();
        }
        
        public void incAggValue(final Integer arg) {
            this.tree.addKey(arg);
        }
        
        public void decAggValue(final Integer arg) {
            this.tree.removeKey(arg);
        }
    }
    
    public static class MaxLong
    {
        MinMaxTree<Long> tree;
        
        public MaxLong() {
            this.tree = new MinMaxTree<Long>();
        }
        
        public Long getAggValue() {
            return this.tree.getMax();
        }
        
        public void incAggValue(final Long arg) {
            this.tree.addKey(arg);
        }
        
        public void decAggValue(final Long arg) {
            this.tree.removeKey(arg);
        }
    }
    
    public static class MaxFloat
    {
        MinMaxTree<Float> tree;
        
        public MaxFloat() {
            this.tree = new MinMaxTree<Float>();
        }
        
        public Float getAggValue() {
            return this.tree.getMax();
        }
        
        public void incAggValue(final Float arg) {
            this.tree.addKey(arg);
        }
        
        public void decAggValue(final Float arg) {
            this.tree.removeKey(arg);
        }
    }
    
    public static class MaxDouble
    {
        MinMaxTree<Double> tree;
        
        public MaxDouble() {
            this.tree = new MinMaxTree<Double>();
        }
        
        public Double getAggValue() {
            return this.tree.getMax();
        }
        
        public void incAggValue(final Double arg) {
            this.tree.addKey(arg);
        }
        
        public void decAggValue(final Double arg) {
            this.tree.removeKey(arg);
        }
    }
    
    public static class MaxString
    {
        MinMaxTree<String> tree;
        
        public MaxString() {
            this.tree = new MinMaxTree<String>();
        }
        
        public String getAggValue() {
            return this.tree.getMax();
        }
        
        public void incAggValue(final String arg) {
            this.tree.addKey(arg);
        }
        
        public void decAggValue(final String arg) {
            this.tree.removeKey(arg);
        }
    }
    
    public static class MaxOccursString
    {
        MinMaxTree<String> tree;
        
        public MaxOccursString() {
            this.tree = new MinMaxTree<String>();
        }
        
        public String getAggValue() {
            return this.tree.getMaxOccurs();
        }
        
        public void incAggValue(final String arg) {
            this.tree.addKey(arg);
        }
        
        public void decAggValue(final String arg) {
            this.tree.removeKey(arg);
        }
    }
}
