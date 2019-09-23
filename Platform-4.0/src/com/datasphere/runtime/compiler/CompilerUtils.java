package com.datasphere.runtime.compiler;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.TraceClassVisitor;

import com.datasphere.runtime.exceptions.AmbiguousSignature;
import com.datasphere.runtime.exceptions.SignatureNotFound;
import com.datasphere.runtime.utils.Factory;
import com.datasphere.runtime.utils.NamePolicy;

import javassist.CtMethod;
import javassist.Modifier;
import javassist.bytecode.InstructionPrinter;

public class CompilerUtils
{
    private static final String[] javaKeywords;
    private static Logger logger;
    private static final Map<Class<?>, Integer> primitiveTypeMap;
    private static final Map<Class<?>, Integer> boxingTypeMap;
    public static final int VOID = 1;
    public static final int BOOLEAN = 2;
    public static final int BYTE = 3;
    public static final int CHAR = 4;
    public static final int SHORT = 5;
    public static final int INTEGER = 6;
    public static final int LONG = 7;
    public static final int FLOAT = 8;
    public static final int DOUBLE = 9;
    public static final int MAXTYPE = 10;
    private static final boolean[][] wideningMap;
    private static final Map<String, Class<?>> shortcuts;
    private static final Map<String, Class<?>> primitives;
    
    public static boolean isJavaKeyword(final String s) {
        return Arrays.binarySearch(CompilerUtils.javaKeywords, s, new Comparator<String>() {
            @Override
            public int compare(final String o1, final String o2) {
                return o1.compareTo(o2);
            }
        }) >= 0;
    }
    
    public static String capitalize(final String s) {
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }
    
    private static Class<?>[] getPrimitiveTypes() {
        final Class<?>[] primitiveTypes = (Class<?>[])new Class[] { Void.TYPE, Boolean.TYPE, Character.TYPE, Byte.TYPE, Short.TYPE, Integer.TYPE, Long.TYPE, Float.TYPE, Double.TYPE };
        return primitiveTypes;
    }
    
    private static Class<?>[] getBoxingTypes() {
        final Class<?>[] boxingTypes = (Class<?>[])new Class[] { Void.class, Boolean.class, Character.class, Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class };
        return boxingTypes;
    }
    
    public static boolean canBeWidened(final int fromTypeID, final int toTypeID) {
        return CompilerUtils.wideningMap[fromTypeID][toTypeID];
    }
    
    public static int getPrimitiveOrBoxingTypeID(final Class<?> type) {
        final int t = getPrimitiveTypeID(type);
        return (t != 0) ? t : getBoxingTypeID(type);
    }
    
    public static int getPrimitiveTypeID(final Class<?> type) {
        final Integer i = CompilerUtils.primitiveTypeMap.get(type);
        return (i == null) ? 0 : i;
    }
    
    public static int getBoxingTypeID(final Class<?> type) {
        final Integer i = CompilerUtils.boxingTypeMap.get(type);
        return (i == null) ? 0 : i;
    }
    
    public static int getBaseTypeID(final Class<?> type) {
        final Integer i = type.isPrimitive() ? CompilerUtils.primitiveTypeMap.get(type) : CompilerUtils.boxingTypeMap.get(type);
        return (i == null) ? 0 : i;
    }
    
    public static boolean compatible(final Class<?> t1, final Class<?> t2) {
        return t1.isAssignableFrom(t2) || t2.isAssignableFrom(t1);
    }
    
    public static boolean compatiblePrimitive(final Class<?> t1, final Class<?> t2) {
        final int pt1 = getPrimitiveTypeID(t1);
        final int pt2 = getPrimitiveTypeID(t2);
        return pt1 == pt2 || (pt1 >= 3 && pt2 >= 3);
    }
    
    public static boolean isCastable(final Class<?> toType, final Class<?> fromType) {
        if (isParam(fromType)) {
            return true;
        }
        if (isNull(fromType)) {
            return !toType.isPrimitive();
        }
        if (compatible(toType, fromType)) {
            return true;
        }
        if (toType.isPrimitive() && fromType.isPrimitive()) {
            return compatiblePrimitive(toType, fromType);
        }
        if (fromType.isPrimitive()) {
            final Class<?> t = getUnboxingType(toType);
            return (t != null && compatiblePrimitive(fromType, t)) || compatible(toType, getBoxingType(fromType));
        }
        if (toType.isPrimitive()) {
            final Class<?> t = getUnboxingType(fromType);
            return (t != null && compatiblePrimitive(toType, t)) || compatible(fromType, getBoxingType(toType));
        }
        final Class<?> t2 = getUnboxingType(toType);
        final Class<?> t3 = getUnboxingType(fromType);
        return t2 != null && t3 != null && compatiblePrimitive(t2, t3);
    }
    
    public static boolean isConvertibleByWidening(final Class<?> formal, final Class<?> actual) {
        if (isNull(actual)) {
            return !formal.isPrimitive();
        }
        if (formal.isAssignableFrom(actual)) {
            return true;
        }
        if (formal.isPrimitive() && actual.isPrimitive()) {
            final int a = getPrimitiveTypeID(actual);
            final int f = getPrimitiveTypeID(formal);
            return canBeWidened(a, f);
        }
        return false;
    }
    
    public static boolean isConvertibleWithBoxing(final Class<?> formal, final Class<?> actual) {
        if (isParam(actual)) {
            return true;
        }
        if (isNull(actual)) {
            return !formal.isPrimitive();
        }
        if (formal.isAssignableFrom(actual)) {
            return true;
        }
        if (actual.isAssignableFrom(formal)) {
            return true;
        }
        if (!formal.isPrimitive()) {
            return actual.isPrimitive() && formal.isAssignableFrom(getBoxingType(actual));
        }
        final int f = getPrimitiveTypeID(formal);
        if (actual.isPrimitive()) {
            final int a = getPrimitiveTypeID(actual);
            return canBeWidened(a, f);
        }
        final int a = getBoxingTypeID(actual);
        return canBeWidened(a, f);
    }
    
    public static Class<?> getPrimitiveType(final int typeID) {
        switch (typeID) {
            case 2: {
                return Boolean.TYPE;
            }
            case 4: {
                return Character.TYPE;
            }
            case 3: {
                return Byte.TYPE;
            }
            case 9: {
                return Double.TYPE;
            }
            case 8: {
                return Float.TYPE;
            }
            case 6: {
                return Integer.TYPE;
            }
            case 7: {
                return Long.TYPE;
            }
            case 5: {
                return Short.TYPE;
            }
            default: {
                return null;
            }
        }
    }
    
    public static Class<?> getBoxingType(final int typeID) {
        switch (typeID) {
            case 3: {
                return Byte.class;
            }
            case 9: {
                return Double.class;
            }
            case 8: {
                return Float.class;
            }
            case 6: {
                return Integer.class;
            }
            case 7: {
                return Long.class;
            }
            case 5: {
                return Short.class;
            }
            case 2: {
                return Boolean.class;
            }
            case 4: {
                return Character.class;
            }
            default: {
                return null;
            }
        }
    }
    
    public static Class<?> getBoxingType(final Class<?> c) {
        final int typeID = getPrimitiveTypeID(c);
        return getBoxingType(typeID);
    }
    
    public static Class<?> getUnboxingType(final Class<?> c) {
        final int typeID = getBoxingTypeID(c);
        return getPrimitiveType(typeID);
    }
    
    public static boolean isBoxingType(final Class<?> c) {
        final int typeID = getBoxingTypeID(c);
        return typeID > 1;
    }
    
    private static Class<?> getCommonPrimitiveType(int t1, int t2) {
        if (t1 == t2) {
            return getPrimitiveType(t1);
        }
        if (t1 < 3 || t2 < 3) {
            return null;
        }
        if (t1 == 4) {
            t1 = 6;
        }
        else if (t2 == 4) {
            t2 = 6;
        }
        return getPrimitiveType((t1 > t2) ? t1 : t2);
    }
    
    public static boolean isParam(final Class<?> t) {
        return ParamTypeUndefined.class.isAssignableFrom(t);
    }
    
    public static boolean isNull(final Class<?> t) {
        return NullType.class.isAssignableFrom(t);
    }
    
    public static Class<?> getDefaultTypeIfCannotInfer(final Class<?> t, final Class<?> defaulttype) {
        return isParam(t) ? defaulttype : t;
    }
    
    public static Class<?> makeReferenceType(final Class<?> c) {
        return c.isPrimitive() ? getBoxingType(c) : c;
    }
    
    public static Class<?> getCommonSuperType(final Class<?> c1, final Class<?> c2) {
        if (isParam(c1)) {
            return c2;
        }
        if (isParam(c2)) {
            return c1;
        }
        if (isNull(c1)) {
            return makeReferenceType(c2);
        }
        if (isNull(c2)) {
            return makeReferenceType(c1);
        }
        if (c1.isAssignableFrom(c2)) {
            return c1;
        }
        if (c2.isAssignableFrom(c1)) {
            return c2;
        }
        final int t1 = getPrimitiveOrBoxingTypeID(c1);
        if (t1 == 0) {
            return null;
        }
        final int t2 = getPrimitiveOrBoxingTypeID(c2);
        if (t1 == 0) {
            return null;
        }
        return getCommonPrimitiveType(t1, t2);
    }
    
    public static boolean isComparable(final Class<?> c) {
        return c.isPrimitive() || Comparable.class.isAssignableFrom(c);
    }
    
    private static Map<String, Class<?>> loadJavaLangShortcuts() {
        final Class<?> klass = String.class;
        final URL location = klass.getResource('/' + klass.getName().replace('.', '/') + ".class");
        final String path = location.getPath();
        final String file = path.substring(0, path.indexOf("!"));
        URI syslibUri = null;
        try {
            syslibUri = new URI(file);
        }
        catch (URISyntaxException e2) {
            throw new RuntimeException(e2);
        }
        final Pattern pattern = Pattern.compile("^java/lang/\\w+\\.class");
        final Map<String, Class<?>> retval = Factory.makeNameMap();
        for (final Class<?> c : getPrimitiveTypes()) {
            retval.put(c.getName(), c);
        }
        try (final ZipFile zf = new ZipFile(new File(syslibUri))) {
            final Enumeration<?> e3 = zf.entries();
            while (e3.hasMoreElements()) {
                final ZipEntry ze = (ZipEntry)e3.nextElement();
                final String fileName = ze.getName();
                if (pattern.matcher(fileName).matches()) {
                    final String classname = fileName.replace('/', '.').replace(".class", "");
                    final String key = classname.substring(classname.lastIndexOf(".") + 1);
                    try {
                        final Class<?> c2 = Class.forName(classname);
                        assert key.equals(c2.getSimpleName());
                        retval.put(key, c2);
                    }
                    catch (ClassNotFoundException e4) {
                        CompilerUtils.logger.error((Object)"Could not find class loading shortcuts", (Throwable)e4);
                    }
                }
            }
        }
        catch (ZipException e5) {
            throw new RuntimeException(e5);
        }
        catch (IOException e6) {
            throw new RuntimeException(e6);
        }
        final Class<?> dtClass = DateTime.class;
        retval.put(dtClass.getSimpleName(), dtClass);
        return retval;
    }
    
    private static Map<String, Class<?>> loadJavaLangPrimitiveTypes() {
        final Map<String, Class<?>> retval = Factory.makeNameMap();
        for (final Class<?> c : getPrimitiveTypes()) {
            retval.put(c.getName(), c);
        }
        return retval;
    }
    
    public static Class<?> getClassNameShortcut(final String name) {
        return CompilerUtils.shortcuts.get(name);
    }
    
    public static Class<?> getPrimitiveClassByName(final String name) {
        return CompilerUtils.primitives.get(name);
    }
    
    public static Class<?> toArrayType(final Class<?> c) {
        return Array.newInstance(c, 0).getClass();
    }
    
    public static boolean isSubclassOf(final Class<?> subclass, final Class<?> superclass) {
        if (superclass.isAssignableFrom(subclass)) {
            return true;
        }
        if (subclass.isPrimitive() && superclass.isPrimitive()) {
            final int psubclass = getPrimitiveTypeID(subclass);
            final int psuperclass = getPrimitiveTypeID(superclass);
            return psubclass < psuperclass && psubclass >= 3;
        }
        return false;
    }
    
    public static Class<?>[] moreSpecificSignature(final Class<?>[] a, final Class<?>[] b) {
        if (a.length != b.length) {
            return null;
        }
        Class<?>[] r = null;
        for (int i = 0; i < a.length; ++i) {
            if (a[i] != b[i]) {
                if (r == a) {
                    if (!isSubclassOf(a[i], b[i])) {
                        return null;
                    }
                }
                else if (r == b) {
                    if (!isSubclassOf(b[i], a[i])) {
                        return null;
                    }
                }
                else if (isSubclassOf(a[i], b[i])) {
                    r = a;
                }
                else {
                    if (!isSubclassOf(b[i], a[i])) {
                        return null;
                    }
                    r = b;
                }
            }
        }
        return r;
    }
    
    public static Class<?>[] moreSpecificSignatureVarargs(final Class<?>[] a, final Class<?>[] b) {
        Class<?>[] shorter = null;
        Class<?>[] longer = null;
        if (a.length < b.length) {
            shorter = (Class<?>[])a.clone();
            longer = (Class<?>[])b.clone();
        }
        else {
            shorter = (Class<?>[])b.clone();
            longer = (Class<?>[])a.clone();
        }
        Class<?>[] r = null;
        for (int i = 0; i < shorter.length - 1; ++i) {
            if (shorter[i] != longer[i]) {
                if (r == shorter) {
                    if (!isSubclassOf(shorter[i], longer[i])) {
                        return null;
                    }
                }
                else if (r == longer) {
                    if (!isSubclassOf(longer[i], shorter[i])) {
                        return null;
                    }
                }
                else if (isSubclassOf(shorter[i], longer[i])) {
                    r = shorter;
                }
                else {
                    if (!isSubclassOf(longer[i], shorter[i])) {
                        return null;
                    }
                    r = longer;
                }
            }
        }
        final Class<?> varArgsType = shorter[shorter.length - 1];
        for (int j = shorter.length - 1; j < longer.length; ++j) {
            if (varArgsType != longer[j]) {
                if (r == shorter) {
                    if (!isSubclassOf(varArgsType, longer[j])) {
                        return null;
                    }
                }
                else if (r == longer) {
                    if (!isSubclassOf(longer[j], varArgsType)) {
                        return null;
                    }
                }
                else if (isSubclassOf(varArgsType, longer[j])) {
                    r = shorter;
                }
                else {
                    if (!isSubclassOf(longer[j], varArgsType)) {
                        return null;
                    }
                    r = longer;
                }
            }
        }
        return r;
    }
    
    public static boolean compareSignature(final Class<?>[] formal, final Class<?>[] actual) {
        if (formal.length != actual.length) {
            return false;
        }
        for (int i = 0; i < formal.length; ++i) {
            if (!isConvertibleByWidening(formal[i], actual[i])) {
                return false;
            }
        }
        return true;
    }
    
    public static boolean compareSignatureWithBoxing(final Class<?>[] formal, final Class<?>[] actual) {
        if (formal.length != actual.length) {
            return false;
        }
        for (int i = 0; i < formal.length; ++i) {
            if (!isConvertibleWithBoxing(formal[i], actual[i])) {
                return false;
            }
        }
        return true;
    }
    
    public static boolean compareSignatureWithBoxingAndVarargs(final Class<?>[] formal, final Class<?>[] actual) {
        if (actual.length < formal.length - 1) {
            return false;
        }
        for (int i = 0; i < formal.length - 1; ++i) {
            if (!isConvertibleWithBoxing(formal[i], actual[i])) {
                return false;
            }
        }
        final Class<?> varArgsType = formal[formal.length - 1].getComponentType();
        for (int j = formal.length - 1; j < actual.length; ++j) {
            if (!isConvertibleWithBoxing(varArgsType, actual[j])) {
                return false;
            }
        }
        return true;
    }
    
    public static Constructor<?> moreSpecificConstructor(final Constructor<?> a, final Constructor<?> b) {
        final Class<?>[] pa = a.getParameterTypes();
        final Class<?>[] pb = b.getParameterTypes();
        final Class<?>[] p = (a.isVarArgs() && b.isVarArgs()) ? moreSpecificSignatureVarargs(pa, pb) : moreSpecificSignature(pa, pb);
        if (p == null) {
            return null;
        }
        return (p == pa) ? a : b;
    }
    
    public static Constructor<?> findConstructor(final Class<?> klass, final Class<?>[] signature) throws AmbiguousSignature, SignatureNotFound {
        try {
            final Constructor<?> con = klass.getConstructor(signature);
            return con;
        }
        catch (NoSuchMethodException | SecurityException ex2) {
            Constructor<?> r = null;
            for (final Constructor<?> co : klass.getConstructors()) {
                if (compareSignature(co.getParameterTypes(), signature)) {
                    if (r == null) {
                        r = co;
                    }
                    else {
                        r = moreSpecificConstructor(co, r);
                        if (r == null) {
                            throw new AmbiguousSignature();
                        }
                    }
                }
            }
            if (r != null) {
                return r;
            }
            for (final Constructor<?> co : klass.getConstructors()) {
                if (compareSignatureWithBoxing(co.getParameterTypes(), signature)) {
                    if (r == null) {
                        r = co;
                    }
                    else {
                        r = moreSpecificConstructor(co, r);
                        if (r == null) {
                            throw new AmbiguousSignature();
                        }
                    }
                }
            }
            if (r != null) {
                return r;
            }
            for (final Constructor<?> co : klass.getConstructors()) {
                if (co.isVarArgs() && compareSignatureWithBoxingAndVarargs(co.getParameterTypes(), signature)) {
                    if (r == null) {
                        r = co;
                    }
                    else {
                        r = moreSpecificConstructor(co, r);
                        if (r == null) {
                            throw new AmbiguousSignature();
                        }
                    }
                }
            }
            if (r != null) {
                return r;
            }
            throw new SignatureNotFound();
        }
    }
    
    public static Method moreSpecificMethod(final Method a, final Method b) {
        final Class<?>[] pa = a.getParameterTypes();
        final Class<?>[] pb = b.getParameterTypes();
        final Class<?>[] p = (a.isVarArgs() && b.isVarArgs()) ? moreSpecificSignatureVarargs(pa, pb) : moreSpecificSignature(pa, pb);
        if (p == null) {
            return null;
        }
        return (p == pa) ? a : b;
    }
    
    public static Method findMethod(final Class<?> klass, final String name, final Class<?>[] signature) throws AmbiguousSignature, SignatureNotFound, NoSuchMethodException {
        try {
            final Method me = klass.getMethod(name, signature);
            return me;
        }
        catch (NoSuchMethodException | SecurityException ex2) {
            Method r = null;
            boolean hasMethodWithSuchName = false;
            for (final Method me2 : klass.getMethods()) {
                if (NamePolicy.isEqual(me2.getName(), name)) {
                    hasMethodWithSuchName = true;
                    if (compareSignature(me2.getParameterTypes(), signature)) {
                        if (r == null) {
                            r = me2;
                        }
                        else {
                            r = moreSpecificMethod(me2, r);
                            if (r == null) {
                                throw new AmbiguousSignature();
                            }
                        }
                    }
                }
            }
            if (!hasMethodWithSuchName) {
                throw new NoSuchMethodException();
            }
            if (r != null) {
                return r;
            }
            for (final Method me2 : klass.getMethods()) {
                if (NamePolicy.isEqual(me2.getName(), name) && compareSignatureWithBoxing(me2.getParameterTypes(), signature)) {
                    if (r == null) {
                        r = me2;
                    }
                    else {
                        r = moreSpecificMethod(me2, r);
                        if (r == null) {
                            throw new AmbiguousSignature();
                        }
                    }
                }
            }
            if (r != null) {
                return r;
            }
            for (final Method me2 : klass.getMethods()) {
                if (NamePolicy.isEqual(me2.getName(), name) && me2.isVarArgs() && compareSignatureWithBoxingAndVarargs(me2.getParameterTypes(), signature)) {
                    if (r == null) {
                        r = me2;
                    }
                    else {
                        r = moreSpecificMethod(me2, r);
                        if (r == null) {
                            throw new AmbiguousSignature();
                        }
                    }
                }
            }
            if (r != null) {
                return r;
            }
            throw new SignatureNotFound();
        }
    }
    
    public static void printBytecode(final byte[] code, final PrintStream out) {
        final ClassReader cr = new ClassReader(code);
        cr.accept((ClassVisitor)new TraceClassVisitor(new PrintWriter(out)), 0);
    }
    
    public static String verifyBytecode(final byte[] code, final ClassLoader cl) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        final ClassReader cr = new ClassReader(code);
        CheckClassAdapter.verify(cr, cl, false, pw);
        final String report = sw.toString();
        return (report.length() > 0) ? report : null;
    }
    
    public static void writeBytecodeToFile(final byte[] code, String className, String dirname) throws IOException {
        if (dirname == null) {
            dirname = System.getProperty("user.dir");
        }
        if (className == null) {
            className = getClassName(code);
        }
        final File dir = new File(dirname);
        final File file = new File(dir, className.replace('.', '/') + ".class");
        final File parent = file.getParentFile();
        if (!parent.exists() && !parent.mkdirs()) {
            throw new IllegalStateException("Couldn't create dir: " + parent);
        }
        final FileOutputStream out = new FileOutputStream(file);
        out.write(code);
        out.close();
    }
    
    public static String getClassName(final byte[] code) {
        try {
            final DataInputStream s = new DataInputStream(new ByteArrayInputStream(code));
            s.skipBytes(8);
            final int const_pool_count = (s.readShort() & 0xFFFF) - 1;
            final int[] classes = new int[const_pool_count];
            final String[] strings = new String[const_pool_count];
            for (int i = 0; i < const_pool_count; ++i) {
                final int tag = s.read();
                switch (tag) {
                    case 1: {
                        strings[i] = s.readUTF();
                        break;
                    }
                    case 7: {
                        classes[i] = (s.readShort() & 0xFFFF);
                        break;
                    }
                    case 8: {
                        s.skipBytes(2);
                        break;
                    }
                    case 5:
                    case 6: {
                        s.skipBytes(8);
                        ++i;
                        break;
                    }
                    default: {
                        s.skipBytes(4);
                        break;
                    }
                }
            }
            s.skipBytes(2);
            final int this_class_index = s.readShort() & 0xFFFF;
            final String className = strings[classes[this_class_index - 1] - 1];
            return className.replace('/', '.');
        }
        catch (IOException e) {
            return null;
        }
    }
    
    public static void printMethodBytecode(final CtMethod meth, final PrintStream out) {
        InstructionPrinter.print(meth, out);
    }
    
    public static List<Field> getNonStaticFields(final Class<?> klass) {
        final List<Field> list = new ArrayList<Field>();
        for (final Field field : klass.getFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                list.add(field);
            }
        }
        return list;
    }
    
    public static List<Field> getNotSpecial(final List<Field> fields) {
        final List<Field> ret = new ArrayList<Field>();
        try {
            final Class<?> anno = ClassLoader.getSystemClassLoader().loadClass("com.datasphere.anno.SpecialEventAttribute");
            for (final Field f : fields) {
                boolean isSpecial = false;
                for (final Annotation a : f.getAnnotations()) {
                    final Class<?> atype = a.annotationType();
                    if (atype.equals(anno)) {
                        isSpecial = true;
                        break;
                    }
                }
                if (!isSpecial) {
                    ret.add(f);
                }
            }
        }
        catch (ClassNotFoundException e) {
            assert false;
        }
        return ret;
    }
    
    public static byte[] compileJavaFile(final String fileName) throws IOException {
        final File sourceFile = new File(fileName);
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        final StandardJavaFileManager stdFileManager = compiler.getStandardFileManager(null, null, null);
        stdFileManager.setLocation(StandardLocation.CLASS_OUTPUT, Arrays.asList(sourceFile.getParentFile()));
        final JavaFileManager fileManager = new XJavaFileManager(stdFileManager);
        final String cp = System.getProperty("java.class.path");
        final JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, null, Arrays.asList("-classpath", cp), null, stdFileManager.getJavaFileObjectsFromFiles(Arrays.asList(sourceFile)));
        final boolean success = task.call();
        fileManager.close();
        if (success) {
            return Files.readAllBytes(Paths.get(fileName.replace(".java", ".class"), new String[0]));
        }
        return null;
    }
    
    static {
        javaKeywords = new String[] { "abstract", "assert", "boolean", "break", "byte", "case", "catch", "char", "class", "const", "continue", "default", "do", "double", "else", "enum", "extends", "final", "finally", "float", "for", "goto", "if", "implements", "import", "instanceof", "int", "interface", "long", "native", "new", "package", "private", "protected", "public", "return", "short", "static", "strictfp", "super", "switch", "synchronized", "this", "throw", "throws", "transient", "try", "void", "volatile", "while" };
        CompilerUtils.logger = Logger.getLogger((Class)CompilerUtils.class);
        primitiveTypeMap = Factory.makeMap();
        boxingTypeMap = Factory.makeMap();
        int index = 0;
        for (final Class<?> c : getPrimitiveTypes()) {
            CompilerUtils.primitiveTypeMap.put(c, ++index);
        }
        index = 0;
        for (final Class<?> c : getBoxingTypes()) {
            CompilerUtils.boxingTypeMap.put(c, ++index);
        }
        wideningMap = new boolean[10][10];
        CompilerUtils.wideningMap[2][2] = true;
        CompilerUtils.wideningMap[3][5] = true;
        for (int t = 3; t < 10; ++t) {
            CompilerUtils.wideningMap[t][t] = true;
            for (int t2 = (t == 3 || t == 4) ? 6 : (t + 1); t2 < 10; ++t2) {
                CompilerUtils.wideningMap[t][t2] = true;
            }
        }
        shortcuts = loadJavaLangShortcuts();
        primitives = loadJavaLangPrimitiveTypes();
    }
    
    public static class NullType implements Comparable<NullType>
    {
        @Override
        public int compareTo(final NullType o) {
            return 0;
        }
    }
    
    public static class ParamTypeUndefined implements Comparable<ParamTypeUndefined>
    {
        @Override
        public int compareTo(final ParamTypeUndefined o) {
            return 0;
        }
    }
}
