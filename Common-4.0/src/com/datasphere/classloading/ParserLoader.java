package com.datasphere.classloading;

import java.util.Map;

import com.datasphere.common.exc.AdapterException;
import com.datasphere.intf.Parser;
import com.datasphere.uuid.UUID;
/*
 * 加载解析器
 * TODO 待改进
 */
public class ParserLoader
{
    public static String MODULE_NAME;
    
    public static Parser createParser(final Map<String, Object> prop, final UUID sourceUUID) throws Exception {
        return loadParser(prop, sourceUUID);
    }
    
    public static Parser loadParser(final Map<String, Object> prop, final UUID sourceUUID) throws Exception {
        final String packageName = "com.datasphere.proc.";
        final String version = "_1_0";
        final String className = (String)prop.get(ParserLoader.MODULE_NAME);
        if (className != null) {
            try {
                String fullyQualifiedName;
                if (className.contains(".")) {
                    fullyQualifiedName = className;
                }
                else {
                    fullyQualifiedName = packageName + className + version;
                }
                Class<?> parserClass = null;
                try {
                    parserClass = Class.forName(fullyQualifiedName, false, ClassLoader.getSystemClassLoader());
                }
                catch (Exception exp) {
                    try {
                        parserClass = Class.forName(className, false, ClassLoader.getSystemClassLoader());
                    }
                    catch (Exception sExp) {
                        sExp.printStackTrace();
                        throw sExp;
                    }
                }
                final Parser parser = (Parser)parserClass.getConstructor(Map.class, UUID.class).newInstance(prop, sourceUUID);
                return parser;
            }
            catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }
        throw new AdapterException("Null parser module name passed");
    }
    
    static {
        ParserLoader.MODULE_NAME = "handler";
    }
}
