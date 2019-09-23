package com.datasphere.gen;

import org.apache.log4j.*;
import org.eclipse.persistence.internal.jpa.config.classes.Attributes;
import org.eclipse.persistence.jpa.config.Column;
import org.eclipse.persistence.jpa.config.FetchAttribute;
import org.eclipse.persistence.jpa.config.FetchGroup;

import com.datasphere.runtime.meta.*;
import org.jdom2.input.*;
import java.util.*;

import javax.persistence.AccessType;
import javax.xml.bind.*;
import org.jdom2.*;
import org.jdom2.Element;

import com.datasphere.persistence.*;
import org.jdom2.output.*;
import java.io.*;
import java.util.concurrent.*;

public class RTMappingGenerator
{
    private static Logger logger;
    static ConcurrentMap<String, String> ormMappings;
    private static String XML_NS;
    private static String VERSION;
    private static final String HD_KEY = "hdkey";
    private static final String EL_NS = "http://www.eclipse.org/eclipselink/xsds/persistence/orm";
    private static final String DEFAULT_JPA_NOSQL_FILE = "default_jpa_orm_nosql.xml";
    private static final String DEFAULT_JPA_NOSQL_ONDB_FILE = "default_jpa_orm_nosql_ondb.xml";
    private static final String DEFAULT_JPA_RDBMS_FILE = "default_jpa_orm_rdbms.xml";
    private static final String DEFAULT_JPA_RDBMS_FILE_NEW = "default_jpa_orm_rdbms_new.xml";
    public static String mappingType;
    private static final String DEFAULT_HIB_MAP_FILE = "default_hibernate_hd.xml";
    
    public static void addMappings(final String name, final String xml) {
        RTMappingGenerator.ormMappings.put(name, xml);
    }
    
    public static String getMappings(final String name) {
        return RTMappingGenerator.ormMappings.get(name);
    }
    
    public static String createOrmMappingforRTHDRDBMSNew(final String className, final String tableName, final Map<String, String> contextFields, final MetaInfo.Type eventType, String eventTableName, final Map<String, Object> wsProps) {
        final Map<String, Object> props = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        if (wsProps != null) {
            props.putAll(wsProps);
        }
        String contextXml = createEntityORMXMLBlockForRuntimeHD(true, className, tableName, contextFields, props);
        contextXml = mergeEntityAttributes("default_jpa_orm_rdbms_new.xml", contextXml);
        final String eventClassName = eventType.className;
        final String eventName = eventType.name;
        if (eventTableName == null) {
            eventTableName = tableName + "_" + eventName;
        }
        contextXml = contextXml.replace("$HD_ENTITY", tableName);
        contextXml = contextXml.replace("$HD_TABLENAME", tableName.toUpperCase());
        contextXml = contextXml.replace("$RUNTIME_HD_CLASS_NAME", className);
        contextXml = contextXml.replace("$EVENT_TARGET_CLASS", eventClassName);
        contextXml = contextXml.replace("$EVENTS_TABLENAME", eventTableName.toUpperCase());
        contextXml = contextXml.replace("$CHECKPOINT_TABLENAME", tableName.toUpperCase() + "_CHECKPOINT");
        final int colLen = userSpecifiedLength("hdkey", tableName, props);
        if (colLen != -1) {
            contextXml = contextXml.replace("$HD_KEY_LENGTH", Integer.toString(colLen));
        }
        else {
            contextXml = contextXml.replace("$HD_KEY_LENGTH", Integer.toString(255));
        }
        String eventXml = createEntityORMXMLBlockForRuntimeEvent(false, eventClassName, eventTableName, eventType.fields, props);
        if (eventXml.contains("<entity>")) {
            eventXml = eventXml.replace("<entity>", "<embeddable>");
        }
        else {
            eventXml = eventXml.replace("<entity ", "<embeddable ");
        }
        eventXml = eventXml.replace("</entity>", "</embeddable>");
        final String[] mappings = { contextXml, eventXml };
        final String mergedXml = mergeAllEntityMappings(mappings);
        return mergedXml;
    }
    
    private static String mergeEntityAttributes(final String fileName, final String xml) {
        String resultXml = "";
        try {
            final InputStream in1 = ClassLoader.getSystemResourceAsStream(fileName);
            final SAXBuilder saxBuilder = new SAXBuilder();
            final Document doc1 = saxBuilder.build(in1);
            final Element rootNode = doc1.getRootElement();
            final Namespace ns1 = rootNode.getNamespace();
            final List<Element> list = (List<Element>)rootNode.getChildren("entity", ns1);
            Element attributes1 = null;
            Element fetchGroup1 = null;
            for (int ie = 0; ie < list.size(); ++ie) {
                final String nameAttr = list.get(ie).getAttributeValue("name");
                if (nameAttr != null && nameAttr.equalsIgnoreCase("$HD_ENTITY")) {
                    attributes1 = list.get(ie).getChildren("attributes", ns1).get(0);
                    fetchGroup1 = list.get(ie).getChildren("fetch-group", ns1).get(0);
                    final InputStream in2 = new ByteArrayInputStream(xml.getBytes());
                    final Document doc2 = saxBuilder.build(in2);
                    final Element root2 = doc2.getRootElement();
                    final Namespace ns2 = root2.getNamespace();
                    final Element entity2 = root2.getChildren("entity", ns2).get(0);
                    final Element attribute2 = entity2.getChildren("attributes", ns2).get(0);
                    final Element fetchGroup2 = entity2.getChildren("fetch-group", ns2).get(0);
                    final List<Element> basic2 = (List<Element>)attribute2.getChildren();
                    final int rtfields = basic2.size();
                    if (RTMappingGenerator.logger.isDebugEnabled()) {
                        RTMappingGenerator.logger.debug((Object)("merging " + rtfields + " context fields to existing hd orm xml."));
                    }
                    for (int rr = rtfields - 1; rr >= 0; --rr) {
                        attributes1.addContent((Content)basic2.get(rr).detach());
                    }
                    attributes1.coalesceText(true);
                    final List<Element> fgas = (List<Element>)fetchGroup2.getChildren();
                    final int fgfields = fgas.size();
                    for (int rr2 = fgfields - 1; rr2 >= 0; --rr2) {
                        fetchGroup1.addContent((Content)fgas.get(rr2).detach());
                    }
                    fetchGroup1.coalesceText(true);
                }
                final Document newdoc = new Document();
                newdoc.addContent((Content)rootNode.detach());
                final XMLOutputter xmlOut = new XMLOutputter();
                resultXml = xmlOut.outputString(newdoc);
            }
        }
        catch (Exception e) {
            RTMappingGenerator.logger.error((Object)"error merging mapping xml files. ", (Throwable)e);
        }
        return resultXml;
    }
    
    public static int userSpecifiedLength(final String fieldName, final String tableName, final Map<String, Object> props) {
        for (final String key : props.keySet()) {
            if (key.equalsIgnoreCase("lengthof_" + tableName + "_" + fieldName)) {
                final Object obj = props.get(key);
                if (obj == null) {
                    return -1;
                }
                try {
                    if (obj instanceof String) {
                        return Integer.parseInt((String)props.get(key));
                    }
                    if (obj instanceof Integer) {
                        return (int)obj;
                    }
                    continue;
                }
                catch (NumberFormatException ex) {}
            }
        }
        return -1;
    }
    
    public static boolean userRequestedIndex(final String fieldName, final String tableName, final Map<String, Object> props) {
        for (final String key : props.keySet()) {
            if (key.equalsIgnoreCase("indexon_" + tableName)) {
                final String obj = (String)props.get(key);
                if (obj == null || obj.isEmpty()) {
                    return false;
                }
                final String[] indexedKeys = obj.split(",");
                if (indexedKeys == null || indexedKeys.length <= 0) {
                    continue;
                }
                for (int kk = 0; kk < indexedKeys.length; ++kk) {
                    final String s1 = indexedKeys[kk];
                    if (s1.trim().equalsIgnoreCase(fieldName)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
    
    private static String createEntityORMXMLBlockForRuntimeHD(final boolean useAccessMethods, final String className, final String tableName, final Map<String, String> fields, final Map<String, Object> props) {
        String xml = "";
        final ObjectFactory factory = new ObjectFactory();
        final EntityMappings entityMappings = factory.createEntityMappings();
        entityMappings.setVersion(RTMappingGenerator.VERSION);
        final Entity entity = factory.createEntity();
        entity.setName(tableName);
        entity.setClazz(className);
        entity.setAccess(AccessType.FIELD);
        final FetchGroup fg = factory.createFetchGroup();
        fg.setName("noEvents");
        final Attributes attrs = factory.createAttributes();
        for (final Map.Entry<String, String> field : fields.entrySet()) {
            final String fieldName = field.getKey();
            final String fieldType = field.getValue();
            if (fieldType.contains("com.datasphere.")) {
                continue;
            }
            if (RTMappingGenerator.logger.isDebugEnabled()) {
                RTMappingGenerator.logger.debug((Object)("adding field name : " + fieldName + ", type is : " + fieldType));
            }
            final int colLen = userSpecifiedLength(fieldName, tableName, props);
            final boolean indexed = userRequestedIndex(fieldName, tableName, props);
            attrs.getBasic().add(getBasic(useAccessMethods, factory, fieldName, fieldType, colLen, indexed));
            final FetchAttribute fa = factory.createFetchAttribute();
            fa.setName(fieldName);
            fg.getAttribute().add(fa);
        }
        entity.getFetchGroup().add(fg);
        entity.setAttributes(attrs);
        entityMappings.getEntity().add(entity);
        try {
            final JAXBContext jcontext = JAXBContext.newInstance(RTMappingGenerator.XML_NS);
            final Marshaller marshaller = jcontext.createMarshaller();
            marshaller.setProperty("jaxb.formatted.output", Boolean.TRUE);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            marshaller.marshal(entityMappings, baos);
            xml = baos.toString();
        }
        catch (JAXBException e) {
            RTMappingGenerator.logger.error((Object)("error creating orm xml mapping for beanDef : " + className), (Throwable)e);
        }
        if (RTMappingGenerator.logger.isInfoEnabled()) {
            RTMappingGenerator.logger.info((Object)("xml mapping creatred for " + className + " is :\n" + xml));
        }
        return xml;
    }
    
    private static String createEntityORMXMLBlockForRuntimeEvent(final boolean useAccessMethods, final String className, final String tableName, final Map<String, String> fields, final Map<String, Object> props) {
        String xml = "";
        final ObjectFactory factory = new ObjectFactory();
        final EntityMappings entityMappings = factory.createEntityMappings();
        entityMappings.setVersion(RTMappingGenerator.VERSION);
        final Entity entity = factory.createEntity();
        entity.setName(tableName);
        entity.setClazz(className);
        entity.setAccess(AccessType.FIELD);
        final Attributes attrs = factory.createAttributes();
        final Basic basicForEventUUID = factory.createBasic();
        basicForEventUUID.setName("_wa_SimpleEvent_ID");
        basicForEventUUID.setAttributeType("String");
        final Column column = factory.createColumn();
        column.setName("EVENT_UUID");
        column.setNullable(true);
        basicForEventUUID.setColumn(column);
        final AccessMethods am = new AccessMethods();
        am.setGetMethod("getIDString");
        am.setSetMethod("setIDString");
        basicForEventUUID.setAccessMethods(am);
        attrs.getBasic().add(basicForEventUUID);
        for (final Map.Entry<String, String> field : fields.entrySet()) {
            final String fieldName = field.getKey();
            final String fieldType = field.getValue();
            if (fieldType.contains("com.datasphere.")) {
                continue;
            }
            if (RTMappingGenerator.logger.isDebugEnabled()) {
                RTMappingGenerator.logger.debug((Object)("adding field name : " + fieldName + ", type is : " + fieldType));
            }
            final int colLen = userSpecifiedLength(fieldName, tableName, props);
            final boolean indexed = userRequestedIndex(fieldName, tableName, props);
            attrs.getBasic().add(getBasic(useAccessMethods, factory, fieldName, fieldType, colLen, indexed));
        }
        entity.setAttributes(attrs);
        entityMappings.getEntity().add(entity);
        try {
            final JAXBContext jcontext = JAXBContext.newInstance(RTMappingGenerator.XML_NS);
            final Marshaller marshaller = jcontext.createMarshaller();
            marshaller.setProperty("jaxb.formatted.output", Boolean.TRUE);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            marshaller.marshal(entityMappings, baos);
            xml = baos.toString();
        }
        catch (JAXBException e) {
            RTMappingGenerator.logger.error((Object)("error creating orm xml mapping for beanDef : " + className), (Throwable)e);
        }
        if (RTMappingGenerator.logger.isInfoEnabled()) {
            RTMappingGenerator.logger.info((Object)("xml mapping creatred for " + className + " is :\n" + xml));
        }
        return xml;
    }
    
    public static String createOrmMappingforRTHDRDBMS(final String className, final String tableName, final MetaInfo.Type typeInfo, final String eventTableName, final Map<String, Object> wsProps) {
        final Map<String, Object> props = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        if (wsProps != null) {
            props.putAll(wsProps);
        }
        String xml = "<entity-mappings/>";
        if (typeInfo == null) {
            RTMappingGenerator.logger.warn((Object)"beandef is empty, no mappings are created");
            return "<entity-mappings/>";
        }
        final ObjectFactory factory = new ObjectFactory();
        final EntityMappings entityMappings = factory.createEntityMappings();
        entityMappings.setVersion(RTMappingGenerator.VERSION);
        final Entity entity = factory.createEntity();
        entity.setName(tableName);
        entity.setClazz(className);
        entity.setAccess(AccessType.FIELD);
        final Attributes attrs = factory.createAttributes();
        for (final Map.Entry<String, String> field : typeInfo.fields.entrySet()) {
            final String fieldName = field.getKey();
            final String fieldType = field.getValue();
            if (fieldType.contains("com.datasphere.")) {
                continue;
            }
            if (RTMappingGenerator.logger.isDebugEnabled()) {
                RTMappingGenerator.logger.debug((Object)("adding field name : " + fieldName + ", type is : " + fieldType));
            }
            attrs.getBasic().add(getBasic(factory, fieldName, fieldType));
        }
        entity.setAttributes(attrs);
        entityMappings.getEntity().add(entity);
        try {
            final JAXBContext jcontext = JAXBContext.newInstance(RTMappingGenerator.XML_NS);
            final Marshaller marshaller = jcontext.createMarshaller();
            marshaller.setProperty("jaxb.formatted.output", Boolean.TRUE);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            marshaller.marshal(entityMappings, baos);
            xml = baos.toString();
        }
        catch (JAXBException e) {
            RTMappingGenerator.logger.error((Object)("error creating orm xml mapping for beanDef : " + typeInfo.name), (Throwable)e);
        }
        if (RTMappingGenerator.logger.isInfoEnabled()) {
            RTMappingGenerator.logger.info((Object)("xml mapping creatred for " + typeInfo.className + " is :\n" + xml));
        }
        String mergedXml = "<entity-mappings/>";
        try {
            final InputStream in = ClassLoader.getSystemResourceAsStream("default_jpa_orm_rdbms.xml");
            final SAXBuilder builder = new SAXBuilder();
            final Document document = builder.build(in);
            final Element rootNode = document.getRootElement();
            final InputStream bis = new ByteArrayInputStream(xml.getBytes());
            final Document runtimeDoc = builder.build(bis);
            final Element rtRoot = runtimeDoc.getRootElement();
            final List<Element> rtList = (List<Element>)rtRoot.getChildren();
            final int rtListSize = rtList.size();
            for (int rr = rtListSize - 1; rr >= 0; --rr) {
                final Element entityEle = rtList.get(rr);
                if (entityEle.getName().equals("entity")) {
                    rootNode.addContent((Content)entityEle.detach());
                }
            }
            final Document merged = new Document();
            merged.addContent((Content)rootNode.detach());
            final XMLOutputter xmlOut = new XMLOutputter();
            mergedXml = xmlOut.outputString(merged);
            if (eventTableName == null) {
                mergedXml = mergedXml.replace("$EVENTS_TABLENAME", tableName.toUpperCase() + "_EVENTS");
            }
            else {
                mergedXml = mergedXml.replace("$EVENTS_TABLENAME", eventTableName.toUpperCase());
            }
            mergedXml = mergedXml.replace("$CHECKPOINT_TABLENAME", tableName.toUpperCase() + "_CHECKPOINT");
            final int colLen = userSpecifiedLength("hdkey", tableName, props);
            if (colLen != -1) {
                mergedXml = mergedXml.replace("$HD_KEY_LENGTH", Integer.toString(colLen));
            }
            else {
                mergedXml = mergedXml.replace("$HD_KEY_LENGTH", Integer.toString(255));
            }
        }
        catch (JDOMException | IOException ex2) {
            final Exception ex;
            final Exception e2 = ex;
            RTMappingGenerator.logger.error((Object)e2);
        }
        if (RTMappingGenerator.logger.isInfoEnabled()) {
            RTMappingGenerator.logger.info((Object)("merged xml mappings are : \n" + mergedXml));
        }
        return mergedXml;
    }
    
    public static String createOrmMappingforRTHDRDBMS(final String wsName, final MetaInfo.Type typeInfo) {
        if (typeInfo == null) {
            RTMappingGenerator.logger.warn((Object)"beandef is empty, no mappings are created");
            return "<entity-mappings/>";
        }
        final ObjectFactory factory = new ObjectFactory();
        final EntityMappings entityMappings = factory.createEntityMappings();
        entityMappings.setVersion(RTMappingGenerator.VERSION);
        final Entity entity = factory.createEntity();
        entity.setName("HD_" + wsName.toUpperCase());
        entity.setClazz("wa.HD_" + wsName);
        entity.setAccess(AccessType.FIELD);
        final Attributes attrs = factory.createAttributes();
        for (final Map.Entry<String, String> field : typeInfo.fields.entrySet()) {
            final String fieldName = field.getKey();
            final String fieldType = field.getValue();
            if (fieldType.contains("com.datasphere.")) {
                continue;
            }
            if (RTMappingGenerator.logger.isDebugEnabled()) {
                RTMappingGenerator.logger.debug((Object)("adding field name : " + fieldName + ", type is : " + fieldType));
            }
            attrs.getBasic().add(getBasic(factory, fieldName, fieldType));
        }
        entity.setAttributes(attrs);
        entityMappings.getEntity().add(entity);
        String xml = "<entity-mappings/>";
        try {
            final JAXBContext jcontext = JAXBContext.newInstance(RTMappingGenerator.XML_NS);
            final Marshaller marshaller = jcontext.createMarshaller();
            marshaller.setProperty("jaxb.formatted.output", Boolean.TRUE);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            marshaller.marshal(entityMappings, baos);
            xml = baos.toString();
        }
        catch (JAXBException e) {
            RTMappingGenerator.logger.error((Object)("error creating orm xml mapping for beanDef : " + typeInfo.name), (Throwable)e);
        }
        if (RTMappingGenerator.logger.isInfoEnabled()) {
            RTMappingGenerator.logger.info((Object)("xml mapping creatred for " + typeInfo.className + " is :\n" + xml));
        }
        String mergedXml = "<entity-mappings/>";
        try {
            final InputStream in = ClassLoader.getSystemResourceAsStream("default-orm-rdbms.xml");
            final SAXBuilder builder = new SAXBuilder();
            final Document document = builder.build(in);
            final Element rootNode = document.getRootElement();
            final InputStream bis = new ByteArrayInputStream(xml.getBytes());
            final Document runtimeDoc = builder.build(bis);
            final Element rtRoot = runtimeDoc.getRootElement();
            final List<Element> rtList = (List<Element>)rtRoot.getChildren();
            final int rtListSize = rtList.size();
            for (int rr = rtListSize - 1; rr >= 0; --rr) {
                final Element entityEle = rtList.get(rr);
                if (entityEle.getName().equals("entity")) {
                    rootNode.addContent((Content)entityEle.detach());
                }
            }
            final Document merged = new Document();
            merged.addContent((Content)rootNode.detach());
            final XMLOutputter xmlOut = new XMLOutputter();
            mergedXml = xmlOut.outputString(merged);
        }
        catch (JDOMException | IOException ex2) {
            final Exception ex;
            final Exception e2 = ex;
            RTMappingGenerator.logger.error((Object)e2);
        }
        if (RTMappingGenerator.logger.isInfoEnabled()) {
            RTMappingGenerator.logger.info((Object)("merged xml mappings are : \n" + mergedXml));
        }
        return mergedXml;
    }
    
    public static String createOrmMappingforRTHDNoSQL(final String className, final String tableName, final MetaInfo.Type typeInfo, final HStore.TARGETDATABASE tdb) {
        final String xml = "<entity-mappings/>";
        if (typeInfo == null) {
            RTMappingGenerator.logger.warn((Object)"beandef is empty, no mappings are created");
            return "<entity-mappings/>";
        }
        String fileName = "";
        if (tdb.equals(HStore.TARGETDATABASE.ONDB)) {
            fileName = "default_jpa_orm_nosql_ondb.xml";
        }
        else {
            fileName = "default_jpa_orm_nosql.xml";
        }
        String jpaNoSQLMappings = "";
        try {
            final InputStream in = ClassLoader.getSystemResourceAsStream(fileName);
            final SAXBuilder builder = new SAXBuilder();
            final Document document = builder.build(in);
            final Namespace ns = Namespace.getNamespace("http://www.eclipse.org/eclipselink/xsds/persistence/orm");
            final Element rootNode = document.getRootElement();
            final List<Element> entityElms = (List<Element>)rootNode.getChildren();
            Element entityElm = null;
            if (entityElms != null && entityElms.size() > 0) {
                for (final Element e : entityElms) {
                    if (e.getName().equalsIgnoreCase("entity")) {
                        entityElm = e;
                    }
                }
            }
            entityElm.setAttribute("name", tableName);
            entityElm.setAttribute("class", className);
            final List<Element> attributeElms = (List<Element>)entityElm.getChildren();
            Element attributeElm = null;
            if (attributeElms != null) {
                for (final Element e2 : attributeElms) {
                    if (e2.getName().equalsIgnoreCase("attributes")) {
                        attributeElm = e2;
                    }
                }
            }
            for (final Map.Entry<String, String> field : typeInfo.fields.entrySet()) {
                final String fieldName = field.getKey();
                final String fieldType = field.getValue();
                if (RTMappingGenerator.logger.isTraceEnabled()) {
                    RTMappingGenerator.logger.trace((Object)("adding field name : " + fieldName + ", type is : " + fieldType));
                }
                if (fieldType.contains("com.datasphere.")) {
                    continue;
                }
                final Element basicElm = new Element("basic", ns);
                if (fieldType.equals("org.joda.time.DateTime")) {
                    basicElm.setAttribute("name", fieldName + "AsLong");
                }
                else {
                    basicElm.setAttribute("name", fieldName);
                }
                basicElm.setAttribute("attribute-type", fieldType);
                final Element column = new Element("column", ns);
                column.setAttribute("name", "RT_" + fieldName.toUpperCase());
                column.setAttribute("nullable", "true");
                basicElm.addContent((Content)column);
                final Element acessMethodElm = new Element("access-methods", ns);
                acessMethodElm.setAttribute("get-method", "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1));
                acessMethodElm.setAttribute("set-method", "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1));
                basicElm.addContent((Content)acessMethodElm);
                attributeElm.addContent((Content)basicElm);
            }
            final XMLOutputter out = new XMLOutputter(Format.getPrettyFormat());
            jpaNoSQLMappings = out.outputString(document);
            jpaNoSQLMappings = jpaNoSQLMappings.replace("$CHECKPOINT_TABLENAME", tableName.toUpperCase() + "_CHECKPOINT");
            return jpaNoSQLMappings;
        }
        catch (Exception ex) {
            RTMappingGenerator.logger.error((Object)("error creating JPA-ORM xml mapping for NoSQL db:" + ex));
            return "<entity-mappings/>";
        }
    }
    
    public static String createOrmMappingforRTHDNoSQL(final String wsName, final MetaInfo.Type typeInfo) {
        String xml = "<entity-mappings/>";
        if (typeInfo == null) {
            RTMappingGenerator.logger.warn((Object)"beandef is empty, no mappings are created");
            return "<entity-mappings/>";
        }
        final ObjectFactory factory = new ObjectFactory();
        final EntityMappings entityMappings = factory.createEntityMappings();
        entityMappings.setVersion(RTMappingGenerator.VERSION);
        final Entity entity = factory.createEntity();
        entity.setName("HD_" + wsName.toUpperCase());
        entity.setClazz("wa.HD_" + wsName);
        entity.setAccess(AccessType.FIELD);
        final NoSql nosql = factory.createNoSql();
        nosql.setDataFormat(DataFormatType.MAPPED);
        entity.setNoSql(nosql);
        final Attributes attrs = factory.createAttributes();
        for (final Map.Entry<String, String> field : typeInfo.fields.entrySet()) {
            final String fieldName = field.getKey();
            final String fieldType = field.getValue();
            if (fieldType.contains("com.datasphere.")) {
                continue;
            }
            if (RTMappingGenerator.logger.isDebugEnabled()) {
                RTMappingGenerator.logger.debug((Object)("adding field name : " + fieldName + ", type is : " + fieldType));
            }
            attrs.getBasic().add(getBasic(factory, fieldName, fieldType));
        }
        entity.setAttributes(attrs);
        entityMappings.getEntity().add(entity);
        try {
            final JAXBContext jcontext = JAXBContext.newInstance(RTMappingGenerator.XML_NS);
            final Marshaller marshaller = jcontext.createMarshaller();
            marshaller.setProperty("jaxb.formatted.output", Boolean.TRUE);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            marshaller.marshal(entityMappings, baos);
            xml = baos.toString();
        }
        catch (JAXBException e) {
            RTMappingGenerator.logger.error((Object)("error creating orm xml mapping for beanDef : " + typeInfo.name), (Throwable)e);
        }
        if (RTMappingGenerator.logger.isInfoEnabled()) {
            RTMappingGenerator.logger.info((Object)("xml mapping creatred for " + typeInfo.className + " is :\n" + xml));
        }
        String mergedXml = "<entity-mappings/>";
        try {
            final InputStream in = ClassLoader.getSystemResourceAsStream("default-orm-mongodb.xml");
            final SAXBuilder builder = new SAXBuilder();
            final Document document = builder.build(in);
            final Element rootNode = document.getRootElement();
            final InputStream bis = new ByteArrayInputStream(xml.getBytes());
            final Document runtimeDoc = builder.build(bis);
            final Element rtRoot = runtimeDoc.getRootElement();
            final List<Element> rtList = (List<Element>)rtRoot.getChildren();
            final int rtListSize = rtList.size();
            for (int rr = rtListSize - 1; rr >= 0; --rr) {
                final Element entityEle = rtList.get(rr);
                if (entityEle.getName().equals("entity")) {
                    rootNode.addContent((Content)entityEle.detach());
                }
            }
            final Document merged = new Document();
            merged.addContent((Content)rootNode.detach());
            final XMLOutputter xmlOut = new XMLOutputter();
            mergedXml = xmlOut.outputString(merged);
        }
        catch (JDOMException | IOException ex2) {
            final Exception ex;
            final Exception e2 = ex;
            RTMappingGenerator.logger.error((Object)e2);
        }
        if (RTMappingGenerator.logger.isInfoEnabled()) {
            RTMappingGenerator.logger.info((Object)("merged xml mappings are : \n" + mergedXml));
        }
        return mergedXml;
    }
    
    public static String createOrmMappingforClassRDBMS(final String tableName, final MetaInfo.Type typeInfo) {
        if (typeInfo == null) {
            RTMappingGenerator.logger.warn((Object)"beandef is empty, no mappings are created");
            return "<entity-mappings/>";
        }
        final ObjectFactory factory = new ObjectFactory();
        final EntityMappings entityMappings = factory.createEntityMappings();
        entityMappings.setVersion(RTMappingGenerator.VERSION);
        final List<Converter> convList = entityMappings.getConverter();
        final Converter converter1 = new Converter();
        converter1.setClazz("com.datasphere.runtime.converters.JodaDateConverter2");
        converter1.setName("JodaDateConverter2");
        convList.add(converter1);
        final Entity entity = factory.createEntity();
        entity.setName(tableName);
        entity.setClazz(typeInfo.className);
        entity.setAccess(AccessType.FIELD);
        final Table table = factory.createTable();
        table.setName(tableName);
        entity.setTable(table);
        final Attributes attrs = factory.createAttributes();
        for (final String fieldName : typeInfo.keyFields) {
            final String keyField = fieldName;
            final String fieldType = typeInfo.fields.get(keyField);
            attrs.getId().add(getId(factory, fieldName, fieldType));
        }
        for (final Map.Entry<String, String> field : typeInfo.fields.entrySet()) {
            final String fieldName = field.getKey();
            if (typeInfo.keyFields.contains(fieldName)) {
                continue;
            }
            final String fieldType = field.getValue();
            if (fieldType.contains("com.datasphere.")) {
                continue;
            }
            if (RTMappingGenerator.logger.isDebugEnabled()) {
                RTMappingGenerator.logger.debug((Object)("adding field name : " + fieldName + ", type is : " + fieldType));
            }
            attrs.getBasic().add(getBasicForJPAWriter(factory, fieldName, fieldType));
        }
        entity.setAttributes(attrs);
        entityMappings.getEntity().add(entity);
        String xml = "<entity-mappings/>";
        try {
            final JAXBContext jcontext = JAXBContext.newInstance(RTMappingGenerator.XML_NS);
            final Marshaller marshaller = jcontext.createMarshaller();
            marshaller.setProperty("jaxb.formatted.output", Boolean.TRUE);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            marshaller.marshal(entityMappings, baos);
            xml = baos.toString();
        }
        catch (JAXBException e) {
            RTMappingGenerator.logger.error((Object)("error creating orm xml mapping for beanDef : " + typeInfo.name), (Throwable)e);
        }
        if (RTMappingGenerator.logger.isInfoEnabled()) {
            RTMappingGenerator.logger.info((Object)("xml mapping creatred for " + typeInfo.className + " is :\n" + xml));
        }
        return xml;
    }
    
    public static String mergeAllEntityMappings(final String[] mappings) {
        String mergedXml = "<entity-mappings/>";
        if (mappings == null || mappings.length == 0) {
            return mergedXml;
        }
        if (mappings.length == 1) {
            return mappings[0];
        }
        final SAXBuilder builder = new SAXBuilder();
        try {
            final Document document = builder.build((Reader)new StringReader(mappings[0]));
            final Element rootNode = document.getRootElement();
            for (int gg = 1; gg < mappings.length; ++gg) {
                final Document document2 = builder.build((Reader)new StringReader(mappings[gg]));
                final Element rootNode2 = document2.getRootElement();
                final List<Element> list2 = (List<Element>)rootNode2.getChildren();
                for (int rr = 0; rr < list2.size(); ++rr) {
                    final Element entityEle = list2.get(rr);
                    if (entityEle.getName().equals("entity")) {
                        rootNode.addContent((Content)entityEle.detach());
                    }
                    if (entityEle.getName().equals("embeddable")) {
                        rootNode.addContent((Content)entityEle.detach());
                    }
                }
            }
            final Document merged = new Document();
            merged.addContent((Content)rootNode.detach());
            final XMLOutputter xmlOut = new XMLOutputter();
            mergedXml = xmlOut.outputString(merged);
        }
        catch (JDOMException | IOException ex2) {
            final Exception ex;
            final Exception e = ex;
            RTMappingGenerator.logger.error((Object)e);
        }
        if (RTMappingGenerator.logger.isInfoEnabled()) {
            RTMappingGenerator.logger.info((Object)("merged xml mappings are : \n" + mergedXml));
        }
        return mergedXml;
    }
    
    private static Id getId(final ObjectFactory factory, final String fname, final String ftype) {
        final Id id = factory.createId();
        id.setName(fname);
        if (ftype.equals("java.lang.String")) {
            id.setAttributeType("String");
        }
        else if (ftype.equals("java.util.Date")) {
            id.setTemporal(TemporalType.TIMESTAMP);
        }
        else if (ftype.equals("org.joda.time.DateTime")) {
            id.setAttributeType("long");
        }
        else {
            id.setAttributeType(ftype);
        }
        final Column column = factory.createColumn();
        column.setName(fname);
        column.setNullable(false);
        id.setColumn(column);
        String getMethod = "";
        String setMethod = "";
        if (ftype.equals("org.joda.time.DateTime")) {
            getMethod = "get" + fname.substring(0, 1).toUpperCase() + fname.substring(1) + "AsLong";
            setMethod = "set" + fname.substring(0, 1).toUpperCase() + fname.substring(1) + "AsLong";
        }
        else {
            getMethod = "get" + fname.substring(0, 1).toUpperCase() + fname.substring(1);
            setMethod = "set" + fname.substring(0, 1).toUpperCase() + fname.substring(1);
        }
        final AccessMethods am = new AccessMethods();
        am.setGetMethod(getMethod);
        am.setSetMethod(setMethod);
        return id;
    }
    
    private static Basic getBasicForJPAWriter(final ObjectFactory factory, final String fname, final String ftype) {
        final Basic basic = factory.createBasic();
        basic.setName(fname);
        if (ftype.equals("java.lang.String")) {
            basic.setAttributeType("String");
        }
        else if (ftype.equals("java.util.Date")) {
            basic.setTemporal(TemporalType.TIMESTAMP);
        }
        else if (ftype.equals("org.joda.time.DateTime")) {
            basic.setConvert("JodaDateConverter2");
        }
        else {
            basic.setAttributeType(ftype);
        }
        final Column column = factory.createColumn();
        column.setName(fname);
        column.setNullable(true);
        basic.setColumn(column);
        String getMethod = "";
        String setMethod = "";
        if (ftype.equals("org.joda.time.DateTime")) {
            return basic;
        }
        getMethod = "get" + fname.substring(0, 1).toUpperCase() + fname.substring(1);
        setMethod = "set" + fname.substring(0, 1).toUpperCase() + fname.substring(1);
        final AccessMethods am = new AccessMethods();
        am.setGetMethod(getMethod);
        am.setSetMethod(setMethod);
        basic.setAccessMethods(am);
        return basic;
    }
    
    private static Basic getBasic(final ObjectFactory factory, final String fname, final String ftype) {
        final Basic basic = factory.createBasic();
        basic.setName(fname);
        if (ftype.equals("java.lang.String")) {
            basic.setAttributeType("String");
        }
        else if (ftype.equals("java.util.Date")) {
            basic.setTemporal(TemporalType.TIMESTAMP);
        }
        else if (ftype.equals("org.joda.time.DateTime")) {
            basic.setAttributeType("long");
        }
        else {
            basic.setAttributeType(ftype);
        }
        final Column column = factory.createColumn();
        column.setName(fname);
        column.setNullable(true);
        basic.setColumn(column);
        String getMethod = "";
        String setMethod = "";
        if (ftype.equals("org.joda.time.DateTime")) {
            getMethod = "get" + fname.substring(0, 1).toUpperCase() + fname.substring(1) + "AsLong";
            setMethod = "set" + fname.substring(0, 1).toUpperCase() + fname.substring(1) + "AsLong";
        }
        else {
            getMethod = "get" + fname.substring(0, 1).toUpperCase() + fname.substring(1);
            setMethod = "set" + fname.substring(0, 1).toUpperCase() + fname.substring(1);
        }
        final AccessMethods am = new AccessMethods();
        am.setGetMethod(getMethod);
        am.setSetMethod(setMethod);
        basic.setAccessMethods(am);
        return basic;
    }
    
    private static Basic getBasic(boolean useAccessMethods, final ObjectFactory factory, final String fname, final String ftype, final int colLen, final boolean indexed) {
        final Basic basic = factory.createBasic();
        basic.setName(fname);
        if (ftype.equals("java.lang.String")) {
            basic.setAttributeType("String");
        }
        else if (ftype.equals("java.util.Date")) {
            basic.setTemporal(TemporalType.TIMESTAMP);
        }
        else if (ftype.equals("org.joda.time.DateTime")) {
            basic.setAttributeType("long");
        }
        else if (ftype.equals("com.fasterxml.jackson.databind.JsonNode")) {
            basic.setAttributeType("String");
        }
        else {
            basic.setAttributeType(ftype);
        }
        final Column column = factory.createColumn();
        if (colLen != -1) {
            column.setLength(colLen);
        }
        column.setName(fname);
        column.setNullable(true);
        basic.setColumn(column);
        if (ftype.equals("org.joda.time.DateTime")) {
            basic.setConvert("JodaDateConverter");
        }
        if (ftype.equals("com.fasterxml.jackson.databind.JsonNode")) {
            basic.setConvert("JsonNodeConverter");
            useAccessMethods = false;
        }
        if (useAccessMethods) {
            String getMethod = "";
            String setMethod = "";
            getMethod = "get" + fname.substring(0, 1).toUpperCase() + fname.substring(1);
            setMethod = "set" + fname.substring(0, 1).toUpperCase() + fname.substring(1);
            final AccessMethods am = new AccessMethods();
            am.setGetMethod(getMethod);
            am.setSetMethod(setMethod);
            basic.setAccessMethods(am);
        }
        if (indexed) {
            final Index index = factory.createIndex();
            basic.setIndex(index);
        }
        return basic;
    }
    
    public static String createHibernateMappings(final String className, final MetaInfo.Type typeInfo) {
        String hibernateMappings = new String();
        try {
            final InputStream in = ClassLoader.getSystemResourceAsStream("default_hibernate_hd.xml");
            final SAXBuilder builder = new SAXBuilder();
            final Document document = builder.build(in);
            final Element rootNode = document.getRootElement();
            final List<Element> classElms = (List<Element>)rootNode.getChildren("class");
            final Element clElm = classElms.get(0);
            clElm.setAttribute("name", className);
            clElm.setAttribute("table", className.replace("wa.", "").toUpperCase());
            for (final Map.Entry<String, String> field : typeInfo.fields.entrySet()) {
                final String fieldName = field.getKey();
                final String fieldType = field.getValue();
                if (fieldType.contains("com.datasphere.")) {
                    continue;
                }
                if (RTMappingGenerator.logger.isTraceEnabled()) {
                    RTMappingGenerator.logger.trace((Object)("adding field name : " + fieldName + ", type is : " + fieldType));
                }
                final Element property = new Element("property");
                if (fieldType.equals("org.joda.time.DateTime")) {
                    property.setAttribute("name", fieldName + "AsLong");
                }
                else {
                    property.setAttribute("name", fieldName);
                }
                property.setAttribute("type", getHibernateType(fieldType));
                final Element column = new Element("column");
                column.setAttribute("name", "RT_" + fieldName.toUpperCase());
                column.setAttribute("not-null", "false");
                if (getHibernateType(fieldType).equalsIgnoreCase("string")) {
                    column.setAttribute("length", "50");
                }
                property.setContent((Content)column);
                clElm.addContent((Content)property);
            }
            final XMLOutputter out = new XMLOutputter(Format.getPrettyFormat());
            hibernateMappings = out.outputString(document);
        }
        catch (JDOMException | IOException ex2) {
            final Exception ex;
            final Exception e = ex;
            RTMappingGenerator.logger.error((Object)("error creating hibernate mapping for " + className + "\n" + e));
        }
        if (RTMappingGenerator.logger.isTraceEnabled()) {
            RTMappingGenerator.logger.trace((Object)("hibernate xml mapping :\n" + hibernateMappings));
        }
        return hibernateMappings;
    }
    
    private static String getHibernateType(final String type) {
        switch (type) {
            case "java.lang.String": {
                return "string";
            }
            case "org.joda.time.DateTime": {
                return "long";
            }
            default: {
                return type;
            }
        }
    }
    
    static {
        RTMappingGenerator.logger = Logger.getLogger((Class)RTMappingGenerator.class);
        RTMappingGenerator.ormMappings = new ConcurrentHashMap<String, String>();
        RTMappingGenerator.XML_NS = "org.eclipse.eclipselink.xsds.persistence.orm";
        RTMappingGenerator.VERSION = "2.4";
        RTMappingGenerator.mappingType = "mongo";
    }
}
