package com.datasphere.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.datasphere.exception.CompilationException;
import com.datasphere.intf.SourceMetadataProvider;
import com.datasphere.kafkamessaging.StreamPersistencePolicy;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.proc.BaseProcess;
import com.datasphere.runtime.compiler.AST;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.TypeDefOrName;
import com.datasphere.runtime.compiler.TypeField;
import com.datasphere.runtime.compiler.exprs.ComparePredicate;
import com.datasphere.runtime.compiler.exprs.Constant;
import com.datasphere.runtime.compiler.exprs.ExprCmd;
import com.datasphere.runtime.compiler.exprs.FuncCall;
import com.datasphere.runtime.compiler.exprs.MethodCall;
import com.datasphere.runtime.compiler.exprs.ObjectRef;
import com.datasphere.runtime.compiler.exprs.Predicate;
import com.datasphere.runtime.compiler.exprs.ValueExpr;
import com.datasphere.runtime.compiler.select.DataSource;
import com.datasphere.runtime.compiler.select.DataSourceStream;
import com.datasphere.runtime.compiler.stmts.CreateCqStmt;
import com.datasphere.runtime.compiler.stmts.CreatePropertySetStmt;
import com.datasphere.runtime.compiler.stmts.CreateStreamStmt;
import com.datasphere.runtime.compiler.stmts.CreateTypeStmt;
import com.datasphere.runtime.compiler.stmts.MappedStream;
import com.datasphere.runtime.compiler.stmts.Select;
import com.datasphere.runtime.compiler.stmts.SelectTarget;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.UUID;

public class TypeGenerator
{
    private static Logger logger;
    public static final String TYPE_SUFFIX = "Type";
    public static final String CQ_SUFFIX = "Cq";
    private Map<String, TypeDefOrName> metadataMap;
    private final String metadataKey;
    
    public TypeGenerator(final Compiler c, final String sourceName) {
        this.metadataMap = new TreeMap<String, TypeDefOrName>(String.CASE_INSENSITIVE_ORDER);
        final Context ctx = c.getContext();
        try {
            final MetaInfo.Source sourceInfo = ctx.getSourceInCurSchema(sourceName);
            final Class<?> adapterFactory = ClassLoader.getSystemClassLoader().loadClass(sourceInfo.adapterClassName);
            final BaseProcess proc = (BaseProcess)adapterFactory.newInstance();
            if (!(proc instanceof SourceMetadataProvider)) {
                throw new RuntimeException("Adapter " + proc.toString() + " does not support providing Metadata.");
            }
            final String defaultMode = (String)sourceInfo.properties.get("SessionType");
            sourceInfo.properties.put("SessionType", "METADATA");
            proc.init(sourceInfo.properties, sourceInfo.parserProperties, sourceInfo.uuid, BaseServer.getServerName(), null, false, null);
            this.metadataKey = ((SourceMetadataProvider)proc).getMetadataKey();
            this.metadataMap = ((SourceMetadataProvider)proc).getMetadata();
            sourceInfo.properties.put("SessionType", defaultMode);
            proc.close();
        }
        catch (Exception e) {
            throw new CompilationException(e);
        }
    }
    
    public List<UUID> generateStream(final Compiler c, final List<MappedStream> mapped_streams, final String sourceStreamName, final boolean isStreamEndpoint) throws MetaDataRepositoryException {
        final List<UUID> metaObjects = new ArrayList<UUID>();
        for (final MappedStream mappedStream : mapped_streams) {
            final String typeName = mappedStream.streamName + "Type";
            final String targetStreamName = mappedStream.streamName;
            final String cqName = mappedStream.streamName + "Cq";
            String sourceTable = mappedStream.mappingProperties.get("table").toString();
            final TypeDefOrName tableDef = this.metadataMap.get(sourceTable.replace(".", "_"));
            if (tableDef == null) {
                throw new CompilationException("No metadata found for " + sourceTable);
            }
            final List<TypeField> fields = tableDef.typeDef;
            final CreateTypeStmt ctStmt = new CreateTypeStmt(typeName, Boolean.valueOf(true), tableDef);
            final UUID typeUUID = c.compileCreateTypeStmt(ctStmt);
            final StreamPersistencePolicy spp = new StreamPersistencePolicy(null);
            final CreateStreamStmt csStmt = new CreateStreamStmt(targetStreamName, true, null, typeName, fields, null, spp);
            final MetaInfo.Stream streamMetaObject = (MetaInfo.Stream)c.compileCreateStreamStmt(csStmt);
            if (isStreamEndpoint) {
                streamMetaObject.getMetaInfoStatus().setGenerated(true);
                MetadataRepository.getINSTANCE().updateMetaObject(streamMetaObject, HSecurityManager.TOKEN);
            }
            final DataSource sourceStream = new DataSourceStream(sourceStreamName, null, null);
            final List<DataSource> source_list = new ArrayList<DataSource>();
            source_list.add(sourceStream);
            final List<SelectTarget> target_list = convertFieldsToTarget(fields);
            sourceTable = sourceTable.replaceAll("\\\\", "\\\\\\\\");
            Predicate where = null;
            if (this.metadataKey != null) {
                final List<ValueExpr> whereArgs = new ArrayList<ValueExpr>();
                final List<ValueExpr> toStringArgs = new ArrayList<ValueExpr>();
                final List<ValueExpr> metaArgs = new ArrayList<ValueExpr>();
                metaArgs.add(new ObjectRef(sourceStreamName));
                metaArgs.add(new Constant(ExprCmd.STRING, this.metadataKey, String.class));
                toStringArgs.add(new FuncCall("META", metaArgs, 0));
                whereArgs.add(new MethodCall("toString", toStringArgs, 0));
                whereArgs.add(new Constant(ExprCmd.STRING, sourceTable, String.class));
                where = new ComparePredicate(ExprCmd.EQ, whereArgs);
            }
            final Select sel = new Select(false, 1, target_list, source_list, where, null, null, null, null, false, null);
            final List<String> field_list = convertFieldsToName(fields);
            final CreateCqStmt ccqStmt = new CreateCqStmt(cqName, true, targetStreamName, field_list, sel, null);
            final MetaInfo.CQ cqMetaObject = (MetaInfo.CQ)c.compileCreateCqStmt(ccqStmt);
            metaObjects.add(cqMetaObject.getUuid());
            metaObjects.add(streamMetaObject.getUuid());
            metaObjects.add(typeUUID);
        }
        return metaObjects;
    }
    
    public static UUID generatePropertySet(final Compiler c, final String propertySetName, final List<Property> properties) throws Exception {
        final CreatePropertySetStmt cpsStmt = new CreatePropertySetStmt(propertySetName, Boolean.valueOf(true), properties);
        return c.compileCreatePropertySetStmt(cpsStmt);
    }
    
    public static List<String> convertFieldsToName(final List<TypeField> field_list) {
        final List<String> name_list = new ArrayList<String>();
        for (final TypeField field : field_list) {
            name_list.add(field.fieldName);
        }
        return name_list;
    }
    
    public static List<SelectTarget> convertFieldsToTarget(final List<TypeField> field_list) {
        final List<SelectTarget> target_list = new ArrayList<SelectTarget>();
        int i = 0;
        for (final TypeField field : field_list) {
            final List<ValueExpr> exprArgs = new ArrayList<ValueExpr>();
            exprArgs.add(AST.NewIntegerConstant(i));
            final ValueExpr indexExpr = AST.NewIndexExpr(AST.NewIdentifierRef("data"), exprArgs);
            final SelectTarget target = new SelectTarget(indexExpr, "data[" + i + "]");
            target_list.add(target);
            ++i;
        }
        return target_list;
    }
    
    public static String getValidJavaIdentifierName(final String name) {
        String modified = "";
        final Character fc = name.charAt(0);
        if (Character.isJavaIdentifierStart(fc)) {
            modified += fc;
        }
        else {
            modified = "_" + getNameOfCharacter(fc) + "_";
        }
        for (int i = 1; i < name.length(); ++i) {
            final Character c = name.charAt(i);
            if (Character.isJavaIdentifierPart(c)) {
                modified += c;
            }
            else {
                modified = modified + "_" + getNameOfCharacter(c) + "_";
            }
        }
        if (TypeGenerator.logger.isInfoEnabled()) {
            TypeGenerator.logger.info((Object)("original " + name + " Modified " + modified));
        }
        return modified;
    }
    
    public static String getNameOfCharacter(final Character c) {
        switch ((char)c) {
            case '!': {
                return "EXCLAMATION_MARK";
            }
            case '@': {
                return "COMMERCIAL_AT";
            }
            case '#': {
                return "HASH";
            }
            case '%': {
                return "PERCENT_SIGN";
            }
            case '^': {
                return "CIRCUMFLEX_ACCENT";
            }
            case '&': {
                return "AMPERSAND";
            }
            case '*': {
                return "ASTERISK";
            }
            case '(': {
                return "LEFT_PARENTHESIS";
            }
            case ')': {
                return "RIGHT_PARENTHESIS";
            }
            case '+': {
                return "PLUS_SIGN";
            }
            case '{': {
                return "LEFT_CURLY_BRACKET";
            }
            case '}': {
                return "RIGHT_CURLY_BRACKET";
            }
            case '[': {
                return "LEFT_SQUARE_BRACKET";
            }
            case ']': {
                return "RIGHT_SQUARE_BRACKET";
            }
            case '/': {
                return "SOLIDUS";
            }
            case '|': {
                return "VERTICAL_LINE";
            }
            case '\\': {
                return "REVERSE_SOLIDUS";
            }
            case '-': {
                return "HYPHEN_MINUS";
            }
            case '=': {
                return "EQUALS_SIGN";
            }
            case ':': {
                return "COLON";
            }
            case ';': {
                return "SEMICOLON";
            }
            case '\'': {
                return "APOSTROPHE";
            }
            case '<': {
                return "LESS_THAN_SIGN";
            }
            case '>': {
                return "GREATER_THAN_SIGN";
            }
            case '?': {
                return "QUESTION_MARK";
            }
            case '\"': {
                return "QUOTATION_MARK";
            }
            case ' ': {
                return "SPACE";
            }
            default: {
                return Character.getName(c).replace(' ', '_').replace('-', '_');
            }
        }
    }
    
    public static String getTypeName(final String namespace, final String sourceName, String tableName) {
        tableName = tableName.replace(".", "_");
        final String typeName = namespace + "." + sourceName + "_" + getValidJavaIdentifierName(tableName) + "_" + "Type";
        if (TypeGenerator.logger.isInfoEnabled()) {
            TypeGenerator.logger.info((Object)("Namespace name - " + namespace + ", Source name - " + sourceName + ", Table Name - " + tableName + " - Generated TypeName - " + typeName));
        }
        return typeName;
    }
    
    static {
        TypeGenerator.logger = Logger.getLogger((Class)TypeGenerator.class);
    }
}
