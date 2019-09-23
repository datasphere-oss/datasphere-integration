package com.datasphere.runtime.compiler.stmts;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.proc.BaseProcess;
import com.datasphere.runtime.TraceOptions;
import com.datasphere.runtime.TypeGenerator;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;

public class CreateSourceOrTargetStmt extends CreateStmt
{
    private static final Logger logger;
    public final AdapterDescription srcOrDest;
    public final AdapterDescription parserOrFormatter;
    public final List<InputOutputSink> ios;
    
    public CreateSourceOrTargetStmt(final EntityType what, final String name, final Boolean doReplace, final AdapterDescription srcOrDest, final AdapterDescription parserOrFormatter, final List<InputOutputSink> ios) {
        super(what, name, doReplace);
        this.checkValidity("One of the params needed is null: EntityType: " + what + ", ObjectName: " + name + ", Replace: " + doReplace + ", SourceAdapter: " + srcOrDest + ", I/O Clause: " + ios + ".\n", what, name, doReplace, srcOrDest, ios);
        this.srcOrDest = srcOrDest;
        this.parserOrFormatter = parserOrFormatter;
        this.ios = ios;
    }
    
    @Override
    public String toString() {
        this.checkValidity("One of the params needed is null: EntityType: " + this.what + ", ObjectName: " + this.name + ", Replace: " + this.doReplace + ", SourceAdapter: " + this.srcOrDest + ", I/O Clause: " + this.ios + ".\n", this.what, this.name, this.doReplace, this.srcOrDest, this.ios);
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.stmtToString()).append(" OF ").append(this.srcOrDest.getAdapterTypeName()).append(" (").append(this.srcOrDest.getProps()).append(")");
        if (this.parserOrFormatter != null) {
            stringBuilder.append("PARSER ").append(this.parserOrFormatter.getAdapterTypeName()).append(" (").append(this.parserOrFormatter.getProps()).append(")");
        }
        return stringBuilder.toString();
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        this.checkValidity("Input / Output clause not defined", this.ios);
        this.checkValidity("Compiler not defined", c);
        final TraceOptions traceOptions = Compiler.buildTraceOptions(this);
        final boolean isAnySourceFiltered = this.isAnySourceFiltered(this.ios);
        if (this.what == EntityType.SOURCE) {
            try {
                final ArrayList<String> selectText = this.getSelectTexts(this.ios, traceOptions);
                final MetaInfo.Source sourceInfo = (MetaInfo.Source)c.createFilteredSource(this);
                sourceInfo.injectOutputClauses(this.ios);
                MetadataRepository.getINSTANCE().updateMetaObject(sourceInfo, c.getContext().getAuthToken());
                this.sourceAdapterRelatedCompilationOperations(c, sourceInfo);
            }
            catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            }
            return null;
        }
        final InputOutputSink ios = this.ios.get(0);
        c.compileCreateSourceOrTargetStmt(this);
        if (ios.getGeneratedStream() != null) {
            final TypeGenerator generator = new TypeGenerator(c, this.name);
            generator.generateStream(c, Collections.singletonList(ios.getGeneratedStream()), ios.getStreamName(), true);
        }
        try {
            final MetaInfo.Target targetInfo = c.getContext().getTargetInCurSchema(this.name);
            this.targetAdapterRelatedCompilationOperations(c, targetInfo);
        }
        catch (Exception e2) {
            throw new MetaDataRepositoryException(e2.getLocalizedMessage(), e2);
        }
        return null;
    }
    
    private boolean isAnySourceFiltered(final List<InputOutputSink> outputClauses) {
        if (this.what == EntityType.TARGET) {
            return false;
        }
        if (outputClauses == null) {
            throw new RuntimeException("Problem with output clause for the source.");
        }
        for (final InputOutputSink inputOutputSink : outputClauses) {
            if (inputOutputSink.isFiltered()) {
                return true;
            }
        }
        return false;
    }
    
    private ArrayList<String> getSelectTexts(final List<InputOutputSink> ios, final TraceOptions traceOptions) {
        final ArrayList<String> selectTexts = new ArrayList<String>();
        for (final InputOutputSink inputOutputSink : ios) {
            if (inputOutputSink.getFilterText() == null) {
                continue;
            }
            String TQL = inputOutputSink.getFilterText().trim();
            if (TQL != null && TQL.length() >= 1 && TQL.endsWith(",")) {
                TQL = TQL.substring(0, TQL.length() - 1);
            }
            if (inputOutputSink.getGeneratedStream() == null) {
                if (inputOutputSink.getStreamName() == null) {
                    continue;
                }
                if (inputOutputSink.getTypeDefinition() != null) {
                    selectTexts.add(inputOutputSink.getStreamName().trim() + " ( " + Joiner.on(", ").join((Iterable)inputOutputSink.getTypeDefinition()) + " ) " + TQL);
                }
                else {
                    selectTexts.add(inputOutputSink.getStreamName().trim() + " " + TQL);
                }
            }
            else {
                final MappedStream ms = inputOutputSink.getGeneratedStream();
                if (ms == null || ms.streamName == null || ms.mappingProperties == null) {
                    continue;
                }
                final Joiner.MapJoiner mapJoiner = Joiner.on(',').withKeyValueSeparator(":");
                selectTexts.add(ms.streamName.trim() + " ( " + mapJoiner.join((Map)ms.mappingProperties) + " ) " + TQL);
            }
        }
        if (this.isWithDebugEnabled() && selectTexts != null) {
            for (final String selectText : selectTexts) {
                CreateSourceOrTargetStmt.logger.warn((Object)("Select extracted: " + selectText));
            }
        }
        return selectTexts;
    }
    
    private void sourceAdapterRelatedCompilationOperations(final Compiler compiler, final MetaInfo.Source sourceMetaInfo) throws Exception {
        if (sourceMetaInfo != null) {
            this.validateAdapter(compiler, sourceMetaInfo.adapterClassName, sourceMetaInfo);
        }
    }
    
    private void targetAdapterRelatedCompilationOperations(final Compiler compiler, final MetaInfo.Target targetMetaInfo) throws Exception {
        if (targetMetaInfo != null) {
            this.validateAdapter(compiler, targetMetaInfo.adapterClassName, targetMetaInfo);
        }
    }
    
    private void validateAdapter(final Compiler compiler, final String adapterClassName, final MetaInfo.MetaObject metaObject) throws Exception {
        final Class<?> adapterFactory = ClassLoader.getSystemClassLoader().loadClass(adapterClassName);
        final BaseProcess proc = (BaseProcess)adapterFactory.newInstance();
        proc.onCompile(compiler, metaObject);
    }
    
    public String getStreamName() {
        this.checkValidity("Input / Output clause not defined", this.ios);
        return this.ios.get(0).getStreamName();
    }
    
    public Select getFilter() {
        this.checkValidity("Input / Output clause not defined", this.ios);
        if (this.ios.get(0).isFiltered()) {
            return this.ios.get(0).getFilter();
        }
        return null;
    }
    
    public List<String> getPartitionFields() {
        return this.ios.get(0).getPartitionFields();
    }
    
    public MappedStream getGeneratedStream() {
        return this.ios.get(0).getGeneratedStream();
    }
    
    static {
        logger = Logger.getLogger((Class)CreateSourceOrTargetStmt.class);
    }
}
