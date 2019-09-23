package com.datasphere.sourcefiltering;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.apache.log4j.Logger;
import org.elasticsearch.common.UUIDs;

import com.datasphere.kafkamessaging.StreamPersistencePolicy;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.TypeGenerator;
import com.datasphere.runtime.compiler.AST;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.ObjectName;
import com.datasphere.runtime.compiler.exprs.ObjectRef;
import com.datasphere.runtime.compiler.select.DataSource;
import com.datasphere.runtime.compiler.stmts.CreateCqStmt;
import com.datasphere.runtime.compiler.stmts.CreateSourceOrTargetStmt;
import com.datasphere.runtime.compiler.stmts.CreateStreamStmt;
import com.datasphere.runtime.compiler.stmts.InputOutputSink;
import com.datasphere.runtime.compiler.stmts.MappedStream;
import com.datasphere.runtime.compiler.stmts.OutputClause;
import com.datasphere.runtime.compiler.stmts.Select;
import com.datasphere.runtime.compiler.stmts.SelectTarget;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.meta.MetaInfoStatus;
import com.datasphere.runtime.utils.RuntimeUtils;
import com.datasphere.uuid.UUID;

public class SourceSideFilteringHandler implements Handler
{
    private static final Logger logger;
    
    private void dump(final Boolean isMapped, final Object... args) {
        final StringBuilder stringBuffer = new StringBuilder();
        final MetaInfo.Source src = (MetaInfo.Source)args[0];
        stringBuffer.append("\n Source name: ").append(src.getFullName()).append(" Outputs to: ").append(MetaInfo.MetaObject.obtainMetaObject(src.getOutputStream()).getFullName()).append(" \n ");
        if (!isMapped) {
            final List<UUID> UUIDs = (List<UUID>)args[1];
            final UUID cqUUID = UUIDs.get(1);
            final MetaInfo.CQ firstCQ = (MetaInfo.CQ)MetaInfo.MetaObject.obtainMetaObject(cqUUID);
            stringBuffer.append("CQ ").append(firstCQ.getFullName()).append(" FROM: ").append(firstCQ.plan.getDataSourceList()).append(" => ");
            stringBuffer.append("TO Stream: ").append(MetaInfo.MetaObject.obtainMetaObject(firstCQ.stream).getFullName()).append("\n ");
        }
        else {
            final List<UUID> UUIDs = (List<UUID>)args[1];
            final UUID firstCQUUID = UUIDs.get(0);
            final UUID firstStreamUUID = UUIDs.get(1);
            final UUID secondCQUUID = UUIDs.get(3);
            final UUID secondStreamUUID = UUIDs.get(4);
            final MetaInfo.CQ firstCQ2 = (MetaInfo.CQ)MetaInfo.MetaObject.obtainMetaObject(firstCQUUID);
            stringBuffer.append("CQ ").append(firstCQ2.getFullName()).append(" FROM: ").append(firstCQ2.plan.getDataSourceList()).append(" => ");
            stringBuffer.append("TO Stream: ").append(MetaInfo.MetaObject.obtainMetaObject(firstStreamUUID).getFullName()).append(" \n ");
            final MetaInfo.CQ secondCQ = (MetaInfo.CQ)MetaInfo.MetaObject.obtainMetaObject(secondCQUUID);
            stringBuffer.append("CQ ").append(secondCQ.getFullName()).append(" FROM: ").append(secondCQ.plan.getDataSourceList()).append(" => ");
            stringBuffer.append("TO Stream: ").append(MetaInfo.MetaObject.obtainMetaObject(secondStreamUUID).getFullName()).append(" \n ");
        }
        SourceSideFilteringHandler.logger.warn((Object)stringBuffer.toString());
    }
    
    @Override
    public Object handle(final Compiler compiler, final CreateSourceOrTargetStmt createSourceOrTargetStmt) throws MetaDataRepositoryException {
        if (createSourceOrTargetStmt == null || createSourceOrTargetStmt.ios == null || createSourceOrTargetStmt.ios.isEmpty()) {
            throw new RuntimeException("Null value passed");
        }
        final Context ctx = compiler.getContext();
        final boolean doReplace = createSourceOrTargetStmt.doReplace;
        final ObjectName sourceName = ctx.makeObjectName(createSourceOrTargetStmt.name);
        if (createSourceOrTargetStmt.ios.size() == 1) {
            final InputOutputSink sink = createSourceOrTargetStmt.ios.get(0);
            if (sink != null && !sink.isFiltered() && sink.getGeneratedStream() == null && sink.getTypeDefinition() == null) {
                try {
                    return this.createSourceWithImplicitStream(compiler, ctx, createSourceOrTargetStmt, doReplace, sourceName.getFullName(), createSourceOrTargetStmt.getStreamName());
                }
                catch (MetaDataRepositoryException e) {
                    try {
                        this.rollbackOperation(ctx, null, new MetaInfo.MetaObject[0]);
                        throw e;
                    }
                    catch (MetaDataRepositoryException e2) {
                        this.log(e2);
                        throw e2;
                    }
                }
            }
        }
        MetaInfo.Source sourceObject = null;
        final String streamName = RuntimeUtils.genRandomName(createSourceOrTargetStmt.getStreamName());
        final ObjectName streamObjectNameTier1 = ctx.makeObjectName(streamName);
        MetaInfo.Stream implicitStream = null;
        final List<UUID> coDependentUUIDs = new ArrayList<UUID>();
        try {
            final MetaInfo.Type inputType = this.findTypeOfSourceAdapter(compiler, ctx, createSourceOrTargetStmt);
            implicitStream = this.createImplicitStreamForRedirect(compiler, ctx, doReplace, streamObjectNameTier1, inputType);
            this.changeMetaObjectState(ctx, implicitStream, Boolean.valueOf(true), null);
            sourceObject = this.createSourceWithImplicitStream(compiler, ctx, createSourceOrTargetStmt, doReplace, sourceName.getFullName(), streamObjectNameTier1.getFullName());
            for (final InputOutputSink sink2 : createSourceOrTargetStmt.ios) {
                final List<UUID> outputClauseImplicitItems = new ArrayList<UUID>();
                if (sink2.isFiltered() && sink2.getGeneratedStream() == null && sink2.getTypeDefinition() != null) {
                    if (createSourceOrTargetStmt.isWithDebugEnabled()) {
                        SourceSideFilteringHandler.logger.warn((Object)"Filtering with Type defined");
                    }
                    outputClauseImplicitItems.addAll(this.handleSimpleFilterWithTypeDef(sourceObject, compiler, ctx, doReplace, sink2, streamObjectNameTier1));
                    if (createSourceOrTargetStmt.isWithDebugEnabled()) {
                        this.dump(false, sourceObject, outputClauseImplicitItems);
                    }
                }
                else if (sink2.isFiltered() && sink2.getGeneratedStream() != null && sink2.getTypeDefinition() == null) {
                    if (createSourceOrTargetStmt.isWithDebugEnabled()) {
                        SourceSideFilteringHandler.logger.warn((Object)"Filtering with Map() clause");
                    }
                    outputClauseImplicitItems.addAll(this.handleMappedStream(sourceObject, compiler, ctx, doReplace, sink2, streamObjectNameTier1));
                    if (createSourceOrTargetStmt.isWithDebugEnabled()) {
                        this.dump(true, sourceObject, outputClauseImplicitItems);
                    }
                }
                else if (sink2.isFiltered() && sink2.getGeneratedStream() == null && sink2.getTypeDefinition() == null) {
                    if (createSourceOrTargetStmt.isWithDebugEnabled()) {
                        SourceSideFilteringHandler.logger.warn((Object)"Filtering with Type not defined");
                    }
                    outputClauseImplicitItems.addAll(this.handleSimpleFilterWithOutTypeDef(createSourceOrTargetStmt, sourceObject, compiler, ctx, doReplace, sink2, streamObjectNameTier1));
                    if (createSourceOrTargetStmt.isWithDebugEnabled()) {
                        this.dump(false, sourceObject, outputClauseImplicitItems);
                    }
                }
                else if (!sink2.isFiltered() && sink2.getGeneratedStream() == null && sink2.getTypeDefinition() == null) {
                    if (createSourceOrTargetStmt.isWithDebugEnabled()) {
                        SourceSideFilteringHandler.logger.warn((Object)"No filtering with Type not defined");
                    }
                    outputClauseImplicitItems.addAll(this.handleSimpleOutput(createSourceOrTargetStmt, sourceObject, compiler, ctx, doReplace, sink2, streamObjectNameTier1));
                }
                else {
                    if (sink2.isFiltered() || sink2.getGeneratedStream() == null || sink2.getTypeDefinition() != null) {
                        throw new RuntimeException("Problem in creating filtered source");
                    }
                    if (createSourceOrTargetStmt.isWithDebugEnabled()) {
                        SourceSideFilteringHandler.logger.warn((Object)"No filtering with Map() clause");
                    }
                    outputClauseImplicitItems.addAll(this.handleSimpleOutputWithMap(createSourceOrTargetStmt, sourceObject, compiler, ctx, doReplace, sink2, streamObjectNameTier1));
                }
                if (coDependentUUIDs != null && outputClauseImplicitItems != null) {
                    coDependentUUIDs.addAll(outputClauseImplicitItems);
                }
            }
        }
        catch (MetaDataRepositoryException e4) {
            try {
                this.rollbackOperation(ctx, coDependentUUIDs, sourceObject, implicitStream);
            }
            catch (MetaDataRepositoryException e3) {
                this.log(e3);
            }
            this.log(e4);
        }
        return sourceObject;
    }
    
    private Collection<UUID> handleSimpleOutputWithMap(final CreateSourceOrTargetStmt createSourceOrTargetStmt, final MetaInfo.Source sourceObject, final Compiler compiler, final Context ctx, final boolean doReplace, final InputOutputSink sink, final ObjectName streamObjectNameTier1) throws MetaDataRepositoryException {
        final List<UUID> allObjectsCreated = new ArrayList<UUID>();
        final MappedStream mappedStream = sink.getGeneratedStream();
        final String lastStream = mappedStream.streamName;
        final TypeGenerator generator = new TypeGenerator(compiler, sourceObject.name);
        SourceSideFilteringHandler.logger.warn((Object)sink.getGeneratedStream().streamName);
        final List<UUID> UUIDs = generator.generateStream(compiler, Collections.singletonList(sink.getGeneratedStream()), streamObjectNameTier1.getFullName(), true);
        allObjectsCreated.addAll(UUIDs);
        this.changeMetaObjectState(ctx, allObjectsCreated, Boolean.valueOf(true), null);
        return allObjectsCreated;
    }
    
    private Collection<UUID> handleSimpleOutput(final CreateSourceOrTargetStmt stmt, final MetaInfo.Source sourceObject, final Compiler compiler, final Context ctx, final boolean doReplace, final InputOutputSink sink, final ObjectName streamObjectNameTier1) throws MetaDataRepositoryException {
        final List<UUID> allObjectsCreated = new ArrayList<UUID>();
        final String cqName = RuntimeUtils.genRandomName(sink.getStreamName());
        final MetaInfo.Type t = this.findTypeOfSourceAdapter(compiler, ctx, stmt);
        final MetaInfo.Stream outputStream = this.createStreamWithGivenType(compiler, ctx, sink.getStreamName(), t.getFullName(), doReplace);
        this.changeMetaObjectState(ctx, outputStream, null, Boolean.valueOf(true));
        allObjectsCreated.add(outputStream.getUuid());
        final List<DataSource> source_list = new ArrayList<DataSource>();
        source_list.add(AST.SourceStream(streamObjectNameTier1.getFullName(), null, "$all"));
        final List<SelectTarget> target_list = new ArrayList<SelectTarget>();
        target_list.add(new SelectTarget(new ObjectRef("$all"), null));
        final Select sel = new Select(false, 1, target_list, source_list, null, null, null, null, null, false, null);
        final CreateCqStmt ccs = new CreateCqStmt(cqName, doReplace, sink.getStreamName(), new ArrayList<String>(), sel, "select $all;");
        final MetaInfo.CQ cqMetaObject = (MetaInfo.CQ)compiler.compileCreateCqStmt(ccs);
        this.changeMetaObjectState(ctx, cqMetaObject, Boolean.valueOf(true), null);
        return allObjectsCreated;
    }
    
    private Collection<UUID> handleMappedStream(final MetaInfo.Source sourceMetaObject, final Compiler compiler, final Context ctx, final Boolean doReplace, final InputOutputSink sink, final ObjectName sourceStreamName) {
        final List<UUID> allObjectsCreated = new ArrayList<UUID>();
        try {
            final MappedStream mappedStream = sink.getGeneratedStream();
            final String lastStream = mappedStream.streamName;
            final String secondStream = RuntimeUtils.genRandomName(lastStream);
            final String firstCQ = RuntimeUtils.genRandomName(lastStream);
            final String oldStream = mappedStream.streamName;
            this.redirectStreamForGeneratedStream(mappedStream, secondStream);
            final TypeGenerator generator = new TypeGenerator(compiler, sourceMetaObject.name);
            final List<UUID> UUIDs = generator.generateStream(compiler, Collections.singletonList(sink.getGeneratedStream()), sourceStreamName.getFullName(), false);
            allObjectsCreated.addAll(UUIDs);
            this.changeMetaObjectState(ctx, allObjectsCreated, Boolean.valueOf(true), null);
            this.setSelectFromClauseToImplicitStream(compiler, ctx, sink, secondStream, null);
            final MetaInfo.CQ cqMetaObject = this.createCQWithImplicitStream(compiler, ctx, doReplace, sink, firstCQ, lastStream);
            this.changeMetaObjectState(ctx, cqMetaObject, Boolean.valueOf(true), null);
            allObjectsCreated.add(cqMetaObject.getUuid());
            allObjectsCreated.add(cqMetaObject.stream);
            for (final UUID uuid : allObjectsCreated) {
                sourceMetaObject.addCoDependentObject(uuid);
            }
            sourceMetaObject.addCoDependentObject(cqMetaObject.getUuid());
            ctx.updateMetaObject(sourceMetaObject);
            this.redirectStreamForGeneratedStream(mappedStream, oldStream);
        }
        catch (MetaDataRepositoryException e) {
            this.log(e);
        }
        return allObjectsCreated;
    }
    
    private Collection<UUID> handleSimpleFilterWithOutTypeDef(final CreateSourceOrTargetStmt stmt, final MetaInfo.Source sourceMetaObject, final Compiler compiler, final Context ctx, final Boolean doReplace, final InputOutputSink sink, final ObjectName sourceStreamName) {
        final List<UUID> allObjectsCreated = new ArrayList<UUID>();
        try {
            final String cqName = RuntimeUtils.genRandomName(sink.getStreamName());
            this.setSelectFromClauseToImplicitStream(compiler, ctx, sink, sourceStreamName.getFullName(), "$all");
            final MetaInfo.Type t = this.findTypeOfSourceAdapter(compiler, ctx, stmt);
            final MetaInfo.Stream outputStream = this.createStreamWithGivenType(compiler, ctx, sink.getStreamName(), t.getFullName(), doReplace);
            this.changeMetaObjectState(ctx, outputStream, null, Boolean.valueOf(true));
            allObjectsCreated.add(outputStream.getUuid());
            final MetaInfo.CQ filterCQ = this.createCQWithImplicitStream(compiler, ctx, doReplace, sink, cqName, sink.getStreamName());
            this.changeMetaObjectState(ctx, filterCQ, Boolean.valueOf(true), null);
            allObjectsCreated.add(filterCQ.getUuid());
            allObjectsCreated.add(outputStream.getDataType());
            sourceMetaObject.addCoDependentObject(filterCQ.getUuid());
            sourceMetaObject.addCoDependentObject(outputStream.getUuid());
            sourceMetaObject.addCoDependentObject(outputStream.getDataType());
            ctx.updateMetaObject(sourceMetaObject);
        }
        catch (MetaDataRepositoryException e) {
            this.log(e);
        }
        return allObjectsCreated;
    }
    
    private Collection<UUID> handleSimpleFilterWithTypeDef(final MetaInfo.Source sourceMetaObject, final Compiler compiler, final Context ctx, final Boolean doReplace, final InputOutputSink sink, final ObjectName sourceStreamName) {
        final List<UUID> allObjectsCreated = new ArrayList<UUID>();
        final String cqName = RuntimeUtils.genRandomName(sink.getStreamName());
        try {
            this.setSelectFromClauseToImplicitStream(compiler, ctx, sink, sourceStreamName.getFullName(), "$all");
            final MetaInfo.Stream outputStream = this.createStreamWithGivenType(compiler, ctx, sink, doReplace);
            this.changeMetaObjectState(ctx, outputStream, null, Boolean.valueOf(true));
            allObjectsCreated.add(outputStream.getUuid());
            final MetaInfo.CQ filterCQ = this.createCQWithImplicitStream(compiler, ctx, doReplace, sink, cqName, sink.getStreamName());
            this.changeMetaObjectState(ctx, filterCQ, Boolean.valueOf(true), null);
            allObjectsCreated.add(filterCQ.getUuid());
            allObjectsCreated.add(outputStream.getDataType());
            sourceMetaObject.addCoDependentObject(filterCQ.getUuid());
            sourceMetaObject.addCoDependentObject(outputStream.getUuid());
            sourceMetaObject.addCoDependentObject(outputStream.getDataType());
            ctx.updateMetaObject(sourceMetaObject);
        }
        catch (MetaDataRepositoryException e) {
            this.log(e);
        }
        return allObjectsCreated;
    }
    
    private void changeMetaObjectState(@NotNull final Context context, @NotNull final List<UUID> UUIDs, @Nullable final Boolean setAnonymous, @Nullable final Boolean setGenerated) throws MetaDataRepositoryException {
        if (UUIDs == null) {
            throw new RuntimeException("Problem in creating MAP() internal components");
        }
        if (UUIDs != null) {
            for (final UUID uuid : UUIDs) {
                final MetaInfo.MetaObject obj = context.getObject(uuid);
                if (obj != null) {
                    this.changeMetaObjectState(context, obj, setAnonymous, setGenerated);
                }
            }
        }
    }
    
    private void changeMetaObjectState(@NotNull final Context context, @NotNull final MetaInfo.MetaObject object, @Nullable final Boolean setAnonymous, @Nullable final Boolean setGenerated) throws MetaDataRepositoryException {
        if (object == null) {
            throw new RuntimeException("Problem in altering null object with setAnonymous: " + setAnonymous + " setGenerated: " + setGenerated);
        }
        if (setGenerated != null) {
            object.getMetaInfoStatus().setGenerated(setGenerated);
        }
        if (setAnonymous != null) {
            object.getMetaInfoStatus().setAnonymous(setAnonymous);
        }
        context.updateMetaObject(object);
    }
    
    private void rollbackOperation(final Context context, @Nullable final List<UUID> optionalUUIDs, final MetaInfo.MetaObject... objects) throws MetaDataRepositoryException {
        if (context == null || objects == null) {
            return;
        }
        for (final MetaInfo.MetaObject obj : objects) {
            if (obj != null) {
                context.removeObject(obj);
            }
        }
    }
    
    private MetaInfo.Type findTypeOfSourceAdapter(@NotNull final Compiler compiler, @NotNull final Context ctx, final CreateSourceOrTargetStmt createSourceWithImplicitCQStmt) throws MetaDataRepositoryException {
        return compiler.prepareSource(createSourceWithImplicitCQStmt.srcOrDest.getAdapterTypeName(), createSourceWithImplicitCQStmt.srcOrDest, createSourceWithImplicitCQStmt.parserOrFormatter);
    }
    
    private MetaInfo.Stream createImplicitStreamForRedirect(@NotNull final Compiler compiler, @NotNull final Context ctx, final boolean doReplace, final ObjectName streamObjectNameTier1, final MetaInfo.Type inputType) throws MetaDataRepositoryException {
        return ctx.putStream(doReplace, streamObjectNameTier1, inputType.getUuid(), null, null, null, null, new MetaInfoStatus().setAnonymous(true));
    }
    
    private void setSelectFromClauseToImplicitStream(@NotNull final Compiler compiler, @NotNull final Context ctx, final InputOutputSink sink, final String streamName, final String alias) {
        (sink.getFilter().from = new ArrayList<DataSource>()).add(AST.SourceStream(streamName, null, alias));
    }
    
    private MetaInfo.Stream createStreamWithGivenType(@NotNull final Compiler compiler, @NotNull final Context ctx, final InputOutputSink sink, final boolean doReplace) throws MetaDataRepositoryException {
        final StreamPersistencePolicy spp = new StreamPersistencePolicy(null);
        final CreateStreamStmt createStreamStmt = new CreateStreamStmt(sink.getStreamName(), doReplace, null, null, sink.getTypeDefinition(), null, spp);
        return (MetaInfo.Stream)compiler.compileCreateStreamStmt(createStreamStmt);
    }
    
    private MetaInfo.Stream createStreamWithGivenType(@NotNull final Compiler compiler, @NotNull final Context ctx, final String streamName, final String typeName, final boolean doReplace) throws MetaDataRepositoryException {
        final StreamPersistencePolicy spp = new StreamPersistencePolicy(null);
        final CreateStreamStmt createStreamStmt = new CreateStreamStmt(streamName, doReplace, null, typeName, null, null, spp);
        return (MetaInfo.Stream)compiler.compileCreateStreamStmt(createStreamStmt);
    }
    
    private MetaInfo.CQ createCQWithImplicitStream(@NotNull final Compiler compiler, @NotNull final Context ctx, final boolean doReplace, final InputOutputSink sink, @NotNull final String cqName, @NotNull final String streamName) throws MetaDataRepositoryException {
        final CreateCqStmt ccs = new CreateCqStmt(cqName, doReplace, streamName, new ArrayList<String>(), sink.getFilter(), sink.getFilterText());
        return (MetaInfo.CQ)compiler.compileCreateCqStmt(ccs);
    }
    
    private MetaInfo.Source createSourceWithImplicitStream(@NotNull final Compiler compiler, @NotNull final Context ctx, final CreateSourceOrTargetStmt createSourceWithImplicitCQStmt, final boolean doReplace, final String sourceName, final String streamName) throws MetaDataRepositoryException {
        final ArrayList<InputOutputSink> sinks = new ArrayList<InputOutputSink>();
        sinks.add(new OutputClause(streamName, null, new ArrayList<String>(), null, null, streamName));
        final CreateSourceOrTargetStmt css = new CreateSourceOrTargetStmt(EntityType.SOURCE, sourceName, doReplace, createSourceWithImplicitCQStmt.srcOrDest, createSourceWithImplicitCQStmt.parserOrFormatter, sinks);
        return (MetaInfo.Source)compiler.compileCreateSourceOrTargetStmt(css);
    }
    
    private void redirectStreamForGeneratedStream(@Nullable final MappedStream getGeneratedStream, @NotNull final String toStream) {
        if (toStream == null) {
            throw new RuntimeException("Mapped stream redirection failed");
        }
        if (getGeneratedStream != null) {
            getGeneratedStream.streamName = toStream;
        }
    }
    
    private void log(final Exception e) {
        if (e != null && SourceSideFilteringHandler.logger.isDebugEnabled()) {
            SourceSideFilteringHandler.logger.debug((Object)e.getMessage(), (Throwable)e);
        }
    }
    
    static {
        logger = Logger.getLogger((Class)SourceSideFilteringHandler.class);
    }
}
