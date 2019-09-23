package com.datasphere.rest.apiv2.appmgmt.templates;

import com.datasphere.security.*;
import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.runtime.components.*;
import com.datasphere.kafkamessaging.*;
import com.datasphere.metaRepository.*;
import com.datasphere.runtime.*;
import java.util.*;
import com.datasphere.runtime.compiler.stmts.*;
import com.datasphere.rest.apiv2.appmgmt.models.*;

public abstract class IntegrationAppTemplate implements ApplicationTemplate
{
    protected Template templateModel;
    
    public IntegrationAppTemplate() {
        this.templateModel = null;
    }
    
    public abstract String getTemplateId();
    
    public abstract String getSourceAdapter();
    
    public abstract String getParserAdapter();
    
    public abstract String getTargetAdapter();
    
    public abstract String getFormatterAdapter();
    
    public static TemplateParameter.ParameterTypeEnum convertParameterTypeClassToEnum(final Class<?> clazz) {
        if (clazz == Boolean.class) {
            return TemplateParameter.ParameterTypeEnum.BOOL;
        }
        if (clazz == Integer.class || clazz == Long.class) {
            return TemplateParameter.ParameterTypeEnum.INT;
        }
        if (clazz == Password.class) {
            return TemplateParameter.ParameterTypeEnum.SECURESTRING;
        }
        return TemplateParameter.ParameterTypeEnum.STRING;
    }
    
    @Override
    public Template getApplicationTemplateModel(final AuthToken token) throws MetaDataRepositoryException {
        if (this.templateModel != null) {
            return this.templateModel;
        }
        (this.templateModel = new Template()).setTemplateName(this.getTemplateId());
        MetaInfo.PropertyTemplateInfo pti = (MetaInfo.PropertyTemplateInfo)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYTEMPLATE, "Global", this.getSourceAdapter(), null, token);
        for (final Map.Entry<String, MetaInfo.PropertyDef> entry : pti.getPropertyMap().entrySet()) {
            final TemplateParameter parameter = new TemplateParameter();
            parameter.setName(entry.getKey());
            parameter.setRequired(entry.getValue().isRequired());
            parameter.setParameterType(convertParameterTypeClassToEnum(entry.getValue().getType()));
            this.templateModel.getSourceParameters().add(parameter);
        }
        pti = (MetaInfo.PropertyTemplateInfo)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYTEMPLATE, "Global", this.getTargetAdapter(), null, token);
        for (final Map.Entry<String, MetaInfo.PropertyDef> entry : pti.getPropertyMap().entrySet()) {
            final TemplateParameter parameter = new TemplateParameter();
            parameter.setName(entry.getKey());
            parameter.setRequired(entry.getValue().isRequired());
            parameter.setParameterType(convertParameterTypeClassToEnum(entry.getValue().getType()));
            this.templateModel.getTargetParameters().add(parameter);
        }
        if (this.getFormatterAdapter() != null) {
            pti = (MetaInfo.PropertyTemplateInfo)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYTEMPLATE, "Global", this.getFormatterAdapter(), null, token);
            for (final Map.Entry<String, MetaInfo.PropertyDef> entry : pti.getPropertyMap().entrySet()) {
                final TemplateParameter parameter = new TemplateParameter();
                parameter.setName(entry.getKey());
                parameter.setRequired(entry.getValue().isRequired());
                parameter.setParameterType(convertParameterTypeClassToEnum(entry.getValue().getType()));
                this.templateModel.getFormatterParameters().add(parameter);
            }
        }
        if (this.getParserAdapter() != null) {
            pti = (MetaInfo.PropertyTemplateInfo)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYTEMPLATE, "Global", this.getParserAdapter(), null, token);
            for (final Map.Entry<String, MetaInfo.PropertyDef> entry : pti.getPropertyMap().entrySet()) {
                final TemplateParameter parameter = new TemplateParameter();
                parameter.setName(entry.getKey());
                parameter.setRequired(entry.getValue().isRequired());
                parameter.setParameterType(convertParameterTypeClassToEnum(entry.getValue().getType()));
                this.templateModel.getParserParameters().add(parameter);
            }
        }
        return this.templateModel;
    }
    
    @Override
    public void createApplication(final AuthToken token, final ApplicationParameters applicationParameters) throws MetaDataRepositoryException {
        if (applicationParameters == null || applicationParameters.getApplicationName().isEmpty()) {
            throw new IllegalArgumentException("Required parameter applicationName not found");
        }
        final String[] nsAndApp = applicationParameters.getApplicationName().split("\\.");
        if (nsAndApp == null || nsAndApp.length != 2) {
            throw new IllegalArgumentException("Required parameter applicationName not found in the specified format namespace.applicationName");
        }
        final QueryValidator queryValidator = new QueryValidator(Server.getServer());
        queryValidator.createNewContext(token);
        queryValidator.setUpdateMode(true);
        final String namespace = nsAndApp[0];
        final String applicationName = nsAndApp[1];
        final MetaInfo.Namespace namsespaceInfo = (MetaInfo.Namespace)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.NAMESPACE, "Global", namespace, null, token);
        if (namsespaceInfo == null) {
            queryValidator.CreateNameSpaceStatement(token, applicationParameters.getApplicationName(), false);
        }
        queryValidator.CreateUseSchemaStmt(token, namespace);
        boolean isAppEncrypted = false;
        RecoveryDescription recoveryDescription = null;
        ExceptionHandler exceptionHandler = null;
        if (applicationParameters.getApplicationSettings() != null) {
            final ApplicationParametersApplicationSettings settings = applicationParameters.getApplicationSettings();
            isAppEncrypted = (settings.getEncryption() != null && settings.getEncryption());
            if (settings.getRecovery() != null && settings.getRecovery().getEnabled()) {
                recoveryDescription = new RecoveryDescription(2, settings.getRecovery().getPeriod());
            }
            if (settings.getExceptionHandlers() != null) {
                final List<Property> propeties = new ArrayList<Property>();
                for (final Map.Entry<String, String> entry : settings.getExceptionHandlers().entrySet()) {
                    propeties.add(new Property(entry.getKey(), entry.getValue()));
                }
                exceptionHandler = new ExceptionHandler(propeties);
            }
        }
        queryValidator.CreateAppStatement(token, applicationName, true, null, isAppEncrypted, recoveryDescription, exceptionHandler, null, null);
        queryValidator.setCurrentApp(namespace + "." + applicationName, token);
        final String streamName = applicationName + "_stream";
        final String sourceFlowName = applicationName + "_sourceFlow";
        final String targetFlowName = applicationName + "_targetFlow";
        final String sourceName = applicationName + "_source";
        final String targetName = applicationName + "_target";
        queryValidator.CreateStreamStatement(token, streamName, true, new String[0], "Global.WAEvent", new StreamPersistencePolicy(null));
        queryValidator.CreateFlowStatement(token, sourceFlowName, true, null);
        queryValidator.setCurrentFlow(namespace + "." + sourceFlowName, token);
        final List<Property> sourcePropList = new ArrayList<Property>();
        for (final Map.Entry<String, String> entry2 : applicationParameters.getSourceParameters().entrySet()) {
            sourcePropList.add(new Property(entry2.getKey(), entry2.getValue()));
        }
        List<Property> parserPropList = null;
        if (applicationParameters.getParserParameters() != null) {
            parserPropList = new ArrayList<Property>();
            for (final Map.Entry<String, String> entry3 : applicationParameters.getParserParameters().entrySet()) {
                parserPropList.add(new Property(entry3.getKey(), entry3.getValue()));
            }
        }
        String sourceAdapterVersion = null;
        if (applicationParameters.getSourceParameters().containsKey("version")) {
            sourceAdapterVersion = applicationParameters.getSourceParameters().get("version");
        }
        queryValidator.CreateSourceStatement_New(token, sourceName, true, this.getSourceAdapter(), sourceAdapterVersion, sourcePropList, this.getParserAdapter(), null, parserPropList, streamName);
        queryValidator.setCurrentFlow(null, token);
        queryValidator.CreateFlowStatement(token, targetFlowName, true, null);
        queryValidator.setCurrentFlow(namespace + "." + targetFlowName, token);
        final Map<String, String>[] props = (Map<String, String>[])new Map[] { applicationParameters.getTargetParameters() };
        Map<String, String>[] formatterProps = null;
        if (applicationParameters.getFormatterParameters() != null) {
            formatterProps = (Map<String, String>[])new Map[] { applicationParameters.getFormatterParameters() };
        }
        String targetAdapterVersion = null;
        if (applicationParameters.getTargetParameters().containsKey("version")) {
            targetAdapterVersion = applicationParameters.getTargetParameters().get("version");
        }
        queryValidator.CreateTargetStatement(token, targetName, true, this.getTargetAdapter(), targetAdapterVersion, props, this.getFormatterAdapter(), null, formatterProps, streamName);
        queryValidator.setCurrentFlow(null, token);
        queryValidator.setCurrentApp(null, token);
        final MetaInfo.Flow appFlow = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, namespace, applicationName, null, token);
        if (appFlow == null) {
            throw new MetaDataRepositoryException("Could not create application:" + applicationParameters.getApplicationName());
        }
    }
}
