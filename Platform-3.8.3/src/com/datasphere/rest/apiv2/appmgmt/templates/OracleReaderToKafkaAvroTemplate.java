package com.datasphere.rest.apiv2.appmgmt.templates;

import com.datasphere.uuid.*;
import com.datasphere.rest.apiv2.appmgmt.models.*;
import com.datasphere.metaRepository.*;

public class OracleReaderToKafkaAvroTemplate extends IntegrationAppTemplate
{
    @Override
    public Template getApplicationTemplateModel(final AuthToken token) throws MetaDataRepositoryException {
        if (this.templateModel == null) {
            final Template returnTemplate = super.getApplicationTemplateModel(token);
            final TemplateParameter versionParameter = new TemplateParameter().name("version").parameterType(TemplateParameter.ParameterTypeEnum.STRING).required(true);
            returnTemplate.getTargetParameters().add(versionParameter);
            this.templateModel = returnTemplate;
        }
        return this.templateModel;
    }
    
    @Override
    public String getTemplateId() {
        return "oracle-to-kafka-avro-cdc";
    }
    
    @Override
    public String getSourceAdapter() {
        return "OracleReader";
    }
    
    @Override
    public String getParserAdapter() {
        return null;
    }
    
    @Override
    public String getTargetAdapter() {
        return "KafkaWriter";
    }
    
    @Override
    public String getFormatterAdapter() {
        return "AvroFormatter";
    }
}
