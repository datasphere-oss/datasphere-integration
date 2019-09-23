package com.datasphere.rest.apiv2.appmgmt.templates;

public class OracleReaderToDatabaseWriterTemplate extends IntegrationAppTemplate
{
    @Override
    public String getTemplateId() {
        return "oracle-to-database-cdc";
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
        return "DatabaseWriter";
    }
    
    @Override
    public String getFormatterAdapter() {
        return null;
    }
}
