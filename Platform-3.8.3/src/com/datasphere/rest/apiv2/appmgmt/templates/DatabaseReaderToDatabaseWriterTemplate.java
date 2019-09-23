package com.datasphere.rest.apiv2.appmgmt.templates;

public class DatabaseReaderToDatabaseWriterTemplate extends IntegrationAppTemplate
{
    @Override
    public String getTemplateId() {
        return "database-to-database-initialload";
    }
    
    @Override
    public String getSourceAdapter() {
        return "DatabaseReader";
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
