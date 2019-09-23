package com.datasphere.runtime;

import com.datasphere.runtime.meta.*;
import java.util.*;
import org.codehaus.jackson.*;
import com.fasterxml.jackson.databind.*;
import java.io.*;

public class VisualizationArtifacts
{
    private MetaInfo.Dashboard dashboard;
    private List<MetaInfo.Page> pages;
    private List<MetaInfo.QueryVisualization> queryVisualizations;
    private List<MetaInfo.Query> parametrizedQuery;
    
    public VisualizationArtifacts() {
        this.dashboard = new MetaInfo.Dashboard();
        this.pages = new ArrayList<MetaInfo.Page>();
        this.queryVisualizations = new ArrayList<MetaInfo.QueryVisualization>();
        this.parametrizedQuery = new ArrayList<MetaInfo.Query>();
    }
    
    public void setDashboard(final MetaInfo.Dashboard dashboard) {
        this.dashboard = dashboard;
    }
    
    public void addPages(final MetaInfo.Page page) {
        this.pages.add(page);
    }
    
    public void addQueryVisualization(final MetaInfo.QueryVisualization qv) {
        this.queryVisualizations.add(qv);
    }
    
    public void addParametrizedQuery(final MetaInfo.Query query) {
        this.parametrizedQuery.add(query);
    }
    
    public String convertToJSON() throws JsonGenerationException, JsonMappingException, IOException {
        return new ObjectMapper().writeValueAsString((Object)this);
    }
    
    public MetaInfo.Dashboard getDashboard() {
        return this.dashboard;
    }
    
    public List<MetaInfo.Page> getPages() {
        return this.pages;
    }
    
    public List<MetaInfo.QueryVisualization> getQueryVisualizations() {
        return this.queryVisualizations;
    }
    
    @Override
    public String toString() {
        return "DASHBOARD : " + this.dashboard + " PAGES : " + this.pages + " QUERYVISUALIZATIONS: " + this.queryVisualizations + " PARAMETRIZEDQUERIES: " + this.parametrizedQuery;
    }
    
    public List<MetaInfo.Query> getParametrizedQuery() {
        return this.parametrizedQuery;
    }
}
