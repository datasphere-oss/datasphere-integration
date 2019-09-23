package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "persistence-unit-metadata", propOrder = { "description", "xmlMappingMetadataComplete", "excludeDefaultMappings", "persistenceUnitDefaults" })
public class PersistenceUnitMetadata
{
    protected String description;
    @XmlElement(name = "xml-mapping-metadata-complete")
    protected EmptyType xmlMappingMetadataComplete;
    @XmlElement(name = "exclude-default-mappings")
    protected EmptyType excludeDefaultMappings;
    @XmlElement(name = "persistence-unit-defaults")
    protected PersistenceUnitDefaults persistenceUnitDefaults;
    
    public String getDescription() {
        return this.description;
    }
    
    public void setDescription(final String value) {
        this.description = value;
    }
    
    public EmptyType getXmlMappingMetadataComplete() {
        return this.xmlMappingMetadataComplete;
    }
    
    public void setXmlMappingMetadataComplete(final EmptyType value) {
        this.xmlMappingMetadataComplete = value;
    }
    
    public EmptyType getExcludeDefaultMappings() {
        return this.excludeDefaultMappings;
    }
    
    public void setExcludeDefaultMappings(final EmptyType value) {
        this.excludeDefaultMappings = value;
    }
    
    public PersistenceUnitDefaults getPersistenceUnitDefaults() {
        return this.persistenceUnitDefaults;
    }
    
    public void setPersistenceUnitDefaults(final PersistenceUnitDefaults value) {
        this.persistenceUnitDefaults = value;
    }
}
