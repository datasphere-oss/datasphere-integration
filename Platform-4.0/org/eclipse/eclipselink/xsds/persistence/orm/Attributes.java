package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "attributes", propOrder = { "description", "id", "embeddedId", "basic", "basicCollection", "basicMap", "version", "manyToOne", "oneToMany", "oneToOne", "variableOneToOne", "manyToMany", "elementCollection", "embedded", "transformation", "_transient", "structure", "array" })
public class Attributes
{
    protected String description;
    protected List<Id> id;
    @XmlElement(name = "embedded-id")
    protected EmbeddedId embeddedId;
    protected List<Basic> basic;
    @XmlElement(name = "basic-collection")
    protected List<BasicCollection> basicCollection;
    @XmlElement(name = "basic-map")
    protected List<BasicMap> basicMap;
    protected List<Version> version;
    @XmlElement(name = "many-to-one")
    protected List<ManyToOne> manyToOne;
    @XmlElement(name = "one-to-many")
    protected List<OneToMany> oneToMany;
    @XmlElement(name = "one-to-one")
    protected List<OneToOne> oneToOne;
    @XmlElement(name = "variable-one-to-one")
    protected List<VariableOneToOne> variableOneToOne;
    @XmlElement(name = "many-to-many")
    protected List<ManyToMany> manyToMany;
    @XmlElement(name = "element-collection")
    protected List<ElementCollection> elementCollection;
    protected List<Embedded> embedded;
    protected List<Transformation> transformation;
    @XmlElement(name = "transient")
    protected List<Transient> _transient;
    protected List<Structure> structure;
    protected List<Array> array;
    
    public String getDescription() {
        return this.description;
    }
    
    public void setDescription(final String value) {
        this.description = value;
    }
    
    public List<Id> getId() {
        if (this.id == null) {
            this.id = new ArrayList<Id>();
        }
        return this.id;
    }
    
    public EmbeddedId getEmbeddedId() {
        return this.embeddedId;
    }
    
    public void setEmbeddedId(final EmbeddedId value) {
        this.embeddedId = value;
    }
    
    public List<Basic> getBasic() {
        if (this.basic == null) {
            this.basic = new ArrayList<Basic>();
        }
        return this.basic;
    }
    
    public List<BasicCollection> getBasicCollection() {
        if (this.basicCollection == null) {
            this.basicCollection = new ArrayList<BasicCollection>();
        }
        return this.basicCollection;
    }
    
    public List<BasicMap> getBasicMap() {
        if (this.basicMap == null) {
            this.basicMap = new ArrayList<BasicMap>();
        }
        return this.basicMap;
    }
    
    public List<Version> getVersion() {
        if (this.version == null) {
            this.version = new ArrayList<Version>();
        }
        return this.version;
    }
    
    public List<ManyToOne> getManyToOne() {
        if (this.manyToOne == null) {
            this.manyToOne = new ArrayList<ManyToOne>();
        }
        return this.manyToOne;
    }
    
    public List<OneToMany> getOneToMany() {
        if (this.oneToMany == null) {
            this.oneToMany = new ArrayList<OneToMany>();
        }
        return this.oneToMany;
    }
    
    public List<OneToOne> getOneToOne() {
        if (this.oneToOne == null) {
            this.oneToOne = new ArrayList<OneToOne>();
        }
        return this.oneToOne;
    }
    
    public List<VariableOneToOne> getVariableOneToOne() {
        if (this.variableOneToOne == null) {
            this.variableOneToOne = new ArrayList<VariableOneToOne>();
        }
        return this.variableOneToOne;
    }
    
    public List<ManyToMany> getManyToMany() {
        if (this.manyToMany == null) {
            this.manyToMany = new ArrayList<ManyToMany>();
        }
        return this.manyToMany;
    }
    
    public List<ElementCollection> getElementCollection() {
        if (this.elementCollection == null) {
            this.elementCollection = new ArrayList<ElementCollection>();
        }
        return this.elementCollection;
    }
    
    public List<Embedded> getEmbedded() {
        if (this.embedded == null) {
            this.embedded = new ArrayList<Embedded>();
        }
        return this.embedded;
    }
    
    public List<Transformation> getTransformation() {
        if (this.transformation == null) {
            this.transformation = new ArrayList<Transformation>();
        }
        return this.transformation;
    }
    
    public List<Transient> getTransient() {
        if (this._transient == null) {
            this._transient = new ArrayList<Transient>();
        }
        return this._transient;
    }
    
    public List<Structure> getStructure() {
        if (this.structure == null) {
            this.structure = new ArrayList<Structure>();
        }
        return this.structure;
    }
    
    public List<Array> getArray() {
        if (this.array == null) {
            this.array = new ArrayList<Array>();
        }
        return this.array;
    }
}
