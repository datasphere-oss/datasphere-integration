package com.datasphere.web.api;

import com.datasphere.runtime.*;
import com.datasphere.uuid.*;
import com.datasphere.runtime.components.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.metaRepository.*;
import com.fasterxml.jackson.databind.node.*;
import java.util.*;

import com.datasphere.event.*;
import com.fasterxml.jackson.databind.*;

public class MetadataHelper
{
    private MetadataRepository _mdRpository;
    private QueryValidator _qv;
    
    public MetadataHelper(final QueryValidator qv) {
        this._mdRpository = MetadataRepository.getINSTANCE();
        this._qv = qv;
    }
    
    public Set<MetadataObjectIdentifier> getMetaObjectIdentifiersByType(final AuthToken token, final String type) throws MetaDataRepositoryException {
        final Set<MetadataObjectIdentifier> identifiers = new HashSet<MetadataObjectIdentifier>();
        final Set<MetaInfo.MetaObject> objects = (Set<MetaInfo.MetaObject>)this._mdRpository.getByEntityType(EntityType.forObject(type), token);
        for (final MetaInfo.MetaObject object : objects) {
            if (object.getMetaInfoStatus().isAdhoc()) {
                continue;
            }
            if (object.getMetaInfoStatus().isAnonymous() && object.type != EntityType.STREAM) {
                continue;
            }
            identifiers.add(new MetadataObjectIdentifier(object));
        }
        return identifiers;
    }
    
    public Map<String, ObjectNode[]> getEntitiesByTypes(final AuthToken token, final List<String> types) throws Exception {
        if (types == null) {
            throw new InvalidPropertiesFormatException("Entity Type can't be null");
        }
        final Map<String, ObjectNode[]> entities = new HashMap<String, ObjectNode[]>();
        final ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
        final ObjectNode node = jsonMapper.createObjectNode();
        for (final String type : types) {
            final EntityType entityType = EntityType.valueOf(type);
            final ObjectNode[] entitiesByType = ClientOperations.getInstance().CRUDWorker(this._qv, token, null, null, entityType, ClientOperations.CRUD.READ, node);
            entities.put(type, entitiesByType);
        }
        return entities;
    }
    
    public class MetadataObjectIdentifier
    {
        public String id;
        public String nsName;
        public String name;
        public String type;
        public Long ctime;
        public Map<String, Boolean> metaInfoStatus;
        
        public MetadataObjectIdentifier(final MetaInfo.MetaObject object) {
            this.id = object.getFQN();
            this.nsName = object.getNsName();
            this.name = object.getName();
            this.type = object.getType().name();
            this.ctime = object.getCtime();
            (this.metaInfoStatus = new HashMap<String, Boolean>()).put("isValid", object.getMetaInfoStatus().isValid());
            this.metaInfoStatus.put("isAnonymous", object.getMetaInfoStatus().isAnonymous());
            this.metaInfoStatus.put("isDropped", object.getMetaInfoStatus().isDropped());
            this.metaInfoStatus.put("isAdhoc", object.getMetaInfoStatus().isAdhoc());
        }
    }
}
