package com.datasphere.metaRepository;

import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.uuid.UUID;

public class MetaDataGraph
{
    private static Logger logger;
    private MetaInfo.Namespace namespace;
    private Hashtable<UUID, MetaDataNode> nodes;
    
    public MetaDataGraph(final MetaInfo.Namespace namespace) {
        this.nodes = new Hashtable<UUID, MetaDataNode>();
        this.namespace = namespace;
        this.buildNodes();
    }
    
    public Set<MetaInfo.HDStore> getDownstreamHDStores(final MetaInfo.Source source) {
        final MetaDataNode sourceNode = this.getNode(source.uuid);
        if (!sourceNode.getMetaDataClass().equals(MetaInfo.Source.class)) {
            MetaDataGraph.logger.error((Object)("Expected MetaInfo.Source but found " + sourceNode.getMetaDataClass()));
            return null;
        }
        final Set<MetaInfo.HDStore> result = this.depthFirstSearch(sourceNode, MetaInfo.HDStore.class);
        return result;
    }
    
    public Collection<MetaInfo.MetaObject> getDownstreamComponents(final MetaInfo.MetaObject source) {
        final MetaDataNode sourceNode = this.getNode(source.uuid);
        if (!sourceNode.getMetaDataClass().equals(MetaInfo.Source.class)) {
            MetaDataGraph.logger.error((Object)("Expected MetaInfo.Source but found " + sourceNode.getMetaDataClass()));
            return null;
        }
        final Set<MetaInfo.MetaObject> result = this.depthFirstSearch(sourceNode, MetaInfo.MetaObject.class);
        return result;
    }
    
    private <T extends MetaInfo.MetaObject> Set<T> depthFirstSearch(final MetaDataNode node, final Class<T> targetClass) {
        final HashSet<T> result = new HashSet<T>();
        for (final MetaDataNode downstreamNode : node.getDownstreamNodes()) {
            if (targetClass.isAssignableFrom(downstreamNode.getMetaObject().getClass())) {
                result.add((T)downstreamNode.getMetaObject());
            }
            result.addAll(this.depthFirstSearch(downstreamNode, targetClass));
        }
        return result;
    }
    
    private MetaDataNode getNode(final UUID objectID) {
        if (this.nodes.containsKey(objectID)) {
            return this.nodes.get(objectID);
        }
        return this.getNode((MetaInfo.MetaObject)MDCache.getInstance().get(null, objectID, null, null, null, MDConstants.typeOfGet.BY_UUID));
    }
    
    private MetaDataNode getNode(final MetaInfo.MetaObject mo) {
        if (this.nodes.containsKey(mo.uuid)) {
            return this.nodes.get(mo.uuid);
        }
        if (mo.type == EntityType.SOURCE) {
            return this.buildNodeSource((MetaInfo.Source)mo);
        }
        if (mo.type == EntityType.CQ) {
            return this.buildNodeCQ((MetaInfo.CQ)mo);
        }
        if (mo.type == EntityType.TARGET) {
            return this.buildNodeTarget((MetaInfo.Target)mo);
        }
        if (mo.type == EntityType.WINDOW) {
            return this.buildNodeWindow((MetaInfo.Window)mo);
        }
        return this.buildNode(mo);
    }
    
    private void buildNodes() {
        this.nodes.clear();
        final List<MetaInfo.MetaObject> lObjects = (List<MetaInfo.MetaObject>)MDCache.getInstance().get(null, null, this.namespace.nsName, this.namespace.name, null, MDConstants.typeOfGet.BY_NAMESPACE);
        for (final MetaInfo.MetaObject mo : lObjects) {
            this.getNode(mo.uuid);
        }
    }
    
    private MetaDataNode buildNodeCQ(final MetaInfo.CQ metaObject) {
        final MetaDataNode node = new MetaDataNode();
        node.setMetaObject(metaObject);
        if (metaObject.plan != null) {
            for (final UUID ds : metaObject.plan.getDataSources()) {
                final MetaDataNode stream = this.getNode(ds);
                node.addUpstreamNode(stream);
                stream.addDownstreamNode(node);
            }
        }
        if (metaObject.stream != null) {
            final MetaDataNode stream2 = this.getNode(metaObject.stream);
            node.addDownstreamNode(stream2);
            stream2.addUpstreamNode(node);
        }
        this.nodes.put(metaObject.uuid, node);
        return node;
    }
    
    private MetaDataNode buildNodeSource(final MetaInfo.Source metaObject) {
        final MetaDataNode node = new MetaDataNode();
        node.setMetaObject(metaObject);
        if (metaObject.outputStream != null) {
            final MetaDataNode outputStream = this.getNode(metaObject.outputStream);
            node.addDownstreamNode(outputStream);
            outputStream.addUpstreamNode(node);
        }
        this.nodes.put(metaObject.uuid, node);
        return node;
    }
    
    private MetaDataNode buildNodeTarget(final MetaInfo.Target metaObject) {
        final MetaDataNode node = new MetaDataNode();
        node.setMetaObject(metaObject);
        if (metaObject.inputStream != null) {
            final MetaDataNode stream = this.getNode(metaObject.inputStream);
            node.addUpstreamNode(stream);
            stream.addDownstreamNode(node);
        }
        this.nodes.put(metaObject.uuid, node);
        return node;
    }
    
    private MetaDataNode buildNodeWindow(final MetaInfo.Window metaObject) {
        final MetaDataNode node = new MetaDataNode();
        node.setMetaObject(metaObject);
        if (metaObject.stream != null) {
            final MetaDataNode stream = this.getNode(metaObject.stream);
            node.addUpstreamNode(stream);
            stream.addDownstreamNode(node);
        }
        this.nodes.put(metaObject.uuid, node);
        return node;
    }
    
    private MetaDataNode buildNode(final MetaInfo.MetaObject metaObject) {
        final MetaDataNode node = new MetaDataNode();
        node.setMetaObject(metaObject);
        this.nodes.put(metaObject.uuid, node);
        return node;
    }
    
    static {
        MetaDataGraph.logger = Logger.getLogger((Class)MetaDataGraph.class);
    }
    
    private class MetaDataNode
    {
        private MetaInfo.MetaObject metaObject;
        private final Set<MetaDataNode> downstreamNodes;
        private final Set<MetaDataNode> upstreamNodes;
        
        public MetaDataNode() {
            this.metaObject = null;
            this.downstreamNodes = new HashSet<MetaDataNode>();
            this.upstreamNodes = new HashSet<MetaDataNode>();
        }
        
        public void setMetaObject(final MetaInfo.MetaObject metaObject) {
            this.metaObject = metaObject;
        }
        
        public MetaInfo.MetaObject getMetaObject() {
            return this.metaObject;
        }
        
        public Set<MetaDataNode> getDownstreamNodes() {
            return this.downstreamNodes;
        }
        
        public void addUpstreamNode(final MetaDataNode node) {
            this.upstreamNodes.add(node);
        }
        
        public void addDownstreamNode(final MetaDataNode node) {
            this.downstreamNodes.add(node);
        }
        
        public Object getMetaDataClass() {
            return this.metaObject.getClass();
        }
    }
}
