package com.datasphere.security;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.datasphere.exception.SecurityException;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class ObjectPermission implements Serializable
{
    private static final long serialVersionUID = -3755014711584508191L;
    public static final String SYSTEM_DOMAIN = "system";
    public static final String WILDCARD_TOKEN = "*";
    protected static final String PART_DIVIDER_TOKEN = ":";
    protected static final String SUBPART_DIVIDER_TOKEN = ",";
    private Set<String> domains;
    private Set<Action> actions;
    private Set<ObjectType> objectTypes;
    private Set<String> objectNames;
    private PermissionType type;
    private String stringVersion;
    
    public ObjectPermission() {
        this.stringVersion = null;
    }
    
    public ObjectPermission(final String domain, final Action action, final ObjectType objectType, final String objectName, final PermissionType type) throws SecurityException {
        this.stringVersion = null;
        if (domain == null) {
            this.domains = null;
        }
        else {
            (this.domains = new HashSet<String>()).add(domain);
        }
        if (action == null) {
            this.actions = null;
        }
        else {
            (this.actions = new HashSet<Action>()).add(action);
        }
        if (objectType == null) {
            this.objectTypes = null;
        }
        else {
            (this.objectTypes = new HashSet<ObjectType>()).add(objectType);
        }
        if (objectName == null) {
            this.objectNames = null;
        }
        else {
            (this.objectNames = new HashSet<String>()).add(objectName);
        }
        this.type = type;
    }
    
    public ObjectPermission(final String domain, final Action action, final ObjectType objectType, final String objectName) throws SecurityException {
        this(domain, action, objectType, objectName, PermissionType.allow);
    }
    
    public ObjectPermission(final Set<String> domains, final Set<Action> actions, final Set<ObjectType> objectTypes, final Set<String> objectNames, final PermissionType type) throws SecurityException {
        this.stringVersion = null;
        this.domains = domains;
        this.actions = actions;
        this.objectTypes = objectTypes;
        this.objectNames = objectNames;
        this.type = type;
    }
    
    public ObjectPermission(final Set<String> domains, final Set<Action> actions, final Set<ObjectType> objectTypes, final Set<String> objectNames) throws SecurityException {
        this(domains, actions, objectTypes, objectNames, PermissionType.allow);
    }
    
    public ObjectPermission(final String permissionString) {
        this.stringVersion = null;
        this.type = PermissionType.allow;
        this.setPermission(permissionString);
    }
    
    public ObjectPermission(final ObjectPermission other) throws SecurityException {
        this(other.domains, other.actions, other.objectTypes, other.objectNames, other.type);
    }
    
    public ObjectPermission(final ObjectPermission other, final PermissionType type) throws SecurityException {
        this(other.domains, other.actions, other.objectTypes, other.objectNames, type);
    }
    
    public Set<String> getDomains() {
        return this.domains;
    }
    
    public void setDomains(final Set<String> domains) {
        this.domains = domains;
    }
    
    public Set<Action> getActions() {
        return this.actions;
    }
    
    public void setActions(final Set<Action> actions) {
        this.actions = actions;
    }
    
    public Set<ObjectType> getObjectTypes() {
        return this.objectTypes;
    }
    
    public void setObjectTypes(final Set<ObjectType> objectTypes) {
        this.objectTypes = objectTypes;
    }
    
    public Set<String> getObjectNames() {
        return this.objectNames;
    }
    
    public void setObjectNames(final Set<String> objectNames) {
        this.objectNames = objectNames;
    }
    
    public PermissionType getType() {
        return this.type;
    }
    
    public void setType(final PermissionType type) {
        this.type = type;
    }
    
    @Override
    public String toString() {
        if (this.stringVersion == null) {
            final StringBuilder sb = new StringBuilder();
            boolean first = true;
            if (this.domains == null) {
                sb.append("*");
            }
            else {
                for (final String domain : this.domains) {
                    if (!first) {
                        sb.append(",");
                    }
                    sb.append(domain);
                    first = false;
                }
            }
            sb.append(":");
            first = true;
            if (this.actions == null) {
                sb.append("*");
            }
            else {
                for (final Action action : this.actions) {
                    if (!first) {
                        sb.append(",");
                    }
                    sb.append(action);
                    first = false;
                }
            }
            sb.append(":");
            first = true;
            if (this.objectTypes == null) {
                sb.append("*");
            }
            else {
                for (final ObjectType objectType : this.objectTypes) {
                    if (!first) {
                        sb.append(",");
                    }
                    sb.append(objectType);
                    first = false;
                }
            }
            sb.append(":");
            first = true;
            if (this.objectNames == null) {
                sb.append("*");
            }
            else {
                for (final String objectName : this.objectNames) {
                    if (!first) {
                        sb.append(",");
                    }
                    sb.append(objectName);
                    first = false;
                }
            }
            if (this.type == PermissionType.disallow) {
                sb.append(":");
                sb.append(this.type);
            }
            this.stringVersion = sb.toString();
        }
        return this.stringVersion;
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof ObjectPermission)) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        final ObjectPermission other = (ObjectPermission)obj;
        return this.toString().equalsIgnoreCase(other.toString()) || (this.compareSets(this.domains, other.domains) && this.compareSets(this.actions, other.actions) && this.compareSets(this.objectTypes, other.objectTypes) && this.compareSets(this.objectNames, other.objectNames) && this.type == other.type);
    }
    
    public boolean compareSets(final Set<?> one, final Set<?> two) {
        return one == two || (one != null && two != null && one.equals(two));
    }
    
    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
    
    public boolean implies(final ObjectPermission p) {
        if (this.domains != null) {
            if (p.domains == null) {
                return false;
            }
            for (final String domain : p.domains) {
                boolean contains = false;
                for (final String d1 : this.domains) {
                    if (d1.equalsIgnoreCase(domain)) {
                        contains = true;
                    }
                }
                if (contains) {
                    continue;
                }
                return false;
            }
        }
        if (this.actions != null) {
            if (p.actions == null) {
                return false;
            }
            for (final Action action : p.actions) {
                if (!this.actions.contains(action)) {
                    return false;
                }
            }
        }
        if (this.objectTypes != null) {
            if (p.objectTypes == null) {
                return false;
            }
            for (final ObjectType objectType : p.objectTypes) {
                if (!this.objectTypes.contains(objectType)) {
                    return false;
                }
            }
        }
        if (this.objectNames != null) {
            if (p.objectNames == null) {
                return false;
            }
            for (final String objectName : p.objectNames) {
                boolean contains = false;
                for (final String on1 : this.objectNames) {
                    if (on1.equalsIgnoreCase(objectName)) {
                        contains = true;
                    }
                }
                if (contains) {
                    continue;
                }
                return false;
            }
        }
        return p.type == this.type;
    }
    
    public String getPermission() {
        return this.toString();
    }
    
    public void setPermission(String permission) {
        if (permission == null || permission.trim().length() == 0) {
            throw new IllegalArgumentException("Permission string cannot be null or empty.");
        }
        permission = permission.trim();
        final String[] parts = permission.split(":");
        for (int i = 0; i < parts.length; ++i) {
            final String part = parts[i].trim();
            final String[] subparts = part.split(",");
            for (int j = 0; j < subparts.length; ++j) {
                final String subpart = subparts[j].trim();
                switch (i) {
                    case 0: {
                        if ("*".equals(subpart)) {
                            this.domains = null;
                            break;
                        }
                        if (this.domains == null) {
                            this.domains = new HashSet<String>();
                        }
                        this.domains.add(subpart);
                        break;
                    }
                    case 1: {
                        if ("*".equals(subpart)) {
                            this.actions = null;
                            break;
                        }
                        if (this.actions == null) {
                            this.actions = new HashSet<Action>();
                        }
                        this.actions.add(Action.valueOf(subpart.toLowerCase()));
                        break;
                    }
                    case 2: {
                        if ("*".equals(subpart)) {
                            this.objectTypes = null;
                            break;
                        }
                        if (this.objectTypes == null) {
                            this.objectTypes = new HashSet<ObjectType>();
                        }
                        this.objectTypes.add(ObjectType.valueOf(subpart.toLowerCase()));
                        break;
                    }
                    case 3: {
                        if ("*".equals(subpart)) {
                            this.objectNames = null;
                            break;
                        }
                        if (this.objectNames == null) {
                            this.objectNames = new HashSet<String>();
                        }
                        this.objectNames.add(subpart);
                        break;
                    }
                    case 4: {
                        this.type = PermissionType.valueOf(subpart.toLowerCase());
                        break;
                    }
                    default: {
                        throw new IllegalArgumentException("Invalid permission format: too many parts");
                    }
                }
            }
        }
    }
    
    public ObjectPermission createReverse() {
        ObjectPermission reverse = null;
        if (this.type == PermissionType.allow) {
            reverse = new ObjectPermission(this.domains, this.actions, this.objectTypes, this.objectNames, PermissionType.disallow);
        }
        else {
            reverse = new ObjectPermission(this.domains, this.actions, this.objectTypes, this.objectNames, PermissionType.allow);
        }
        return reverse;
    }
    
    @JsonIgnore
    public boolean isDisallowedType() {
        return this.type == PermissionType.disallow;
    }
    
    public enum Action
    {
        create, 
        drop, 
        update, 
        read, 
        resume, 
        grant, 
        select, 
        start, 
        deploy, 
        undeploy, 
        stop, 
        quiesce, 
        all, 
        memory;
    }
    
    public enum ObjectType
    {
        cluster, 
        node, 
        application, 
        propertytemplate, 
        type, 
        stream, 
        window, 
        cq, 
        query, 
        source, 
        target, 
        propertyset, 
        propertyvariable, 
        hdstore, 
        cache, 
        wi, 
        alertsubscriber, 
        user, 
        role, 
        permission, 
        server, 
        deploymentgroup, 
        initializer, 
        visualization, 
        subscription, 
        unknown, 
        namespace, 
        flow, 
        stream_generator, 
        dashboard, 
        page, 
        queryvisualization, 
        dashboard_ui, 
        sourcepreview_ui, 
        apps_ui, 
        admin_ui, 
        monitor_ui;
    }
    
    public enum PermissionType
    {
        allow, 
        disallow;
    }
}
