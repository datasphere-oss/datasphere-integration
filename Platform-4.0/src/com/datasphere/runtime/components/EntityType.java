package com.datasphere.runtime.components;

public enum EntityType
{
    UNKNOWN(0), 
    APPLICATION(1), 
    STREAM(2), 
    WINDOW(3), 
    TYPE(4), 
    CQ(5), 
    SOURCE(6), 
    TARGET(7), 
    FLOW(8), 
    PROPERTYSET(9), 
    HDSTORE(10), 
    PROPERTYTEMPLATE(11), 
    CACHE(12), 
    WI(13), 
    ALERTSUBSCRIBER(14), 
    SERVER(15), 
    USER(16), 
    ROLE(17), 
    INITIALIZER(18), 
    DG(19), 
    VISUALIZATION(20), 
    NAMESPACE(22), 
    EXCEPTIONHANDLER(23), 
    STREAM_GENERATOR(24), 
    SORTER(25), 
    WASTOREVIEW(26), 
    AGENT(27), 
    DASHBOARD(28), 
    PAGE(29), 
    QUERYVISUALIZATION(30), 
    QUERY(32), 
    POSITION(33), 
    PROPERTYVARIABLE(34);
    
    private final int val;
    public static EntityType[] orderOfRecompile;
    
    private EntityType(final int v) {
        this.val = v;
    }
    
    public int getValue() {
        return this.val;
    }
    
    public boolean canBePartOfDashboard() {
        switch (this) {
            case QUERYVISUALIZATION:
            case PAGE: {
                return true;
            }
            default: {
                return false;
            }
        }
    }
    
    public boolean canBePartOfApp() {
        switch (this) {
            case TYPE:
            case STREAM:
            case STREAM_GENERATOR:
            case WINDOW:
            case CACHE:
            case CQ:
            case TARGET:
            case HDSTORE:
            case SOURCE:
            case FLOW:
            case SORTER:
            case WASTOREVIEW:
            case VISUALIZATION:
            case EXCEPTIONHANDLER:
            case PROPERTYSET: {
                return true;
            }
            default: {
                return false;
            }
        }
    }
    
    public boolean isAccessible() {
        switch (this) {
            case CACHE:
            case HDSTORE:
            case SOURCE: {
                return true;
            }
            default: {
                return false;
            }
        }
    }
    
    public boolean canBePartOfFlow() {
        switch (this) {
            case TYPE:
            case STREAM:
            case STREAM_GENERATOR:
            case WINDOW:
            case CACHE:
            case CQ:
            case TARGET:
            case HDSTORE:
            case SOURCE:
            case FLOW:
            case SORTER:
            case WASTOREVIEW:
            case PROPERTYSET: {
                return true;
            }
            default: {
                return false;
            }
        }
    }
    
    public boolean isGlobal() {
        switch (this) {
            case NAMESPACE:
            case PROPERTYTEMPLATE:
            case ALERTSUBSCRIBER:
            case SERVER:
            case AGENT:
            case USER:
            case INITIALIZER:
            case DG: {
                return true;
            }
            default: {
                return false;
            }
        }
    }
    
    public boolean isSystem() {
        switch (this) {
            case INITIALIZER:
            case UNKNOWN: {
                return true;
            }
            default: {
                return false;
            }
        }
    }
    
    public boolean isNotVersionable() {
        switch (this) {
            case ALERTSUBSCRIBER:
            case SERVER:
            case USER:
            case INITIALIZER:
            case DG:
            case ROLE:
            case QUERY: {
                return true;
            }
            default: {
                return false;
            }
        }
    }
    
    public boolean isStoreable() {
        switch (this) {
            case ALERTSUBSCRIBER:
            case SERVER:
            case AGENT:
            case INITIALIZER:
            case UNKNOWN:
            case WI: {
                return false;
            }
            default: {
                return true;
            }
        }
    }
    
    public static boolean isGlobal(final EntityType val) {
        return val.isGlobal();
    }
    
    public static EntityType createFromInt(final int n) {
        for (final EntityType et : values()) {
            if (et.getValue() == n) {
                return et;
            }
        }
        return EntityType.UNKNOWN;
    }
    
    public static EntityType forObject(final Object obj) {
        if (obj != null) {
            if (obj instanceof Integer) {
                final int v = (int)obj;
                for (final EntityType et : values()) {
                    if (et.val == v) {
                        return et;
                    }
                }
            }
            else if (obj instanceof String) {
                final String key = obj.toString();
                for (final EntityType et : values()) {
                    if (et.name().equalsIgnoreCase(key)) {
                        return et;
                    }
                }
            }
        }
        return EntityType.UNKNOWN;
    }
    
    public boolean isFlowComponent() {
        switch (this) {
            case STREAM:
            case STREAM_GENERATOR:
            case WINDOW:
            case CACHE:
            case CQ:
            case TARGET:
            case HDSTORE:
            case SOURCE:
            case FLOW:
            case SORTER:
            case WASTOREVIEW:
            case APPLICATION: {
                return true;
            }
            default: {
                return false;
            }
        }
    }
    
    static {
        EntityType.orderOfRecompile = new EntityType[] { EntityType.TYPE, EntityType.STREAM, EntityType.STREAM_GENERATOR, EntityType.CACHE, EntityType.HDSTORE, EntityType.WINDOW, EntityType.SOURCE, EntityType.CQ, EntityType.TARGET, EntityType.FLOW, EntityType.VISUALIZATION, EntityType.APPLICATION };
    }
}
