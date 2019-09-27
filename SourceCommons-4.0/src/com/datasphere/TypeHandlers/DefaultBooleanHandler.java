package com.datasphere.TypeHandlers;

import java.sql.*;

class DefaultBooleanHandler extends TypeHandler
{
    public DefaultBooleanHandler(final String targetType) {
        super(16, Boolean.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindDefaultBooleanHandler(stmt, bindIndex, value);
    }
}
