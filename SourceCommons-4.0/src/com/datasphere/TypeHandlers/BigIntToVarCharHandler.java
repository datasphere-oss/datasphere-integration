package com.datasphere.TypeHandlers;

import java.math.*;
import java.sql.*;

class BigIntToVarCharHandler extends TypeHandler
{
    public BigIntToVarCharHandler(final String targetType) {
        super(12, BigInteger.class, targetType);
    }
    
    public BigIntToVarCharHandler(final int sqlType, final String targetType) {
        super(sqlType, BigInteger.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindBigIntToVarCharHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        return "" + value.toString();
    }
}
