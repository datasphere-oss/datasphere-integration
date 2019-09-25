package com.datasphere.TypeHandler;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BigDecimalHandler extends TypeHandler
{
    public BigDecimalHandler() {
        super(2, BigDecimal.class);
    }
    
    public BigDecimalHandler(final int sqlType) {
        super(sqlType, BigDecimal.class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        try {
        		if(value == null) {
        			stmt.setNull(bindIndex, java.sql.Types.BIGINT);
        		}else {
        			if(value instanceof BigDecimal) {
        				stmt.setBigDecimal(bindIndex, (BigDecimal)value);
        			}else {
        				stmt.setBigDecimal(bindIndex, new BigDecimal((String)value));
        			}
        		}
        }
        catch (ClassCastException exp) {
            throw exp;
        }
    }

}
