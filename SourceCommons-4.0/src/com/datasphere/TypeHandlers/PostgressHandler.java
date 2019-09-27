package com.datasphere.TypeHandlers;

import java.math.*;
import java.sql.*;

public class PostgressHandler extends TargetTypeHandler
{
    public String targetType;
    
    public PostgressHandler(final String targetType) {
        super(targetType);
        this.targetType = targetType;
    }
    
    @Override
    public void initialize() {
        super.initialize();
        this.addToMap(new StringToNumberHandler(this.targetType));
        this.addToMap(new StringToIntegerHandler(this.targetType));
        this.addToMap(new StringToBigIntHandler(this.targetType));
        this.addToMap(new StringToDoubleHandler(this.targetType));
        this.addToMap(new StringToBigIntHandler(this.targetType));
        this.addToMap(new StringToRealHandler(this.targetType));
        this.addToMap(new StringToSmallIntHandler(this.targetType));
    }
    
    class StringToNumberHandler extends TypeHandler
    {
        public StringToNumberHandler(final String targetType) {
            super(2, String.class, targetType);
        }
        
        public StringToNumberHandler(final int sqlType, final String targetType) {
            super(sqlType, String.class, targetType);
        }
        
        public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
            final BigDecimal bd = new BigDecimal((String)value);
            stmt.setBigDecimal(bindIndex, bd);
        }
        
        @Override
        public String toString(final Object value) {
            return (String)value;
        }
    }
    
    class StringToIntegerHandler extends StringToNumberHandler
    {
        public StringToIntegerHandler(final String targetType) {
            super(4, targetType);
        }
    }
    
    class StringToFloatHandler extends StringToNumberHandler
    {
        public StringToFloatHandler(final String targetType) {
            super(6, targetType);
        }
    }
    
    class StringToDoubleHandler extends StringToNumberHandler
    {
        public StringToDoubleHandler(final String targetType) {
            super(8, targetType);
        }
    }
    
    class StringToBigIntHandler extends StringToNumberHandler
    {
        public StringToBigIntHandler(final String targetType) {
            super(-5, targetType);
        }
    }
    
    class StringToRealHandler extends StringToNumberHandler
    {
        public StringToRealHandler(final String targetType) {
            super(7, targetType);
        }
    }
    
    class StringToSmallIntHandler extends StringToNumberHandler
    {
        public StringToSmallIntHandler(final String targetType) {
            super(5, targetType);
        }
    }
}
