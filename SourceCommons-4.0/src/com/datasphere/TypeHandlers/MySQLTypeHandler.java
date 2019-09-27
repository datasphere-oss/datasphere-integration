package com.datasphere.TypeHandlers;

import com.datasphere.Tables.*;
import com.datasphere.source.lib.meta.*;

import org.joda.time.*;
import java.sql.*;

public class MySQLTypeHandler extends TargetTypeHandler
{
    private static final int YEAR_COL = 9100;
    private static final String YEAR_COL_NAME = "YEAR";
    public String targetType;
    
    public MySQLTypeHandler(final String targetType) {
        super(targetType);
        this.targetType = targetType;
    }
    
    @Override
    public int getSqlType(final int sqlType, final DatabaseColumn columnDetails) {
        if (sqlType == 91) {
            final DBWriterColumn colDetails = (DBWriterColumn)columnDetails;
            final String columnTypeName = colDetails.typeName();
            if (columnTypeName.equalsIgnoreCase("YEAR")) {
                return 9100;
            }
        }
        return sqlType;
    }
    
    @Override
    public void initialize() {
        super.initialize();
        this.addToMap(new LocalDateToYearHandler(this.targetType));
        this.addToMap(new LocalDateHandler(this.targetType));
    }
    
    class LocalDateToYearHandler extends LocalDateHandler
    {
        public LocalDateToYearHandler(final String targetType) {
            super(9100, targetType);
        }
        
        @Override
        public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
            final int year = ((LocalDate)value).getYear();
            stmt.setInt(bindIndex, year);
        }
        
        @Override
        public String toString(final Object value) {
            final int year = ((LocalDate)value).getYear();
            return "" + year;
        }
    }
    
    class LocalDateHandler extends TypeHandler
    {
        public LocalDateHandler(final String targetType) {
            super(91, LocalDate.class, targetType);
        }
        
        public LocalDateHandler(final int sqlType, final String targetType) {
            super(sqlType, LocalDate.class, targetType);
        }
        
        public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
            final Date date = new Date(((LocalDate)value).toDate().getTime());
            stmt.setDate(bindIndex, date);
        }
        
        @Override
        public String toString(final Object value) {
            return "'" + ((LocalDate)value).toString() + "'";
        }
    }
}
