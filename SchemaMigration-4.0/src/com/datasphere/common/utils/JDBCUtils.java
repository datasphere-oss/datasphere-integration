package com.datasphere.common.utils;

import java.sql.Statement;
import java.util.Collection;

import com.datasphere.db.entity.Column;

public class JDBCUtils {

	public static void safeClose(Statement t) {
		if(t != null) {
			try {
				t.close();
			} catch(Throwable ex) {}
		}
	}
	
	public static String csColumnNames(Collection<Column> columns) {
		StringBuilder sb = new StringBuilder();
		for(Column col: columns) {
			sb.append(col.getName());
			sb.append(",");
		}
		sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}
}
