package com.datasphere.parser;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.TStatementList;
import gudusoft.gsqlparser.TSyntaxError;

public class SqlParser {
	Logger logger = LoggerFactory.getLogger(SqlParser.class);
	private EDbVendor sourceType;
	private EDbVendor targetType;
	private String newTableName;
	private String operationSubName;

	public SqlParser(EDbVendor sourceType, EDbVendor targetType, String newTableName) {
		this.sourceType = sourceType;
		this.targetType = targetType;
		this.newTableName = newTableName;
	}
	
	/**
	 * DDL 解析
	 */
	public List<String> ddlParse(String sql) {
		List<String> list = new ArrayList<>();
//		if(this.sourceType == EDbVendor.dbvmysql && this.targetType == EDbVendor.dbvpostgresql) {
//			try {
//				return MySQL2PostgreSQL.parser(sql);
//			} catch (JSQLParserException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
		String mSql = sql.replaceAll("/\\*{1,2}[\\s\\S]*?\\*/", "");

		String newsql = "";
		List<Integer> indexs = new ArrayList<>();
		if (mSql.toUpperCase().contains("ALGORITHM") || mSql.toUpperCase().contains("LOCK")) {
			String[] sqlStmt = mSql.split(",");
			for (int i = 0; i < sqlStmt.length; i++) {
				String stmt = sqlStmt[i];
				if ("ALGORITHM".equalsIgnoreCase(stmt.split("=")[0].trim())) {
					indexs.add(i);
				}
				if ("LOCK".equalsIgnoreCase(stmt.split("=")[0].trim())) {
					indexs.add(i);
				}
			}
			if (indexs.size() > 0) {
				for (int j = 0; j < sqlStmt.length; j++) {
					if (!indexs.contains(j)) {
						if (newsql.length() > 0) {
							newsql += ",";
						}
						newsql += sqlStmt[j];
					}
				}
			}
		}
		if (newsql.length() == 0) {
			newsql = mSql;
		}

		TGSqlParser parser = new TGSqlParser(this.sourceType);
		String query = reBuildSql(parser, newsql);
		parser.sqltext = query;
		parser.parse();

		if (parser.getErrorCount() == 0) {
			TStatementList stmts = parser.sqlstatements;
			for (int i = 0; i < stmts.size(); i++) {
				TCustomSqlStatement stmt = stmts.get(i);
				AnalyzeStmt analyzeStmt = new AnalyzeStmt(this.targetType, this.newTableName);
				List<String> lst = analyzeStmt.analyzeStmt(stmt);
				if (lst != null) {
					list.addAll(lst);
				}
			}
		} else {
			System.out.println(parser.getErrormessage());
		}
		return list;
	}

	private String reBuildSql(TGSqlParser parser, String sql) {
		String newsql = "";
		parser.sqltext = sql;
		parser.parse();
		if (parser.getSyntaxErrors().size() > 0) {
			TSyntaxError error = parser.getSyntaxErrors().get(0);
			long columnNo = error.columnNo;
			newsql = sql.substring(0, (int) columnNo - 1);
			if ("STORAGE".equalsIgnoreCase(error.tokentext)) {
				newsql += sql.substring((int) columnNo + "STORAGE".length() - 1);
			}
			if ("DISK".equalsIgnoreCase(error.tokentext)) {
				newsql += sql.substring((int) columnNo + "DISK".length() - 1);
			}
			if ("MEMORY".equalsIgnoreCase(error.tokentext)) {
				newsql += sql.substring((int) columnNo + "MEMORY".length() - 1);
			}
			parser.sqltext = newsql;
			parser.parse();
			if (parser.getErrorCount() > 0) {
				newsql = reBuildSql(parser, newsql);
			}
		} else {
			return sql;
		}
		return newsql;
	}

	public static void main(String[] args) {
		String sql = "alter table test2 add column email varchar(20) not null after addr";
		sql = "ALTER TABLE table_name MODIFY COLUMN email MEDIUMINT;";
		sql = "Alter table tabname add primary key(email)";
		sql = "Alter table tabname drop primary key(col)";
		sql = "ALTER TABLE buffer_test.test1 DROP COLUMN Column1,DROP COLUMN Column2";
		sql = "ALTER TABLE tbl_name ADD INDEX indexname (id,name)";
		// sql = "ALTER TABLE GOODS ADD CONSTRAINT UNIQUE_GOODS_SID UNIQUE(SID);";
		// sql = "ALTER TABLE tbl_name ADD PRIMARY KEY (column), ALGORITHM=INPLACE,
		// LOCK=NONE;";
		// sql = "ALTER TABLE tbl_name DROP PRIMARY KEY, ADD PRIMARY KEY (column),
		// ALGORITHM=INPLACE, LOCK=NONE;";
		// sql = "ALTER TABLE TBL1 ADD CONSTRAINT FK_NAME FOREIGN KEY INDEX (COL1)
		// REFERENCES TBL2(COL2) ON UPDATE CASCADE ON DELETE CASCADE;";
		// sql = "ALTER TABLE ORDERS ADD FOREIGN KEY(GOODS_ID) REFERENCES GOODS(SID) ON
		// UPDATE CASCADE ON DELETE CASCADE";
		// sql = "ALTER TABLE aaa ADD FOREIGN KEY (aa) REFERENCES test(bb)";
		// sql = "ALTER TABLE table DROP FOREIGN KEY constraint, DROP INDEX index;";
		// sql = "ALTER TABLE t1 ADD COLUMN (c2 INT GENERATED ALWAYS AS (c1 + 1)
		// VIRTUAL), ALGORITHM=INPLACE, LOCK=NONE;";//不支持
		// sql = "ALTER TABLE tbl_name CHANGE COLUMN c1 c1 VARCHAR(255),
		// ALGORITHM=INPLACE, LOCK=NONE;";

		// sql = "ALTER TABLE tbl_name MODIFY COLUMN col_name column_definition FIRST,
		// ALGORITHM=INPLACE, LOCK=NONE;";
		// sql = "ALTER TABLE tbl_name DROP INDEX i1, ADD INDEX i1(key_part,key_part2)
		// USING BTREE, ALGORITHM=INPLACE;";
		// sql = "ALTER TABLE t1 MODIFY COLUMN c1 ENUM('a', 'b', 'c', 'd'),
		// ALGORITHM=INPLACE, LOCK=NONE;";
		// sql = "CREATE TABLE `scripts` (\n" +
		// " `scriptid` bigint(20) unsigned NOT NULL,\n" +
		// " `name` varchar(255) NOT NULL DEFAULT '',\n" +
		// " `command` varchar(255) NOT NULL DEFAULT '',\n" +
		// " `host_access` int(11) NOT NULL DEFAULT '2',\n" +
		// " `usrgrpid` bigint(20) unsigned DEFAULT NULL,\n" +
		// " `groupid` bigint(20) unsigned DEFAULT NULL,\n" +
		// " `description` text NOT NULL,\n" +
		// " `confirmation` varchar(255) NOT NULL DEFAULT '',\n" +
		// " `type` int(11) NOT NULL DEFAULT '0',\n" +
		// " `execute_on` int(11) NOT NULL DEFAULT '1',\n" +
		// " PRIMARY KEY (`scriptid`),\n" +
		// " KEY `scripts_1` (`usrgrpid`),\n" +
		// " KEY `scripts_2` (`groupid`),\n" +
		// " CONSTRAINT `c_scripts_1` FOREIGN KEY (`usrgrpid`) REFERENCES `usrgrp`
		// (`usrgrpid`) ON DELETE CASCADE ON UPDATE CASCADE,\n" +
		// " CONSTRAINT `c_scripts_2` FOREIGN KEY (`groupid`) REFERENCES `groups`
		// (`groupid`)\n" +
		// ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
		// sql = "CREATE TABLE `users_groups` (\n" +
		// " `id` bigint(20) unsigned NOT NULL,\n" +
		// " `usrgrpid` bigint(20) unsigned NOT NULL,\n" +
		// " PRIMARY KEY (`id`),\n" +
		// " UNIQUE KEY `users_groups_1` (`usrgrpid`),\n" +
		// " CONSTRAINT `c_users_groups_1` FOREIGN KEY (`usrgrpid`) REFERENCES `usrgrp`
		// (`usrgrpid`) ON DELETE CASCADE ON UPDATE CASCADE\n" +
		// ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";
		// sql = "CREATE TABLE `usrgrp` (\n" +
		// " `usrgrpid` bigint(20) unsigned NOT NULL,\n" +
		// " `name` varchar(64) NOT NULL DEFAULT '',\n" +
		// " `gui_access` int(11) NOT NULL DEFAULT '0',\n" +
		// " `users_status` int(11) NOT NULL DEFAULT '0',\n" +
		// " `debug_mode` int(11) NOT NULL DEFAULT '0',\n" +
		// " PRIMARY KEY (`usrgrpid`),\n" +
		// " KEY `usrgrp_1` (`name`)\n" +
		// ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";
		// sql = "ALTER table tbl_name RENAME INDEX old_7index_name TO
		// new_index_name";//mysql5.7不支持
		// String patt =
		// "(?i)ALTER{1,2}[\\s\\S]*?(?i)TABLE{1,2}[\\s\\S]*?(?i)RENAME{1,2}[\\s\\S]*?(?i)INDEX{1,2}[\\s\\S]*?(?i)TO{1,2}[\\s\\S]*?";
		// String patt = "(?i)INDEX(.*?)(?i)TO{1,2}[\\s\\S]*?";
		// Pattern r = Pattern.compile(patt);
		// Matcher m = r.matcher(sql);
		// while (m.find()) {
		// for(int i = 0; i < m.groupCount(); i++) {
		// String patt2 = "(?i)TO(.*?)";
		// Pattern r2 = Pattern.compile(patt);
		// Matcher m2 = r2.matcher(m.group(i));
		// while (m2.find()) {
		// System.out.println(m2.group(0));
		// }
		// }
		// }
		// sql = "ALTER TABLE tbl_name ALTER ABC SET DEFAULT 'DDDD'";
		// sql = "ALTER TABLE tbl_name ALTER COLUMN SID DROP DEFAULT;";
		// sql = "ALTER TABLE tbl_name MODIFY zk_env VARCHAR(16) NOT NULL;";

		// sql = "ALTER TABLE tbl_name CHANGE PHYSICS PHYSISC CHAR(10) NOT NULL";
		// sql = "alter table tbl_name CHANGE oldcol newcol int;";
		// sql = "ALTER TABLE Tbl_name CHANGE c1 C1 BIGINT, ALGORITHM=COPY;";
		// sql = "alter table Tbl_name alter column oldcol set default 1;";
		// sql = "ALTER TABLE buffer_test.test1 CHANGE address addr varchar(100)
		// CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '1' NOT NULL";
		// sql = "ALTER TABLE buffer_test.test1 MODIFY COLUMN addr varchar(100)
		// CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '23' NOT NULL";
		// sql = "ALTER TABLE buffer_test.test1 ADD `type` BIGINT UNSIGNED NULL";
		sql = "CREATE TABLE t_user(user_id INT(10) ZEROFILL)";
		sql = "CREATE TABLE t_user(user_id INT(10) DEFAULT  3);";
		sql = "CREATE TABLE T_USER(user_id INT(10) UNSIGNED);";
		sql = "CREATE TABLE class(\n" + "cla_id INT(6) AUTO_INCREMENT PRIMARY KEY,\n"
				+ "cla_name VARCHAR(30) NOT NULL UNIQUE,\n" + "CHECK(cla_id>0)\n" + ");";
		sql = "CREATE TABLE students(\n" + "stu_id INT(10) AUTO_INCREMENT PRIMARY KEY,\n"
				+ "stu_name VARCHAR(30) NOT NULL,\n" + "stu_score FLOAT(5,2) DEFAULT 0.0,\n" + "cla_id INT(10),\n"
				+ "CONSTRAINT FK_CLA_ID FOREIGN KEY(cla_id) REFERENCES class(cla_id) ON UPDATE CASCADE\n" + ");";
		sql = "CREATE TABLE t1 (c1 INT STORAGE  DISK,c2 INT STORAGE MEMORY) TABLESPACE ts_1 ENGINE NDB;";
		sql = "CREATE TABLE lookup\n" + 
				"  (id INT, INDEX USING BTREE (id))\n" + 
				"  ENGINE = MEMORY;";
		sql = "CREATE OR REPLACE VIEW buffer_test.new_view\n" + 
				"AS select * from test1;";
//		sql = "DELETE FROM test1 where pid = '123' and pid between '1' and '234'";
//		sql = "UPDATE test set name='aabb',createtime=TO_DATE('2018-09-12','YYYY-MM-DD') where pid='123'";
//		SqlParser parser = new SqlParser(EDbVendor.dbvmysql, EDbVendor.dbvpostgresql, "t_test1");
//		List<String> list = parser.ddlParse(sql);
//		System.out.println(list);
		
		 sql = "CREATE TABLE `scripts` (\n" +
				 " `scriptid` bigint(20) unsigned NOT NULL,\n" +
				 " `name` varchar(255) NOT NULL DEFAULT '',\n" +
				 " `command` varchar(255) NOT NULL DEFAULT '',\n" +
				 " `host_access` int(11) NOT NULL DEFAULT '2',\n" +
				 " `usrgrpid` bigint(20) unsigned DEFAULT NULL,\n" +
				 " `groupid` bigint(20) unsigned DEFAULT NULL,\n" +
				 " `description` text NOT NULL,\n" +
				 " `confirmation` varchar(255) NOT NULL DEFAULT '',\n" +
				 " `type` int(11) NOT NULL DEFAULT '0',\n" +
				 " `execute_on` int(11) NOT NULL DEFAULT '1',\n" +
				 " PRIMARY KEY (`scriptid`),\n" +
				 " KEY `scripts_1` (`usrgrpid`),\n" +
				 " KEY `scripts_2` (`groupid`),\n" +
				 " CONSTRAINT `c_scripts_1` FOREIGN KEY (`usrgrpid`) REFERENCES `usrgrp`  (`usrgrpid`) ON DELETE CASCADE ON UPDATE CASCADE,\n" +
				 " CONSTRAINT `c_scripts_2` FOREIGN KEY (`groupid`) REFERENCES `groups`  (`groupid`)\n" +
				 ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
		
		SqlParser sqlParser = new SqlParser(EDbVendor.dbvmysql,EDbVendor.dbvpostgresql,"abc");
		List<String> list = sqlParser.ddlParse(sql);
		for(String s : list) {
			System.out.println(s);
		}
//		TGSqlParser parser = new TGSqlParser(EDbVendor.dbvmysql);
//		
//		parser.sqltext = sql;
//		parser.parse();
//
//		if (parser.getErrorCount() == 0) {
//			TStatementList stmts = parser.sqlstatements;
//			for (int i = 0; i < stmts.size(); i++) {
//				String whereList = "";
//				String updateColumn = "";
//				TCustomSqlStatement stmt = stmts.get(i);
//				if (stmt.sqlstatementtype == ESqlStatementType.sstdelete) {
//					TDeleteSqlStatement deletestmt = (TDeleteSqlStatement) stmt;
//					TWhereClause wc = deletestmt.getWhereClause();
//					TExpression exp = wc.getCondition();
//					exp.getLikeEscapeOperand();
//					exp.getBetweenOperand();
//					ArrayList expList = exp.getFlattedAndOrExprs();
//					for(int j = 0; j < expList.size(); j++) {
//						TExpression expression = (TExpression)expList.get(j);
//						EComparisonType type = expression.getComparisonType();
//						if(type == EComparisonType.unknown) {
//							
//						}else {
//							String oper = expression.getComparisonOperator() == null ? "" : expression.getComparisonOperator().toString();
//							String colName = expression.getStartToken().toString();
//							System.out.println(colName + " " + oper + " ?");
//						}
//					}
//				}
//				else if (stmt.sqlstatementtype == ESqlStatementType.sstupdate) {
//					TUpdateSqlStatement updatestmt = (TUpdateSqlStatement) stmt;
//					if (updatestmt != null) {
//						TResultColumnList colList = updatestmt.getResultColumnList();
//						for(int j = 0; j < colList.size(); j++) {
//							TResultColumn column = colList.getResultColumn(j);
//							updateColumn += column.getExpr();
//							if(j < colList.size() - 1) {
//								updateColumn += ",";
//							}
//						}
//						whereList = updatestmt.getWhereClause().toString();
//					}
//				}
//				
//			}
//		}

	}
	
	class SecurityAccess {
	    public void disopen() {
	        
	    }
	}
	
	
}
