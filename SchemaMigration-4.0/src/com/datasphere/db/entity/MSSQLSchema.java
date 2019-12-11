package com.datasphere.db.entity;

public class MSSQLSchema extends Schema{
	
	//数据库名称
	String dbNname;
	
	public MSSQLSchema(String name) {
		super(name);
	}

	public String getDbName() {
		return dbNname;
	}

	public void setDbName(String dBname) {
		dbNname = dBname;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MSSQLSchema other = (MSSQLSchema) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (dbNname == null) {
			if (other.dbNname != null)
				return false;
		} else if (!dbNname.equals(other.dbNname))
			return false;
		return true;
	}
}
