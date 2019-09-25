package com.datasphere.databasewriter;

public class PositionData {
	private long poistion;
	private String sourceTableName;
	private String targetTableName;
	private String targetUUID;
	private String rowid;
	private String currentScn;
	private String commitScn;
	private String scn;
	private String startScn;
	
	public long getPoistion() {
		return poistion;
	}
	public void setPoistion(long poistion) {
		this.poistion = poistion;
	}
	
	public String getSourceTableName() {
		return sourceTableName;
	}
	public void setSourceTableName(String sourceTableName) {
		this.sourceTableName = sourceTableName;
	}
	public String getTargetTableName() {
		return targetTableName;
	}
	public void setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
	}
	public String getTargetUUID() {
		return targetUUID;
	}
	public void setTargetUUID(String targetUUID) {
		this.targetUUID = targetUUID;
	}
	public String getRowid() {
		return rowid;
	}
	public void setRowid(String rowid) {
		this.rowid = rowid;
	}
	public String getCurrentScn() {
		return currentScn;
	}
	public void setCurrentScn(String currentScn) {
		this.currentScn = currentScn;
	}
	public String getCommitScn() {
		return commitScn;
	}
	public void setCommitScn(String commitScn) {
		this.commitScn = commitScn;
	}
	public String getScn() {
		return scn;
	}
	public void setScn(String scn) {
		this.scn = scn;
	}
	public String getStartScn() {
		return startScn;
	}
	public void setStartScn(String startScn) {
		this.startScn = startScn;
	}
	
	
}
