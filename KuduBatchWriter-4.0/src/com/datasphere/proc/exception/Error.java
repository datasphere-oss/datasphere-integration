package com.datasphere.proc.exception;

public enum Error
{
    INCORRECT_TABLE_MAP(2749, "表属性中指定的表映射不正确"), 
    TABLES_NOT_SPECIFIED(2709, "未指定映射请指定表参数."), 
    INCORRECT_CHECKPOINT_TABLE_STRCUTRE(2748, "检查点表结构不正确"), 
    MAPPED_COLUMN_DOES_NOT_EXISTS(2754, "映射字段不存在."), 
    INCONSISTENT_TABLE_STRUCTURE(2751, "源表和目标表结构不一致"), 
    TARGET_TABLE_DOESNOT_EXISTS(2747, "目标表不存在");
    
    private int type;
    private String text;
    
    private Error(final int type, final String text) {
        this.type = type;
        this.text = text;
    }
    
    @Override
    public String toString() {
        return this.type + " : " + this.text;
    }
    
    public int getType() {
        return this.type;
    }
}
