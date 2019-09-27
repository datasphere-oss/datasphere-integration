package com.datasphere.source.sm;

public class Delimiter
{
    private String delimiterString;
    private boolean singleChar;
    private char firstChar;
    private char[] delimiterArray;
    private int offset;
    private boolean completedFlag;
    private boolean isMatched;
    
    public boolean isMatched() {
        return this.isMatched;
    }
    
    public boolean isCompleted() {
        return this.completedFlag;
    }
    
    public Delimiter(final String str) {
        this.delimiterString = str;
        this.delimiterArray = str.toCharArray();
        this.firstChar = this.delimiterArray[0];
        this.singleChar = (this.delimiterArray.length == 1);
        this.offset = 0;
        this.completedFlag = false;
        this.isMatched = false;
    }
    
    public boolean compare(final char c) {
        if (this.completedFlag) {
            return this.isMatched;
        }
        if (this.singleChar && this.firstChar == c) {
            this.completedFlag = true;
            return this.isMatched = true;
        }
        if ((this.offset == 0 && this.firstChar == c) || c == this.delimiterArray[this.offset]) {
            ++this.offset;
            if (this.delimiterArray.length == this.offset) {
                this.completedFlag = true;
                this.isMatched = true;
            }
            return true;
        }
        this.offset = 0;
        this.completedFlag = true;
        return false;
    }
    
    public void reset(final boolean forceReset) {
        if (this.completedFlag || forceReset) {
            this.offset = 0;
            this.completedFlag = false;
            this.isMatched = false;
        }
    }
    
    public int length() {
        return this.delimiterArray.length;
    }
    
    public String getString() {
        return this.delimiterString;
    }
}
