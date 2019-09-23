package com.datasphere.runtime.compiler.patternmatch;

public class PatternRepetition
{
    public static final PatternRepetition zeroOrMore;
    public static final PatternRepetition oneOrMore;
    public static final PatternRepetition zeroOrOne;
    public final int mintimes;
    public final int maxtimes;
    private final int symbol;
    
    public PatternRepetition(final int mintimes, final int maxtimes, final int symbol) {
        this.mintimes = mintimes;
        this.maxtimes = maxtimes;
        this.symbol = symbol;
    }
    
    @Override
    public String toString() {
        if (this.symbol != 0) {
            return String.valueOf((char)this.symbol);
        }
        if (this.maxtimes == this.mintimes) {
            return "{" + this.mintimes + "}";
        }
        return "{" + this.mintimes + ", " + this.maxtimes + "}";
    }
    
    static {
        zeroOrMore = new PatternRepetition(0, Integer.MAX_VALUE, 42);
        oneOrMore = new PatternRepetition(1, Integer.MAX_VALUE, 43);
        zeroOrOne = new PatternRepetition(0, 1, 63);
    }
}
