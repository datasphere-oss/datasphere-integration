package com.datasphere.source.smlite;

public class NumericExpression extends Expression
{
    public NumericExpression(final CharParser parser, final String[] pattern, final SMEvent event) {
        super(parser, pattern, event);
    }
    
    @Override
    protected void initializeHashLookup() {
        int numericOrdinal = 0;
        for (int pItr = 0; pItr < this.pattern.length; ++pItr) {
            for (int charItr = 0; charItr < this.pattern[pItr].length(); ++charItr) {
                if (numericOrdinal == 0 && this.pattern[pItr].charAt(charItr) >= '0' && this.pattern[pItr].charAt(charItr) <= '9') {
                    for (char itr = '0'; itr <= '9'; ++itr) {
                        if (numericOrdinal == 0) {
                            numericOrdinal = this.parser.addIntoLookupTable(itr);
                        }
                        else {
                            this.parser.addIntoLookupTable(itr, numericOrdinal);
                        }
                    }
                }
                else {
                    this.parser.addIntoLookupTable(this.pattern[pItr].charAt(charItr));
                }
            }
        }
    }
}
