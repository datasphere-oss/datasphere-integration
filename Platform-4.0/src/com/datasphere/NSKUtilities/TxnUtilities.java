package com.datasphere.NSKUtilities;

import java.math.*;

public final class TxnUtilities
{
    public static String formatNSKTxnID(final String TxnID, final String SystemName) {
        final BigInteger bigTxnID = new BigInteger(TxnID);
        final int flags = bigTxnID.and(new BigInteger("FFFF", 16)).intValue();
        final int sequence = bigTxnID.and(new BigInteger("FFFFFFFF0000", 16)).shiftRight(16).intValue();
        final int cpu = bigTxnID.and(new BigInteger("FF000000000000", 16)).shiftRight(48).intValue();
        String res = SystemName;
        if (flags != 0) {
            res = res + "(" + flags + ")";
        }
        res = res + "." + cpu + "." + sequence;
        return res;
    }
    
    public static boolean NSKTxnIDsAreSame(final String TxnID1, final String TxnID2) {
        BigInteger bigTxnID1 = new BigInteger(TxnID1);
        BigInteger bigTxnID2 = new BigInteger(TxnID2);
        bigTxnID1 = bigTxnID1.and(new BigInteger("FFFFFFFFFFFFFFFF", 16));
        bigTxnID2 = bigTxnID2.and(new BigInteger("FFFFFFFFFFFFFFFF", 16));
        return bigTxnID1.compareTo(bigTxnID2) == 0;
    }
    
    public static String convertNSKTxnIDToUnsigned(final String TxnID) {
        BigInteger bigTxnID = new BigInteger(TxnID);
        bigTxnID = bigTxnID.and(new BigInteger("FFFFFFFFFFFFFFFF", 16));
        return bigTxnID.toString();
    }
    
    public static void main(final String[] args) {
        System.out.println("Formatted Transaction ID is " + formatNSKTxnID("4828422733004865538", "\\NSKIT06"));
        System.out.println("Formatted Transaction ID is " + formatNSKTxnID("-9006635322277298174", "\\NSKIT06"));
        System.out.println("Formatted Transaction ID is " + formatNSKTxnID("9440108751432253442", "\\NSKIT06"));
        System.out.println("Compare should give true " + NSKTxnIDsAreSame("9440108751432253442", "-9006635322277298174"));
        System.out.println("Compare should give false " + NSKTxnIDsAreSame("9440108751432253442", "4828422733004865538"));
        System.out.println("Should give 9440108751432253442 " + convertNSKTxnIDToUnsigned("9440108751432253442"));
        System.out.println("Should give 9440108751432253442 " + convertNSKTxnIDToUnsigned("-9006635322277298174"));
    }
}
