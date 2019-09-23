package com.datasphere.runtime;

import org.apache.log4j.*;
import org.apache.commons.lang.builder.*;

public class ReleaseNumber
{
    private int major;
    private int minor;
    private int patch;
    private String extras;
    private boolean isDevVersion;
    private static Logger logger;
    
    private ReleaseNumber(final int major, final int minor, final int patch, final String extras) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.extras = extras;
        if (major == 1 && minor == 0 && patch == 0) {
            this.setIsDevVersion(true);
        }
    }
    
    public static ReleaseNumber createReleaseNumber(final int major, final int minor, final int patch) {
        return new ReleaseNumber(major, minor, patch, null);
    }
    
    public static ReleaseNumber createReleaseNumber(final int major, final int minor, final String patch) {
        final int patch_int = Integer.parseInt(Character.toString(patch.charAt(0)));
        final String extras = patch.substring(1);
        return new ReleaseNumber(major, minor, patch_int, extras);
    }
    
    public static ReleaseNumber createReleaseNumber(final String releaseAsString) {
        String[] releaseStringSplit = releaseAsString.split("\\.");
        if (releaseStringSplit.length == 1) {
            releaseAsString.concat(".").concat(String.valueOf(0)).concat(".").concat(String.valueOf(0));
        }
        else if (releaseStringSplit.length == 2) {
            releaseAsString.concat(".").concat(String.valueOf(0));
        }
        else if (releaseStringSplit.length == 3) {
            if (releaseStringSplit[2].length() > 1) {
                final int patch_int = Integer.parseInt(Character.toString(releaseStringSplit[2].charAt(0)));
                final String patch_extra = releaseStringSplit[2].substring(1);
                return new ReleaseNumber(Integer.parseInt(releaseStringSplit[0]), Integer.parseInt(releaseStringSplit[1]), patch_int, patch_extra);
            }
        }
        else if (releaseStringSplit.length > 3) {
            ReleaseNumber.logger.error((Object)("Unexpected release number: " + releaseAsString));
            System.exit(1);
        }
        releaseStringSplit = releaseAsString.split("\\.");
        return new ReleaseNumber(Integer.parseInt(releaseStringSplit[0]), Integer.parseInt(releaseStringSplit[1]), Integer.parseInt(releaseStringSplit[2]), null);
    }
    
    public boolean isDevVersion() {
        return this.isDevVersion;
    }
    
    public void setIsDevVersion(final boolean isDevVersion) {
        this.isDevVersion = isDevVersion;
    }
    
    public boolean greaterThanEqualTo(final ReleaseNumber releaseNumber) {
        return this.isDevVersion || (!releaseNumber.isDevVersion && (this.major > releaseNumber.major || (this.major == releaseNumber.major && this.minor > releaseNumber.minor) || (this.major == releaseNumber.major && this.minor == releaseNumber.minor && this.patch > releaseNumber.patch)));
    }
    
    public boolean lessThan(final ReleaseNumber releaseNumber) {
        return !this.isDevVersion && (releaseNumber.isDevVersion || this.major < releaseNumber.major || (this.major == releaseNumber.major && this.minor < releaseNumber.minor) || (this.major == releaseNumber.major && this.minor == releaseNumber.minor && this.patch < releaseNumber.patch));
    }
    
    @Override
    public String toString() {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.major).append(".").append(this.minor).append(".").append(this.patch);
        if (this.extras != null) {
            stringBuilder.append(this.extras);
        }
        return stringBuilder.toString();
    }
    
    @Override
    public boolean equals(final Object obj) {
        final ReleaseNumber releaseNumber = (ReleaseNumber)obj;
        return this.major == releaseNumber.major && this.minor == releaseNumber.minor && this.patch == releaseNumber.patch;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(this.major).append(this.minor).append(this.patch).toHashCode();
    }
    
    static {
        ReleaseNumber.logger = Logger.getLogger((Class)ReleaseNumber.class);
    }
}
