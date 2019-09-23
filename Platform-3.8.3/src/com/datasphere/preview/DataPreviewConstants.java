package com.datasphere.preview;

import com.datasphere.runtime.*;

public class DataPreviewConstants
{
    public static final String SPLIT_CONSTANT = "parserProperties=";
    public static final String SPACE = " ";
    public static final String SAMPLES = "samples";
    public static final String APPDATA = "appData";
    public static final String HOME;
    public static final String PLATFORM = "Platform";
    public static final String POSDATA = "/PosApp/appData/PosDataPreview.csv";
    public static final String POSDATATYPE = "CSV";
    public static final String RETAILDATA = "/RetailApp/appData/RetailDataPreview.csv";
    public static final String RETAILDATATYPE = "CSV";
    public static final String MULTILOGDATA = "/MultiLogApp/appData/ApacheAccessLogPreview";
    public static final String MULTILOGTYPE = "LOG";
    
    static {
        HOME = NodeStartUp.getPlatformHome();
    }
    
    public enum DataFileInfo
    {
        posdata {
            @Override
            public String getPath() {
                return "/PosApp/appData/PosDataPreview.csv";
            }
            
            @Override
            public String getType() {
                return "CSV";
            }
        }, 
        multilogdata {
            @Override
            public String getPath() {
                return "/MultiLogApp/appData/ApacheAccessLogPreview";
            }
            
            @Override
            public String getType() {
                return "LOG";
            }
        }, 
        retaildata {
            @Override
            public String getPath() {
                return "/RetailApp/appData/RetailDataPreview.csv";
            }
            
            @Override
            public String getType() {
                return "CSV";
            }
        };
        
        public abstract String getPath();
        
        public abstract String getType();
    }
}
