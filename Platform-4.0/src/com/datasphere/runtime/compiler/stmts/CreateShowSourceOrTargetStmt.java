package com.datasphere.runtime.compiler.stmts;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.fileMetaExtension.FileMetadataExtension;
import com.datasphere.utility.GenerateTableFormat;

public class CreateShowSourceOrTargetStmt extends Stmt
{
    private final String componentName;
    private final List<String> props;
    private final int limit;
    private final boolean isDescending;
    
    public String getComponentName() {
        return this.componentName;
    }
    
    public List<String> getProps() {
        return this.props;
    }
    
    public int getLimit() {
        return this.limit;
    }
    
    public boolean isDescending() {
        return this.isDescending;
    }
    
    public CreateShowSourceOrTargetStmt(final String componentName, final int limit, final List<String> props, final boolean isDescending) {
        this.componentName = componentName;
        this.limit = limit;
        this.props = props;
        this.isDescending = isDescending;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        final Compiler.FileMetadataExtensionResult obj = (Compiler.FileMetadataExtensionResult)c.compileShowSourceOrTargetStmt(this);
        if (obj == null) {
            return null;
        }
        if (obj.getResult() == null) {
            System.out.println("Set \"com.datasphere.config.trackFileLineageMetadata\" to true to activate this and restart the server.");
            return null;
        }
        this.prettyPrint(obj);
        return obj;
    }
    
    public void prettyPrint(final Compiler.FileMetadataExtensionResult obj) {
        final List<String> listOfColumnNames = new ArrayList<String>(Arrays.asList("*", "File Name", "Status", "Directory Name", "File Creation Time", "Number Of Events", "First Event Timestamp", "Last Event Timestamp", "Wrap Number"));
        final List<List<String>> listOfRowData = new ArrayList<List<String>>();
        String parentComponent = null;
        int i = 1;
        final String msgToBeDisplayed = obj.getMsg();
        final String appName = obj.getAppName();
        final Set<FileMetadataExtension> fileMetadataExtensions = obj.getResult();
        final Iterator<FileMetadataExtension> itr = fileMetadataExtensions.iterator();
        while (itr.hasNext()) {
            final List<String> list = new ArrayList<String>();
            final FileMetadataExtension fme = itr.next();
            list.add(String.valueOf(i));
            list.add(fme.getFileName());
            list.add(fme.getStatus().toString());
            list.add(fme.getDirectoryName());
            final long externalFileCreationTimestamp = fme.getExternalFileCreationTime();
            if (externalFileCreationTimestamp != 0L) {
                final Timestamp ft = new Timestamp(externalFileCreationTimestamp);
                final Date date = new Date(ft.getTime());
                list.add(String.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date)));
            }
            else {
                list.add("N/A");
            }
            String numberOfEvents = String.valueOf(fme.getNumberOfEvents());
            if (fme.getStatus().equalsIgnoreCase("created") && fme.getNumberOfEvents() == 0L) {
                numberOfEvents = "";
            }
            list.add(numberOfEvents);
            final Long firstEventTimeStamp = fme.getFirstEventTimestamp();
            if (firstEventTimeStamp != null && firstEventTimeStamp != 0L) {
                final Timestamp ft2 = new Timestamp(firstEventTimeStamp);
                final Date date2 = new Date(ft2.getTime());
                list.add(String.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date2)));
            }
            else {
                list.add("N/A");
            }
            final Long lastEventTimeStamp = fme.getLastEventTimestamp();
            if (lastEventTimeStamp != null && lastEventTimeStamp != 0L) {
                final Timestamp ft3 = new Timestamp(lastEventTimeStamp);
                final Date date3 = new Date(ft3.getTime());
                list.add(String.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date3)));
            }
            else {
                list.add("N/A");
            }
            list.add(String.valueOf(fme.getWrapNumber()));
            ++i;
            parentComponent = fme.getParentComponent();
            listOfRowData.add(list);
        }
        if (msgToBeDisplayed != null && fileMetadataExtensions.size() > 0) {
            System.out.println("\n\n======== \n");
            System.out.println("Application Name : " + appName + "\nParent component : " + parentComponent + ".");
            System.out.println("\n" + msgToBeDisplayed);
        }
        System.out.println(GenerateTableFormat.generateTable((List)listOfColumnNames, (List)listOfRowData, new int[] { 1 }));
        System.out.println("\n========");
    }
}
