package com.datasphere.proc;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.math.NumberUtils;

import com.datasphere.common.constants.Constant;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.utils.Utils;
import com.datasphere.Checkpoint.CheckpointTableImpl;
import com.datasphere.Policy.Policy;
import com.datasphere.Policy.PolicyFactory;
import com.datasphere.exception.DatabaseWriterException;
import com.sun.media.jfxmedia.logging.Logger;

public class Validator
{
    Property prop;
    StringBuilder info;
    static String POSTGRES_NO_TYPE_CHECK_PROP;
    Map<String, Integer> policyDefault;
    
    public Validator(final Map<String, Object> propMap) {
        this.policyDefault = new TreeMap<String, Integer>((Comparator)String.CASE_INSENSITIVE_ORDER) {
            {
                this.put(Policy.EVENT_COUNT, Policy.DEFAULT_EVENT_COUNT);
                this.put(Policy.INTERVAL, Policy.DEFAULT_INTERVAL);
            }
        };
        this.prop = new Property((Map)propMap);
    }
    
    public boolean validate() {
        this.info = new StringBuilder();
        try {
            this.validateConnectionString(this.info);
            this.validateBatchPolicy(this.info);
            this.validateCommitPolicy(this.info);
            this.validateWildcardProperty(this.info);
            this.validateCheckpointTable(this.info);
        }
        catch (ImproperConfig exp) {
            return false;
        }
        return true;
    }
    
    public void validateConnectionString(final StringBuilder msg) throws ImproperConfig {
        final String url = this.prop.getString(Constant.CONNECTION_URL, (String)null);
        if (url == null || url.isEmpty()) {
            msg.append("\n\n" + Constant.CONNECTION_URL + "- can not be empty or null");
            throw new ImproperConfig();
        }
        final String targetType = Utils.getDBType(url);
        
//        if ((Constant.POSTGRESS_TYPE.equalsIgnoreCase(targetType) || Constant.EDB_TYPE.equalsIgnoreCase(targetType)) && !url.contains(Validator.POSTGRES_NO_TYPE_CHECK_PROP)) {
//            msg.append("\n\n Please specify {" + Validator.POSTGRES_NO_TYPE_CHECK_PROP + "} as part of URL, like " + url + Validator.POSTGRES_NO_TYPE_CHECK_PROP);
//            throw new ImproperConfig();
//        }
    }
    
    private void validateBatchPolicy(final StringBuilder msg) throws ImproperConfig {
        final String batchPolicy = this.prop.getString(PolicyFactory.BATCH_POLICY, (String)null);
        if (batchPolicy == null || batchPolicy.isEmpty()) {
            msg.append("\n\nBatchPolicy is not spcified, supported format is {" + PolicyFactory.DEFAULT_BATCH_POLICY + "}");
            throw new ImproperConfig();
        }
        final Property batchProp = PolicyFactory.getPolicyProperties(this.prop, PolicyFactory.BATCH_POLICY);
        this.validatePolicyProperty("BatchPolicy", PolicyFactory.DEFAULT_BATCH_POLICY, batchProp, msg);
    }
    
    private void validateCommitPolicy(final StringBuilder msg) throws ImproperConfig {
        final String batchPolicy = this.prop.getString(PolicyFactory.COMMIT_POLICY, (String)null);
        if (batchPolicy == null || batchPolicy.isEmpty()) {
            msg.append("\n\nCommitPolicy is not spcified, supported format is {" + PolicyFactory.DEFAULT_COMMIT_POLICY + "}");
            throw new ImproperConfig();
        }
        final Property batchProp = PolicyFactory.getPolicyProperties(this.prop, PolicyFactory.COMMIT_POLICY);
        this.validatePolicyProperty("CommitPolicy", PolicyFactory.DEFAULT_COMMIT_POLICY, batchProp, msg);
    }
    
    private void validatePolicyProperty(final String nameOfThePolicy, final String defaultValue, final Property prop, final StringBuilder msg) throws ImproperConfig {
        boolean emptyMap = true;
        for (final String entry : this.policyDefault.keySet()) {
            if (!prop.propMap.containsKey(entry)) {
                msg.append("\n\nCommitPolicy Property {" + entry + "} is not spcified, setting to default value {" + this.policyDefault.get(entry) + "}");
            }
            else {
                emptyMap = false;
                final Object value = prop.propMap.get(entry);
                if (value instanceof Integer || value instanceof Long) {
                    continue;
                }
                if (value instanceof String && NumberUtils.isNumber((String)value)) {
                    continue;
                }
                msg.append("\n\n" + nameOfThePolicy + " - Policy property {" + entry + "} is suppose to be number");
                throw new ImproperConfig();
            }
        }
        if (emptyMap) {
            msg.delete(0, msg.length());
            String keys = "";
            for (final String key : this.policyDefault.keySet()) {
                if (keys.isEmpty()) {
                    keys = key;
                }
                else {
                    keys = keys + "," + key;
                }
            }
            msg.append("\n\n" + nameOfThePolicy + " - Atleast one of the policy property is expected {" + keys + "}, or do not specify this propperty for system to use default value {" + defaultValue + "}");
            throw new ImproperConfig();
        }
    }
    
    private void validateWildcardProperty(final StringBuilder msg) throws ImproperConfig {
        String maps = this.prop.getString("Maps", (String)null);
        if (maps == null) {
            maps = this.prop.getString("Tables", (String)null);
        }
        if (maps == null) {
            msg.append("\n\nWildcard mapping property (Tables) is not specified, please specify table mapping as <SrcDB>.<SrcTable>,<TargetDB>.<TargetTable>");
            throw new ImproperConfig();
        }
        if (maps.isEmpty()) {
            msg.append("\n\nWildcard mapping property (Tables) is empty, please specify table mapping as <SrcDB>.<SrcTable>,<TargetDB>.<TargetTable>");
            throw new ImproperConfig();
        }
    }
    
    private void validateCheckpointTable(final StringBuilder msg) throws ImproperConfig {
        final String chkPntTal = this.prop.getString("CheckPointTable", (String)null);
        CheckpointTableImpl impl = null;
        try {
            impl = CheckpointTableImpl.getInstance(this.prop, null);
        }
        catch (DatabaseWriterException e) {
            msg.append("\n\nFailed to get checkpoint table implementation");
            throw new ImproperConfig();
        }
        if (impl.createSQL() != null) {
            if (chkPntTal == null) {
                msg.append("\n" + impl.createSQL());
                throw new ImproperConfig();
            }
            if (chkPntTal.isEmpty()) {
                msg.append("\n\nCheckpointTable property is Empty, please use the below SQL for creating it");
                msg.append("\n" + impl.createSQL());
                throw new ImproperConfig();
            }
        }
    }
    
    public String getMessage() {
        return this.info.toString();
    }
    
    static {
        Validator.POSTGRES_NO_TYPE_CHECK_PROP = "?stringtype=unspecified";
    }
    
    class ImproperConfig extends Exception
    {
    }
    
    public static void main(String[] args) {
    		try {
    			String url = "jdbc:postgresql://172.16.11.11:5432/postgres";
			URI jURI = new URI(url.substring(5));
			jURI.getScheme();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
