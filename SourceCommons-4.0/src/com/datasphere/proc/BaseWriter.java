package com.datasphere.proc;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.TreeMap;

import com.datasphere.classloading.FormatterLoader;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.intf.Formatter;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.WASecurityManager;
import com.datasphere.uuid.UUID;
import com.datasphere.source.lib.prop.Property;

public abstract class BaseWriter extends BaseProcess
{
    protected Field[] fields;
    protected Formatter formatter;
    protected int bufferSize;
    
    public BaseWriter() {
        this.formatter = null;
        this.bufferSize = 1000000;
    }
    
    public void onDeploy(final Map<String, Object> writerProperties, final Map<String, Object> formatterProperties, final Map<String, Object> parallelismProperties, final UUID inputStream) throws Exception {
        try {
            final MetaInfo.Stream stream = (MetaInfo.Stream)MetadataRepository.getINSTANCE().getMetaObjectByUUID(inputStream, WASecurityManager.TOKEN);
            final MetaInfo.Type dataType = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(stream.dataType, WASecurityManager.TOKEN);
            String typeName = dataType.name;
            if (formatterProperties == null || !formatterProperties.isEmpty()) {
                formatterProperties.put("TypeName", typeName);
            }
            final Class<?> typeClass = ClassLoader.getSystemClassLoader().loadClass(dataType.className);
            this.fields = typeClass.getDeclaredFields();
            if (typeClass.getSimpleName().equals("HDEvent")) {
                final Field[] waEventFields = new Field[4];
                for (final Field field : this.fields) {
                    if (Modifier.isPublic(field.getModifiers())) {
                        if (field.getName().equals("metadata")) {
                            waEventFields[0] = field;
                        }
                        else if (field.getName().equals("data")) {
                            waEventFields[1] = field;
                        }
                        else if (field.getName().equals("before")) {
                            waEventFields[2] = field;
                        }
                        else if (field.getName().equalsIgnoreCase("userdata")) {
                            waEventFields[3] = field;
                        }
                    }
                }
                this.fields = waEventFields;
                if (formatterProperties == null || !formatterProperties.isEmpty()) {
                    formatterProperties.put("EventType", "HDEvent");
                }
            }
            else {
                typeName = typeClass.getName();
                if (!typeName.startsWith("wa.")) {
                    throw new AdapterException("Unsupported type " + typeName + ". Formatters currently support formatting TypedEvent and HDEvent only. Please convert this type " + typeName + " to a DataExchange type.");
                }
                final Field[] typedEventFields = new Field[this.fields.length - 1];
                int i = 0;
                for (final Field field2 : this.fields) {
                    if (Modifier.isPublic(field2.getModifiers())) {
                        if (!"mapper".equals(field2.getName())) {
                            typedEventFields[i] = field2;
                            ++i;
                        }
                    }
                }
                this.fields = typedEventFields;
            }
            if (stream.partitioningFields.size() > 0) {
                final int[] keyFieldIndex = new int[stream.partitioningFields.size()];
                int index = 0;
                for (int j = 0; j < stream.partitioningFields.size(); ++j) {
                    final String keyFieldName = stream.partitioningFields.get(j);
                    for (int k = 0; k < this.fields.length; ++k) {
                        if (this.fields[k].getName().equals(keyFieldName)) {
                            keyFieldIndex[index] = k;
                            ++index;
                            break;
                        }
                    }
                }
                writerProperties.put("partitionFieldIndex", keyFieldIndex);
            }
            if (writerProperties.containsKey("Mode")) {
                String mode = (String)writerProperties.get("Mode");
                if (writerProperties.containsKey("KafkaConfig")) {
                    final String value = (String)writerProperties.get("KafkaConfig");
                    if (value != null && !value.isEmpty()) {
                        final String[] split;
                        final String[] splitValues = split = value.split(";");
                        final int length3 = split.length;
                        int n2 = 0;
                        while (n2 < length3) {
                            final String splitValue = split[n2];
                            final String[] property = splitValue.split("=");
                            if (property == null || property.length < 2) {
                                throw new RuntimeException("Kafka Property \"" + property[0] + "\" is invalid. Expected structure of KafkaConfig is <name>=<value>;<name>=<value>");
                            }
                            if (property[0].equalsIgnoreCase("producer.type")) {
                                if (property[1].equalsIgnoreCase("Sync") || property[1].equalsIgnoreCase("Async")) {
                                    mode = property[1];
                                    break;
                                }
                                break;
                            }
                            else {
                                ++n2;
                            }
                        }
                    }
                }
                String messageVersion = "v2";
                if (writerProperties.containsKey("KafkaMessageFormatVersion")) {
                    messageVersion = (String)writerProperties.get("KafkaMessageFormatVersion");
                }
                if (mode.equalsIgnoreCase("sync") && messageVersion.equalsIgnoreCase("v2")) {
                    formatterProperties.put("e1p", true);
                }
            }
        }
        catch (MetaDataRepositoryException | ClassNotFoundException e) {
            throw new AdapterException("Failure in Writer initialization. Problem in retrieving field information from stream's type", (Throwable)e);
        }
        if (formatterProperties == null || !formatterProperties.isEmpty()) {
            this.formatter = FormatterLoader.loadFormatter((Map)formatterProperties, this.fields);
        }
    }
    
    public void init(final Map<String, Object> writerProperties, final Map<String, Object> formatterProperties, final UUID inputStream, final String distributionID) throws Exception {
        super.init((Map)writerProperties, (Map)formatterProperties, inputStream, distributionID);
        final Map<String, Object> localPropertyMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        localPropertyMap.putAll(writerProperties);
        localPropertyMap.putAll(formatterProperties);
        final Property property = new Property(localPropertyMap);
        this.bufferSize = property.getInt("buffersize", 1000000);
    }
}
