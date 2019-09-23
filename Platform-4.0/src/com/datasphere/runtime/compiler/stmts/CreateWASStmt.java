package com.datasphere.runtime.compiler.stmts;

import java.util.List;
import java.util.Map;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.Interval;
import com.datasphere.runtime.Property;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.TypeDefOrName;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.security.Password;
import com.datasphere.utility.Utility;

public class CreateWASStmt extends CreateStmt
{
    public final TypeDefOrName typeDef;
    public final List<EventType> evenTypes;
    public final Interval howToPersist;
    public final List<Property> properties;
    
    public CreateWASStmt(final String name, final Boolean doReplace, final TypeDefOrName typeDef, final List<EventType> ets, final Interval how, final List<Property> props) {
        super(EntityType.HDSTORE, name, doReplace);
        this.typeDef = typeDef;
        this.evenTypes = ets;
        this.howToPersist = how;
        this.properties = props;
        this.encryptJdbcPassword();
    }
    
    private void encryptJdbcPassword() {
        if (this.properties == null || this.properties.isEmpty()) {
            return;
        }
        final Map<String, Object> map = Utility.makePropertyMap(this.properties);
        final String key = "jdbc_password";
        final String passwd = (String)map.get(key);
        if (passwd == null || passwd.isEmpty()) {
            return;
        }
        if (Utility.isValueEncryptionFlagSetToTrue(key, map)) {
            return;
        }
        if (Utility.isValueEncryptionFlagExists(key, map)) {
            for (int ik = 0; ik < this.properties.size(); ++ik) {
                if (this.properties.get(ik).name.equalsIgnoreCase(key)) {
                    this.properties.set(ik, new Property(this.properties.get(ik).name, Password.getEncryptedStatic((String)this.properties.get(ik).value)));
                }
                if (this.properties.get(ik).name.equalsIgnoreCase(key + "_encrypted")) {
                    this.properties.set(ik, new Property(this.properties.get(ik).name, true));
                }
            }
        }
        else {
            for (int ik = 0; ik < this.properties.size(); ++ik) {
                if (this.properties.get(ik).name.equalsIgnoreCase(key)) {
                    this.properties.set(ik, new Property(this.properties.get(ik).name, Password.getEncryptedStatic((String)this.properties.get(ik).value)));
                }
            }
            this.properties.add(new Property(key + "_encrypted", true));
        }
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileCreateWASStmt(this);
    }
}
