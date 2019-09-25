package com.datasphere.OperationHandler;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.mahout.math.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.common.constants.Constant;
import com.datasphere.BitPattern.BitPatternMap;
import com.datasphere.TypeHandler.TableToTypeHandlerMap;
import com.datasphere.TypeHandler.TypeHandler;
import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.exception.UnsupportedMapping;
import com.datasphere.exception.UpdateTypeMapException;
import com.datasphere.intf.DBInterface;
import com.datasphere.proc.events.HDEvent;

public class PkUpdateHandler extends UpdateHandler
{
    private Logger logger;
    
    public PkUpdateHandler(final DBInterface intf) {
        super(intf);
        this.logger = LoggerFactory.getLogger((Class)PkUpdateHandler.class);
    }
    
    @Override
    protected int bindAfter(final String targetTable, TypeHandler[] columns, final HDEvent event, final PreparedStatement stmt) throws SQLException, DatabaseWriterException {
        int bindIndex = 0;
        int itr = 0;
        int columnIndex = 0;
        final Integer[] dataFields = BitPatternMap.getPattern(event, event.dataPresenceBitMap);
        final Object[] data = event.data;
        while (true) {
            try {
                bindIndex = 0;
                for (itr = 0; itr < dataFields.length; ++itr) {
                    columnIndex = dataFields[itr];
                    if (data[columnIndex] == null) {
                        this.bindNull(columns[columnIndex], stmt, ++bindIndex);
                    }
                    else {
                        columns[columnIndex].bind(stmt, ++bindIndex, data[columnIndex]);
                    }
                }
            }
            catch (UpdateTypeMapException exp) {
                final String key = TableToTypeHandlerMap.formKey(targetTable, event);
                try {
                    for (int xitr = itr; xitr < dataFields.length; ++xitr) {
                        columnIndex = dataFields[xitr];
                        if (data[columnIndex] != null) {
                            columns = this.tableToTypeHandlerMap.updateColumnMapping(key, columnIndex, data[columnIndex].getClass());
                        }
                    }
                }
                catch (UnsupportedMapping mappingExp) {
                    final String srcTable = (String)event.metadata.get(Constant.TABLE_NAME);
                    final String errMsg = "Mapping of SourceType {" + mappingExp.sourceType + "} to TargetType {" + mappingExp.targetType + "} is not supported. Source Table {" + srcTable + "} Target Table {" + targetTable + "}, Column index {" + columnIndex + "}";
                    throw new DatabaseWriterException(errMsg);
                }
                continue;
            }
            catch (Exception uExp) {
                this.logger.error("Unhandled exception occured while biding after image", (Throwable)uExp);
                this.logger.error("Event caused this issue = " + event.toJSON());
                final Integer[] afterImageCols = BitPatternMap.getPattern(event, event.dataPresenceBitMap);
                final Integer[] beforeIamgeCols = BitPatternMap.getPattern(event, event.beforePresenceBitMap);
                this.logger.error("After Image column referred " + Arrays.toString((Object[])afterImageCols));
                this.logger.error("Before Image column referred " + Arrays.toString((Object[])beforeIamgeCols));
                try {
                    final String debugQuery = this.generateDML(event, targetTable);
                    this.logger.error("Debug SQL Statement {" + debugQuery + "}");
                }
                catch (Exception ex) {}
                throw uExp;
            }
            break;
        }
        return bindIndex;
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
