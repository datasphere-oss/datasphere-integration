package com.datasphere.db.migration.scheduler;

import com.datasphere.db.config.MSSQLConfig;
import com.datasphere.db.config.MySQLConfig;
import com.datasphere.db.config.OracleConfig;
import com.datasphere.db.config.PGConfig;
import com.datasphere.db.config.HiveConfig;
import com.datasphere.db.migration.Migration;
import com.datasphere.db.migration.scheduler.impl.MSSQLHiveMigrationScheduler;
import com.datasphere.db.migration.scheduler.impl.MySQL2HiveMigrationScheduler;
import com.datasphere.db.migration.scheduler.impl.Oracle2HiveMigrationScheduler;
import com.datasphere.db.migration.scheduler.impl.PGSql2HiveMigrationScheduler;

/**
 * 迁移任务调度器工厂类。
 * @author houyunfei
 *
 */
public class MigrationSchedulerFactory {

	public static MigrationScheduler createMigrationScheduler(Migration migration) {
		AbstractMigrationScheduler migrationScheduler = null;
		if(migration.getSourceConfig() instanceof PGConfig && migration.getDestConfig() instanceof HiveConfig) {
			migrationScheduler = new PGSql2HiveMigrationScheduler();
		} else if(migration.getSourceConfig() instanceof MSSQLConfig && migration.getDestConfig() instanceof HiveConfig) {
		    migrationScheduler = new MSSQLHiveMigrationScheduler();
		} else if(migration.getSourceConfig() instanceof OracleConfig && migration.getDestConfig() instanceof HiveConfig) {
		    migrationScheduler = new Oracle2HiveMigrationScheduler();
		} else if(migration.getSourceConfig() instanceof MySQLConfig && migration.getDestConfig() instanceof HiveConfig) {
		    migrationScheduler = new MySQL2HiveMigrationScheduler();
		}
		
		
		else {
			throw new RuntimeException("未定义类型" + migration.getSourceConfig().getClass() + "到类型" + migration.getDestConfig().getClass() + "的调度器！");
		}
		migrationScheduler.setMigration(migration);
		return migrationScheduler;
	}
}
