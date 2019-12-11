package com.datasphere.db.migration.scheduler;

import com.datasphere.db.config.MSSQLConfig;
import com.datasphere.db.config.OracleConfig;
import com.datasphere.db.config.PGConfig;
import com.datasphere.db.config.SnappyConfig;
import com.datasphere.db.migration.Migration;
import com.datasphere.db.migration.scheduler.impl.MSSQLSnappyMigrationScheduler;
import com.datasphere.db.migration.scheduler.impl.Oracle2SnappyMigrationScheduler;
import com.datasphere.db.migration.scheduler.impl.PGSql2SnappyMigrationScheduler;

/**
 * 迁移任务调度器工厂类。
 * @author houyunfei
 *
 */
public class MigrationSchedulerFactory {

	public static MigrationScheduler createMigrationScheduler(Migration migration) {
		AbstractMigrationScheduler migrationScheduler = null;
		if(migration.getSourceConfig() instanceof PGConfig && migration.getDestConfig() instanceof SnappyConfig) {
			migrationScheduler = new PGSql2SnappyMigrationScheduler();
		} else if(migration.getSourceConfig() instanceof MSSQLConfig && migration.getDestConfig() instanceof SnappyConfig) {
		    migrationScheduler = new MSSQLSnappyMigrationScheduler();
		} else if(migration.getSourceConfig() instanceof OracleConfig && migration.getDestConfig() instanceof SnappyConfig) {
		    migrationScheduler = new Oracle2SnappyMigrationScheduler();
		} 
		else {
			throw new RuntimeException("未定义类型" + migration.getSourceConfig().getClass() + "到类型" + migration.getDestConfig().getClass() + "的调度器！");
		}
		migrationScheduler.setMigration(migration);
		return migrationScheduler;
	}
}
