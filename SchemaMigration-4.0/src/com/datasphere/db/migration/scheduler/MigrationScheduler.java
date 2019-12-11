package com.datasphere.db.migration.scheduler;

import java.util.concurrent.ExecutorService;

import com.datasphere.db.migration.Migration;

/**
 * 迁移任务调度器接口。
 * @author houyunfei
 *
 */
public abstract class MigrationScheduler {

	Migration migration;
	
	ExecutorService dataLoadExecutorService;

	public void setDataLoadExecutorService(ExecutorService dataLoadExecutorService) {
		this.dataLoadExecutorService = dataLoadExecutorService;
	}

	public void setMigration(Migration migration) {
		this.migration = migration;
	}

	public Migration getMigration() {
		return migration;
	}

	public ExecutorService getDataLoadExecutorService() {
		return dataLoadExecutorService;
	}

	public abstract void doSchedule() throws Exception;
}
