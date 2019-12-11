package com.datasphere.db.migration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 迁移任务管理器。
 * （1）包含一个迁移任务线程池，每个任务一个线程。
 * （2）数据迁移子线程池，所有任务共享这一个线程池。
 * @author houyunfei
 *
 */
public class MigrationManager {

	ExecutorService migrationService;
	
	ExecutorService migrationSubThreadService;

	public MigrationManager() {
		migrationService = Executors.newCachedThreadPool();
		migrationSubThreadService = Executors.newCachedThreadPool();
	}
	
	public void addMigration(Runnable migration) {
		migrationService.execute(migration);
	}
	
	public void shutdown(){
		migrationService.shutdown();
		while (true) {  
            if (migrationService.isTerminated()) {  
                break;  
            }  
            try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}  
        }
	}
}
