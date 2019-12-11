package com.datasphere.common.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 快捷输出类；
 * 1.输出到System.out对象
 * @author theseusyang
 *
 */
public class O {
	
	public static void log(Object obj) {
		if(obj == null) return;
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
		System.out.println(sdf.format(new Date()) + " Thread(" + String.valueOf(Thread.currentThread().getId()) + ")--" + obj);
	}
}
