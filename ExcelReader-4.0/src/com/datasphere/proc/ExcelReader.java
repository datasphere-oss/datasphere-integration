package com.datasphere.proc;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.common.constants.Constant;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.event.Event;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.components.Flow;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.HDEvent;
import com.github.Dorae132.easyutil.easyexcel.ExcelProperties;
import com.github.Dorae132.easyutil.easyexcel.ExcelUtils;
import com.github.Dorae132.easyutil.easyexcel.read.IReadDoneCallBack;
import com.github.Dorae132.easyutil.easyexcel.read.IRowConsumer;

@PropertyTemplate(name = "ExcelReader", type = AdapterType.source, properties = {
		@PropertyTemplateProperty(name = "directory", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "wildcard", type = String.class, required = true, defaultValue = ""),
		@PropertyTemplateProperty(name = "beginline", type = Integer.class, required = true, defaultValue = "1") }, inputType = HDEvent.class)
public class ExcelReader extends SourceProcess {
	private String directory;
	private String wildcard;
	private int beginline;
	private boolean isStop = true;

	@Override
	public void init(Map<String, Object> properties, Map<String, Object> properties2, UUID sourceUUID,
			String distributionID, SourcePosition restartPosition, boolean sendPositions, Flow flow) throws Exception {
		super.init(properties, properties2, sourceUUID, distributionID, restartPosition, sendPositions, flow);
		this.directory = properties.get("directory").toString();
		this.wildcard = properties.get("wildcard").toString();
		this.beginline = Integer.parseInt(properties.get("beginline").toString());
		if (this.beginline <= 0) {
			this.beginline = 1;
		}
		this.isStop = true;
	}

	@Override
	public void receiveImpl(int p0, Event p1) throws Exception {
		if (!isStop) {
			isStop = true;
		}
		while (isStop) {
			readExcel(this.directory);
		}
	}

	private void readExcel(String path) throws Exception {
		File file = new File(path);
		if (!file.exists()) {
			throw new AdapterException("目录" + path + "不存在");
		}
		File[] files = file.listFiles();
		for (File file2 : files) {
			if (file2.isDirectory()) {
				readExcel(file2.getAbsolutePath());
			}
			String fileName = file2.getPath();
			String dir = file2.getParent();
			String name = file2.getName();
			if (wildcardMatch(this.wildcard, fileName)) {
				System.out.println("dir:" + dir + "  name:"+ name);
//				List list = ExcelUtils.getInstance().readExcel2List(fileName);
				
				HDEvent out = new HDEvent(1, this.sourceUUID);
				out.metadata = new HashMap<>();
				out.metadata.put(Constant.OPERATION, "INSERT");
				out.metadata.put(Constant.TABLE_NAME, fileName);
				out.typeUUID = this.typeUUID;
				
				AtomicInteger count = new AtomicInteger(0);
				long start = System.currentTimeMillis();
				ExcelUtils.excelRead(ExcelProperties.produceReadProperties(dir + File.separator,
						name), new IRowConsumer<Object>() {
							@Override
							public void consume(List<Object> row) {
								if(isStop) {
									count.incrementAndGet();
									if(count.get() > beginline) {
										try {
//											System.out.println(row);
											out.data = new Object[row.size()];
											for(int i = 0; i < row.size(); i++) {
												out.setData(i,  row.get(i));
//												System.out.println(row.get(i));
											}
											send((Event) out, 0);
										} catch (Exception e) {
											e.printStackTrace();
										}
	//									System.out.println(row);
									}
								}
							}
						}, new IReadDoneCallBack<Void>() {
							@Override
							public Void call() {
								System.out.println(
										"fileName:" + name + " rows: " + count.get() + "\ttimed: " + (System.currentTimeMillis() - start));
								
								return null;
							}
						}, 1, false);
			}
		}

	}

	@Override
	public void close() throws Exception {
		super.close();
		isStop = false;
	}

	/**
	 * 通配符匹配
	 * 
	 * @param pattern
	 *            通配符模式
	 * @param str
	 *            待匹配的字符串
	 * @return 匹配成功则返回true，否则返回false
	 */
	private static boolean wildcardMatch(String pattern, String str) {
		int patternLength = pattern.length();
		int strLength = str.length();
		int strIndex = 0;
		char ch;
		for (int patternIndex = 0; patternIndex < patternLength; patternIndex++) {
			ch = pattern.charAt(patternIndex);
			if (ch == '*') {
				while (strIndex < strLength) {
					if (wildcardMatch(pattern.substring(patternIndex + 1), str.substring(strIndex))) {
						return true;
					}
					strIndex++;
				}
			} else if (ch == '?') {
				strIndex++;
				if (strIndex > strLength) {
					return false;
				}
			} else {
				if ((strIndex >= strLength) || (ch != str.charAt(strIndex))) {
					return false;
				}
				strIndex++;
			}
		}
		return (strIndex == strLength);
	}

	class SecurityAccess {
		public void disopen() {

		}
	}
	
	public static void main(String[] args) throws Exception {
		ExcelReader reader = new ExcelReader();
		reader.directory = "/Users/Dylan/Documents/DataExchange-3.9/ExcelReader/demo/";
		reader.wildcard = "*.xls";
		reader.beginline = 2;
		reader.readExcel(reader.directory);
		// List list =
		// ExcelUtils.getInstance().readExcel2List("/Users/Dylan/Documents/DataExchange-3.9/ExcelReader/demo/aaa.xls");
		// System.out.println(list);
	}

}
