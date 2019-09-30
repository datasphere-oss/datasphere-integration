package com.datasphere.proc;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.math.NumberUtils;


public class ParserMergeTree {
	private final static String SPLIT_STRING = " ";
	// static Pattern pattern1 = Pattern.compile(".*\\s+([^\\s\\)]+)\\)$");

	static List<String> engineList = null;
	static {
		engineList = new ArrayList<>();
		engineList.add("COLLAPSINGMERGETREE");
		engineList.add("SUMMINGMERGETREE");
		engineList.add("COLLAPSINGMERGETREE");
		engineList.add("VERSIONEDCOLLAPSINGMERGETREE");
		engineList.add("REPLICATEDMERGETREE");
		engineList.add("REPLACINGMERGETREE");
		engineList.add("REPLICATEDCOLLAPSINGMERGETREE");
		engineList.add("AGGREGATINGMERGETREE");
		engineList.add("GRAPHITEMERGETREE");
		engineList.add("GRAPHITEMERGETREE");
		engineList.add("MERGETREE");
		engineList.add("ORDER");
		engineList.add("PARTITION");
		engineList.add("SAMPLE");
		engineList.add("INTHASH32");
		engineList.add("TOYYYYMM");
		engineList.add("SETTINGS");
		engineList.add("BY");
		engineList.add("INDEX_GRANULARITY");
		engineList.add("TODATE");
		engineList.add("=");
		engineList.add("8192");
		engineList.add("(");
		engineList.add(")");
		engineList.add("ENGINE");
	}
	public ParserMergeTree() {
		
	}
	public static List<String> parser(String str){
		List<String> list = new ArrayList<>();
		if(str.indexOf(SPLIT_STRING)>-1) {
			String[] keys = str.split(SPLIT_STRING);
			if(keys.length > 1) {
				for(int i = 0; i < keys.length; i++) {
					String[] tmp = keys[i].split(",");
					for(int j = 0; j < tmp.length; j++) {
						String key = tmp[j];
						String[] tmp2 = key.split("\\(");
						for(int k = 0; k < tmp2.length; k++) {
							String k2 = tmp2[k];
							String[] tmp3 = k2.split("\\)");
							for(int m = 0; m < tmp3.length; m++) {
								String k3 = tmp3[m];
								String[] tmp4 = k3.split("=");
								for(int n = 0; n < tmp4.length; n++) {
									String k4 = tmp4[n];
									if(k4.trim().length() > 0 && !engineList.contains(k4.toUpperCase())) {
										if(!"substring".equalsIgnoreCase(k4) && !";".equalsIgnoreCase(k4.trim()) && !NumberUtils.isNumber(k4) && !"\n".equalsIgnoreCase(k4)) {
											System.out.println(k4);
											list.add(k4);
										}
									}
								}
								
							}
						}
					}
				}
			}
		}
		return list;
	}
	/**
	 * 使用正则表达式提取中括号中的内容
	 * 
	 * @param sql
	 * @return
	 */
	public static List<String> parser1(String str) {
		List<String> list = new ArrayList<String>();
		ArrayList<String> word = new ArrayList<String>();
		int mm = 0, n = 0;
		int count = 0;
		for (int i = 0; i < str.length(); i++) {
			if (str.charAt(i) == '(') {
				if (count == 0) {
					mm = i;
				}
				count++;
			}
			if (str.charAt(i) == ')') {
				count--;
				if (count == 0) {
					n = i;
					word.add(str.substring(mm, n + 1));
				}
			}
		}
		for (String a : word) {
			if(a.startsWith("(")) {
				a = a.substring(1);
			}
			if(a.endsWith(")")) {
				a = a.substring(0,a.length()-1);
			}
			String[] aa = a.split(",");
			String s = null;
			if(aa.length>1) {
				for(int i = 0;i<aa.length;i++) {
					if(aa[i].indexOf("(")>-1 && aa[i].indexOf(")")>-1) {
						aa[i] = aa[i].substring(aa[i].indexOf("(")+1,aa[i].indexOf(")"));
						s = filter(aa[i]);
						if(s != null) {
							list.add(s);
						}
					}else if(aa[i].indexOf("(")>-1) {
						aa[i] = aa[i].substring(aa[i].indexOf("(")+1);
						s = filter(aa[i]);
						if(s != null) {
							list.add(s);
						}
					}else if(aa[i].indexOf(")")>-1) {
						aa[i] = aa[i].substring(0,aa[i].indexOf(")"));
						s = filter(aa[i]);
						if(s != null) {
							list.add(s);
						}
					}else {
						s = filter(aa[i]);
						if(s != null) {
							list.add(s);
						}
					}
				}
			}else {
				s = filter(a);
				if(s != null) {
					list.add(s);
				}
			}
		}
		return list;
	}

	private static String filter(String str) {
		String s = str.trim();
		List<String> list = new ArrayList<>();
		list.add("8192");
		list.add("Sign");
		
		if(list.contains(s) || s.length()==0) {
			return null;
		}
		return s;
	}


	public static void main(String[] args) throws Exception {
		String[] sql = new String[10];
		sql[0] = "CollapsingMergeTree(EventDate, (CounterID, EventDate, intHash32(UniqID), VisitID), 8192, Sign)";
		sql[1] = "ENGINE = MergeTree(date, id, 8192)";
		sql[2] = "ENGINE= MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192";
		sql[3] = "ORDER BY (CounterID, EventDate, intHash32(UserID)) ";
		sql[4] = "ENGINE = MergeTree(JJDB_CREATE_TIME,JJDB_JJDBH,8192)";
		sql[5] = "ENGINE = SummingMergeTree() ORDER BY key";
		sql[6] = "ENGINE = CollapsingMergeTree(Sign) ORDER BY UserID";
		sql[7] = "ENGINE = VersionedCollapsingMergeTree(Sign, Version) PARTITION BY createtime ORDER BY UserID";
		sql[8] = "engine=MergeTree() partition by toDate(DJSJ) order by CBBH SETTINGS index_granularity=8192";
		sql[9] = "engine=MergeTree() partition by substring(rodpssj,1,7) order by rodpssj SETTINGS index_granularity=8192 ;";
		for(int i = 8; i< 10; i++) {
			parser(sql[i]);
		}
	}
}
