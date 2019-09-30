package com.datasphere.proc;

import java.util.ArrayList;
import java.util.List;

public class ParserMergeTree {

	static String sql1 = "CollapsingMergeTree(EventDate, (CounterID, EventDate, intHash32(UniqID), VisitID), 8192, Sign)";
	static String sql2 = "ENGINE = MergeTree(date, id, 8192)";
	static String sql3 = "ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192";
	static String sql4 = "ORDER BY (CounterID, EventDate, intHash32(UserID)) ";
	// static Pattern pattern1 = Pattern.compile(".*\\s+([^\\s\\)]+)\\)$");

	/**
	 * 使用正则表达式提取中括号中的内容
	 * 
	 * @param sql
	 * @return
	 */
	public static List<String> parser(String str) {
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
		List<String> list = parser(sql1);
		for(String s : list) {
			System.out.println(s);
		}
	}
}
