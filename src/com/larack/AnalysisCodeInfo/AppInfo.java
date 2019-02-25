package com.larack.AnalysisCodeInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

public class AppInfo {

	public String filePath;

	public String appName;

	public long totalRecords = 0;

	public TreeMap<String, Integer> resultMap;

	/**
	 * 按照value进行降序排序
	 */
	private Comparator<Map.Entry<String, Integer>> valueDownComparator = new Comparator<Map.Entry<String, Integer>>() {
		@Override
		public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
			if (o1 == null || o2 == null) {
				return 0;
			}
			return o2.getValue() - o1.getValue();
		}
	};

	public AppInfo() {

	}

	public AppInfo(String filePath, String appName, TreeMap<String, Integer> resultMap) {
		super();
		this.filePath = filePath;
		this.appName = appName;
		this.resultMap = resultMap;
	}

	public void recordCode(String key) {
		if (null == resultMap) {
			resultMap = new TreeMap<String, Integer>();
		}
		if (resultMap.get(key) == null) {
			resultMap.put(key, 1);
		} else {
			resultMap.put(key, resultMap.get(key) + 1);
			totalRecords++;
		}
	}

	public List<Map.Entry<String, Integer>> getSortList() {
		if (null == resultMap) {
			return null;
		}
		List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(resultMap.entrySet());
		Collections.sort(list, valueDownComparator);
		return list;
	}

}
