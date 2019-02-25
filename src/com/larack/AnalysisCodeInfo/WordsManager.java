package com.larack.AnalysisCodeInfo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;

/**
 * 
 * @author larack
 *
 */
public class WordsManager {

	/**
	 * 默认线程个数
	 */
	public static final int DEFAULT_THREAD_NUM = 5 * 1000;

	/**
	 * 默认文件分块大小 100M
	 */
	public static final long DEFAULT_SPLIT_SIZE = 1024 * 1024 * 1000;

	/**
	 * 忽略的文件名称
	 */
	public static final String IGNORE_FILES[] = { ".svn", ".git" };

	/**
	 * 要处理的文件
	 */
	private String fromFilePath = null;

	/**
	 * 保存結果的文件
	 */
	private String resultFilePath;

	/**
	 * 要處理的文件格式
	 */
	private String fromFileFormat = null;

	/**
	 * 分片线程表
	 */
	private Vector<CalcWordsThread> listCalcWordsThreads = null;
	private Vector<Thread> listThread = null;

	/**
	 * 线程数
	 */
	private int threadNum;

	/**
	 * 文件分割大小
	 */
	private long splitSize;

	/**
	 * 统计过的文件数
	 */
	private long totalCalcFileCount = 0;

	/**
	 * 当前处理的文件位置
	 */
	private long currentPos;

	private String searchParten = PartenUtils.PARTEN_WORDS;

	private String showParten = null;

	private long calcStartTime;

	private HashMap<String, Integer> appList;

	/**
	 * 
	 * @param fromFilePath
	 * @param resultPath
	 * @param searchParten
	 */
	public WordsManager(String fromFilePath, String resultPath, String searchParten) {
		this(fromFilePath, null, resultPath, searchParten, null);
	}

	/**
	 * 
	 * @param fromFilePath
	 * @param fromFileFormat 可以为空,表示任何格式文件
	 * @param resultPath
	 * @param searchParten
	 * @param showParten     可以为空,表示按搜索到的原始结果输出
	 */
	public WordsManager(String fromFilePath, String fromFileFormat, String resultPath, String searchParten,
			String showParten) {
		this(fromFilePath, fromFileFormat, resultPath, searchParten, showParten, DEFAULT_THREAD_NUM,
				DEFAULT_SPLIT_SIZE);
	}

	/**
	 * 
	 * @param fromFilePath   读取文件路径
	 * @param fromFileFormat 要查的文件格式
	 * @param resultPath     结果保存路径
	 * @param searchParten   搜索正则表达式
	 * @param showParten     显示结果正则表达式
	 * @param threadNum      线程数
	 * @param splitSize      文件分割大小
	 */
	public WordsManager(String fromFilePath, String fromFileFormat, String resultFilePath, String searchParten,
			String showParten, int threadNum, long splitSize) {
		// 确定线程数最小是1个
		if (threadNum < 1)
			threadNum = 1;
		// 确定线程数最大是10000个，防止内存不够用
		if (threadNum > 10000)
			threadNum = 10000;
		// 分割最小为1M大小文件
		if (splitSize < 1 * 1024 * 1024)
			splitSize = 1 * 1024 * 1024;
		// 分割最大为10M大小文件
		if (splitSize > 1024 * 1024 * 1000)
			splitSize = 1024 * 1024 * 1000;

		this.totalCalcFileCount = 0;
		this.fromFilePath = fromFilePath;
		this.resultFilePath = resultFilePath;
		this.fromFileFormat = fromFileFormat;
		this.searchParten = searchParten;
		this.showParten = showParten;
		this.threadNum = threadNum;
		this.splitSize = splitSize;
		this.currentPos = 0;
		this.listCalcWordsThreads = new Vector<CalcWordsThread>();
		this.listThread = new Vector<Thread>();

		System.out.println(">>> 1.初始化: fromFileFormat=" + fromFileFormat + ", searchParten=" + searchParten
				+ ", showParten=" + showParten + ", threadNum=" + threadNum + ", splitSize=" + splitSize);
	}

	public void calc() throws IOException {
		calcTotal();
	}

	public void calcBySubDir() throws IOException {
		calcStartTime = System.currentTimeMillis();
		File files = new File(fromFilePath);
		if (!files.exists() || !files.canRead()) {
			return;
		}
		if (null == appList) {
			appList = new HashMap<String, Integer>();
		}
		if (files.isDirectory()) {
			File[] fs = files.listFiles();
			for (File f : fs) {
				if (f.isDirectory()) {
					appList.put(f.getName(), 1);
					calc(f);
				} else {
					appList.put(f.getName(), 1);
					calc(f);
				}
			}
		} else {
			appList.put(files.getName(), 1);
			calc(files);
		}
		saveResult();
	}

	public void calcTotal() throws IOException {
		calcStartTime = System.currentTimeMillis();
		File files = new File(fromFilePath);
		if (!files.exists() || !files.canRead()) {
			return;
		}
		appList = null;
		calc(files);
		saveResult();
	}

	private void calc(File file) throws IOException {
		if (null == file) {
			return;
		}

		if (file.isFile()) {
			doFile(file);
		} else if (file.isDirectory()) {
			File[] fs = file.listFiles();
			for (File f : fs) {
				calc(f);
			}
		}
	}

	private boolean canCalcFile(File f) {
		if (null == f || !f.canRead() || !f.exists()) {
			return false;
		}
		String fp = f.getAbsolutePath();
		String[] pdirs = fp.split(File.separator);
		ArrayList<String> arrayList = new ArrayList<String>(Arrays.asList(pdirs));
		for (String ig : IGNORE_FILES) {
			if (arrayList.contains(ig)) {
				System.out.println("** ignore file " + fp);
				return false;
			}
		}
		if (f.isDirectory()) {
			return true;
		}
		if (f.isFile()) {
			if (null != fromFileFormat) {
				return fp.endsWith(fromFileFormat);
			}
			return true;
		}
		return false;
	}

	/**
	 * 分片处理
	 * 
	 * @param file
	 * @throws IOException
	 */
	private void doFile(File file) throws IOException {
		if (!canCalcFile(file)) {
			return;
		}
		totalCalcFileCount++;
		System.out.println(">>> 2.正在统计文件 " + totalCalcFileCount + ": " + file.getAbsolutePath());
		currentPos = 0;
		while (currentPos < file.length()) {
			for (int num = 0; num < threadNum; num++) {
				if (currentPos < file.length()) {
					CalcWordsThread calcWordsThread = null;

					try {
						if (currentPos + splitSize < file.length()) {
							RandomAccessFile raf = new RandomAccessFile(file, "r");
							raf.seek(currentPos + splitSize);

							int offset = 0;

							while (true) {
								char ch = (char) raf.read();
								// 是否到文件末尾，到了跳出
								if (-1 == ch) {
									currentPos = 0;
									break;
								}

								if (PartenUtils.PARTEN_WORDS.equals(searchParten)) {
									// 是否是字母和'，都不是跳出（防止单词被截断）
									if (false == Character.isLetter(ch) && '\'' != ch) {
										break;
									}
								}
								offset++;
							}

							String appname = getAppName(file.getAbsolutePath());
							calcWordsThread = new CalcWordsThread(file, currentPos, splitSize + offset, searchParten,
									showParten, appname);
							currentPos += splitSize + offset;

							raf.close();
						} else {
							String appname = getAppName(file.getAbsolutePath());
							calcWordsThread = new CalcWordsThread(file, currentPos, file.length() - currentPos,
									searchParten, showParten, appname);
							currentPos = file.length();
						}

						Thread thread = new Thread(calcWordsThread);
						thread.start();
						listCalcWordsThreads.add(calcWordsThread);
						listThread.add(thread);
					} catch (Exception e) {
//						System.out.print("Exception when init CalcWordsThread : " + e.getMessage());
//						e.printStackTrace();
					}
				}
			}

			// 判断线程是否执行完成
			while (true) {
				boolean threadsDone = true;

				for (int loop = 0; loop < listThread.size(); loop++) {
					if (listThread.get(loop).getState() != Thread.State.TERMINATED) {
						threadsDone = false;
						break;
					}
				}

				if (threadsDone) {
					break;
				}
			}
		}

	}

	private String getAppName(String path) {
		String appname = null;
		if (null != appList && null != path) {
			Set<String> keys = appList.keySet();
			Iterator<String> iterator = keys.iterator();
			while (iterator.hasNext()) {
				String key = (String) iterator.next();
				int count = appList.get(key);
				if (path.contains(key) && count > 0) {
					appname = key;
				}
			}
		}
		return appname;
	}

	/**
	 * 保存结果
	 */
	private void saveResult() {
		System.out.println(">>> 3.正在处理结果");
		if (appList == null || appList.size() == 0) {
			saveTotolResult();
		} else {
			saveAppSortList();
		}
	}

	/**
	 * 保存结果
	 */
	private void saveTotolResult() {
		System.out.println(">>> 3.正在处理结果");

		// 当分别统计的线程结束后，开始统计总数目的线程
		new Thread(() -> {

			Comparator<Map.Entry<String, Integer>> valueDownComparator = new Comparator<Map.Entry<String, Integer>>() {
				@Override
				public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
					return o2.getValue() - o1.getValue();
				}
			};

			TreeMap<String, Integer> tMap = new TreeMap<String, Integer>();

			// 使用TreeMap保证结果有序（按首字母排序）
			System.out.println("# 1. 正在按首字母排序... ");
			for (int loop = 0; loop < listCalcWordsThreads.size(); loop++) {
				AppInfo appinfo = listCalcWordsThreads.get(loop).getRecordResult();
				if (null == appinfo) {
					continue;
				}
				Map<String, Integer> hMap = appinfo.resultMap;
				if (null == hMap) {
					continue;
				}
				Set<String> keys = hMap.keySet();
				Iterator<String> iterator = keys.iterator();
				while (iterator.hasNext()) {
					String key = (String) iterator.next();
					if (key.equals(""))
						continue;
					if (tMap.get(key) == null) {
						tMap.put(key, hMap.get(key));
					} else {
						tMap.put(key, tMap.get(key) + hMap.get(key));
					}
				}
			}

			for (int loop = 0; loop < listThread.size(); loop++) {
				listThread.get(loop).interrupt();
			}

			if (tMap.size() <= 0) {
				System.out.println("** warning ** 总共查询了 " + totalCalcFileCount + " 个文件, 没有匹配到任何数据，程序退出 !!! ");
				return;
			}

			// 使用TreeMap保证结果有序（然后再按查找到的次数递减排序）
			// map转换成list进行排序
			System.out.println("# 2. 正在按统计次数排序... ");
			List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(tMap.entrySet());
			Collections.sort(list, valueDownComparator);

			System.out.println("# 3. 正在保存到文件... ");

			saveTotalResultKVtogether(list);
			saveTotalResultKVindependent(list);

			System.out.println("## done !! ");
			long end = System.currentTimeMillis();
			System.out.println("@@ 总结: 总共查询了 " + totalCalcFileCount + " 个文件, 创建 " + listCalcWordsThreads.size()
					+ " 个线程, 匹配到 " + tMap.size() + " 个单词, 耗时 " + (end - calcStartTime) / 1000.0 + " 秒!");
			return;
		}).start();
	}

	public void saveTotalResultKVindependent(List<Map.Entry<String, Integer>> list) {
		String keyFilePath = resultFilePath + "_" + "key.txt";
		String valueFilePath = resultFilePath + "_" + "value.txt";
		File keyFile = new File(keyFilePath);
		if (keyFile.exists()) {
			keyFile.delete();
		}
		File valueFile = new File(valueFilePath);
		if (valueFile.exists()) {
			valueFile.delete();
		}
		System.out.println(
				"## 正在分别保存Key和Value到文件中, Key:" + keyFile.getAbsolutePath() + "; Value:" + valueFile.getAbsolutePath());
		for (Map.Entry<String, Integer> entry : list) {
			String key = entry.getKey();
			Integer value = entry.getValue();
			saveResultToFile(keyFilePath, key + "\n");
			saveResultToFile(valueFilePath, String.valueOf(value) + "\n");
		}
	}

	public void saveTotalResultKVtogether(List<Map.Entry<String, Integer>> list) {
		File fileText = new File(this.resultFilePath);
		if (fileText.exists()) {
			fileText.delete();
		}
		System.out.println("## 正在保存结果到文件中 " + fileText.getAbsolutePath());
		for (Map.Entry<String, Integer> entry : list) {
			String key = entry.getKey();
			Integer value = entry.getValue();
			String calcResult = "字符:" + key + ",出现次数:" + value + "\n";
//			System.out.print(calcResult);
			saveResultToFile(resultFilePath, calcResult);
		}
	}

	/**
	 * 按照appList排序
	 */
	private void saveAppSortList() {
		System.out.println(">>> 3.正在处理App列表");
		Comparator<AppInfo> appDownComparator = new Comparator<AppInfo>() {
			@Override
			public int compare(AppInfo o1, AppInfo o2) {
				return (int) (o2.totalRecords - o1.totalRecords);
			}
		};

		for (int loop = 0; loop < listThread.size(); loop++) {
			listThread.get(loop).interrupt();
		}

		List<AppInfo> alist = new ArrayList<>();
		// 使用TreeMap保证结果有序（按首字母排序）
		System.out.println("# 1. 正在按首字母排序... ");
		for (int loop = 0; loop < listCalcWordsThreads.size(); loop++) {
			AppInfo appInfo = listCalcWordsThreads.get(loop).getRecordResult();
			alist.add(appInfo);
		}

		Collections.sort(alist, appDownComparator);

		if (alist.size() <= 0) {
			System.out.println("** warning ** 总共查询了 " + totalCalcFileCount + " 个文件, 没有匹配到任何数据，程序退出 !!! ");
			return;
		}

		System.out.println("# 2. 正在保存到文件... ");

		File fileText = new File(this.resultFilePath);
		if (fileText.exists()) {
			fileText.delete();
		}
		System.out.println("## 正在保存结果到文件中 " + fileText.getAbsolutePath());

		long totalWords = getTotalWords(alist);

		if (totalWords <= 0) {
			System.out.println("** warning ** 总共查询了 " + totalCalcFileCount + " 个文件, 没有匹配到任何数据，程序退出 !!! ");
			return;
		}

		saveResultKVtogether(alist);// 合并保存
		saveResultKVindependent(alist);// 单独保存K/V

		System.out.println("## done !! ");
		long end = System.currentTimeMillis();

		System.out.println("@@ 总结: 总共查询了 " + totalCalcFileCount + " 个文件, 创建 " + listCalcWordsThreads.size()
				+ " 个线程, 匹配到 " + totalWords + " 个单词, 耗时 " + (end - calcStartTime) / 1000.0 + " 秒!");
		return;
	}

	public long getTotalWords(List<AppInfo> alist) {
		long totalWords = 0;
		for (AppInfo info : alist) {
			totalWords += info.totalRecords;
		}
		return totalWords;
	}

	public void saveResultKVindependent(List<AppInfo> alist) {
		String keyFilePath = resultFilePath + "_" + "key.txt";
		String valueFilePath = resultFilePath + "_" + "value.txt";
		File keyFile = new File(keyFilePath);
		if (keyFile.exists()) {
			keyFile.delete();
		}
		File valueFile = new File(valueFilePath);
		if (valueFile.exists()) {
			valueFile.delete();
		}
		System.out.println(
				"## 正在分别保存Key和Value到文件中, Key:" + keyFile.getAbsolutePath() + "; Value:" + valueFile.getAbsolutePath());
		int index = 0;
		for (AppInfo info : alist) {
			index++;
			String appResult = index + ".appName:" + info.appName + "\n";
			saveResultToFile(keyFilePath, appResult);
			saveResultToFile(valueFilePath, appResult);
			List<Map.Entry<String, Integer>> list = info.getSortList();
			if (null == list) {
				continue;
			}
			for (Map.Entry<String, Integer> entry : list) {
				String key = entry.getKey();
				Integer value = entry.getValue();
				saveResultToFile(keyFilePath, key + "\n");
				saveResultToFile(valueFilePath, String.valueOf(value) + "\n");
			}
		}
	}

	public void saveResultKVtogether(List<AppInfo> alist) {
		File fileText = new File(this.resultFilePath);
		if (fileText.exists()) {
			fileText.delete();
		}
		System.out.println("## 正在保存结果到文件中 " + fileText.getAbsolutePath());
		int index = 0;
		for (AppInfo info : alist) {
			index++;
			String appResult = index + ".appName:" + info.appName + "\n";
			saveResultToFile(resultFilePath, appResult);
			List<Map.Entry<String, Integer>> list = info.getSortList();
			if (null == list) {
				continue;
			}
			for (Map.Entry<String, Integer> entry : list) {
				String key = entry.getKey();
				Integer value = entry.getValue();
				String calcResult = "\t字符:" + key + ",出现次数:" + value + "\n";
//				System.out.print(calcResult);
				saveResultToFile(resultFilePath, calcResult);
			}
		}
	}

	/**
	 * 结果写入文件
	 * 
	 * @param strFilename
	 * @param strBuffer
	 */
	public static void saveResultToFile(final String strFilename, final String strBuffer) {
		try {
			// 创建文件对象
			File fileText = new File(strFilename);
			// 向文件写入对象写入信息
			FileWriter fileWriter = new FileWriter(fileText, true);
			// 写文件
			fileWriter.write(strBuffer);
			// 关闭
			fileWriter.close();
		} catch (IOException e) {
			System.out.print("IOException when saveResultToFile " + strFilename);
			e.printStackTrace();
		}
	}
}