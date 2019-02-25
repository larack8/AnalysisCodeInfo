package com.larack.AnalysisCodeInfo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 * @author larack
 *
 */
public class CalcWordsThread implements Runnable {

	private FileChannel fileChannel = null;

	private FileLock lock = null;

	private MappedByteBuffer mbBuf = null;

	private AppInfo appInfo = null;

	private String searchParten = "[^a-zA-Z']+";

	private String showParten = null;

	/**
	 * 
	 * @param file
	 * @param start
	 * @param size
	 * @param searchParten
	 * @param showParten
	 * @param showParten
	 */
	@SuppressWarnings("resource")
	public CalcWordsThread(File file, long start, long size, String searchParten, String showParten, String appName)
			throws IOException {
//		System.out.println("** CalcWordsThread: " + file.getAbsolutePath() + ", start=" + start + ", size=" + size);
		try {
			this.searchParten = searchParten;
			this.showParten = showParten;
			this.appInfo = new AppInfo(file.getAbsolutePath(), appName, null);

			file.getAbsolutePath();
			// 得到当前文件的通道
			fileChannel = new RandomAccessFile(file, "rw").getChannel();
			// 锁定当前文件的部分
			lock = fileChannel.lock(start, size, false);
			// 对当前文件片段建立内存映射，如果文件过大需要切割成多个片段
			mbBuf = fileChannel.map(FileChannel.MapMode.READ_ONLY, start, size);
			// 创建HashMap实例存放处理结果
		} catch (Exception e) {
			System.out.print("Exception when init CalcWordsThread : " + e.getMessage());
//			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		String str = Charset.forName("UTF-8").decode(mbBuf).toString();
		str = str.toLowerCase();
		String reg = searchParten;
		Pattern pattern = Pattern.compile(reg);
		Matcher matcher = pattern.matcher(str);
		while (matcher.find()) {
//			System.out.println(matcher.group());
			// System.out.println(matcher.group(1));
			String key = matcher.group();

			if (null != showParten) {
				Pattern showpattern = Pattern.compile(showParten);
				Matcher showmatcher = showpattern.matcher(key);
				if (showmatcher.find()) {
					String showkey = showmatcher.group();
					recordWords(showkey);
				}
			} else {
				recordWords(key);
			}

		}

		try {
			// 释放文件锁
			lock.release();
			// 关闭文件通道
			fileChannel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// System.out.println("finish filePath: " + filePath);
		return;
	}

	private void recordWords(String key) {
		if (null == appInfo) {
			appInfo = new AppInfo();
		}
		appInfo.recordCode(key);
	}

	// 获取当前线程的执行结果
	public AppInfo getRecordResult() {
		return appInfo;
	}
}