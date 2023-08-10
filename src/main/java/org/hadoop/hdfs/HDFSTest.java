package org.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class HDFSTest {
	
	private static final String FS_DEFAULT_NAME_KEY = "fs.default.name";
	private static final String FS_DEFAULT_NAME_VALUE = "hdfs://192.168.130.128:9000";
	
	
	/**
	 * 创建目录
	 */
	@Test
	public void mkdir() {
		Configuration cfg = new Configuration();
		cfg.set(FS_DEFAULT_NAME_KEY, FS_DEFAULT_NAME_VALUE);
		try (FileSystem fs = FileSystem.get(cfg)) {
			boolean mkdirs = fs.mkdirs(new Path("hdfs:/mydir"));
			System.out.println(mkdirs);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 创建文件并写入内容
	 */
	@Test
	public void create() {
		Configuration cfg = new Configuration();
		// 设置hdfs地址
		cfg.set(FS_DEFAULT_NAME_KEY, FS_DEFAULT_NAME_VALUE);
		// 去掉fileSystem 文件系统实例
		try (// 创建文件
		     FileSystem fs = FileSystem.get(cfg);
		     // 打开输入流
		     FSDataOutputStream out = fs.create(new Path("hdfs:/file.txt"))) {
			out.write("hello hadoop, this is file".getBytes());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 上传文件并显示进度
	 * create的重构方法中，有一个 progress() 的回调函数，使用该方法可以得知数据被写入节点的速度。
	 */
	@Test
	public void progress() {
		Configuration cfg = new Configuration();
		cfg.set(FS_DEFAULT_NAME_KEY, FS_DEFAULT_NAME_VALUE);
		java.nio.file.Path path = Paths.get("D:/zip/ibm-semeru-open-jdk_x64_linux_8.tar.gz");
		try (BufferedInputStream in = new BufferedInputStream(Files.newInputStream(path));
		     FileSystem fs = FileSystem.get(cfg);
		     FSDataOutputStream out = fs.create(new Path("hdfs:/test1.tar.gz"), () -> System.out.print("."))) {
			IOUtils.copyBytes(in, out, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
	/**
	 * 删除文件
	 */
	@Test
	public void deleteFile() {
		Configuration cfg = new Configuration();
		cfg.set(FS_DEFAULT_NAME_KEY, FS_DEFAULT_NAME_VALUE);
		Path path = new Path("hdfs:/test1.tar.gz");
		try (FileSystem fs = FileSystem.get(cfg)){
			boolean b = fs.deleteOnExit(path);
			System.out.println(b);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 遍历文件树
	 */
	@Test
	public void listFiles() {
		Configuration cfg = new Configuration();
		cfg.set(FS_DEFAULT_NAME_KEY, FS_DEFAULT_NAME_VALUE);
		try (FileSystem fs = FileSystem.get(cfg)){
			// 遍历主目录
			FileStatus[] fsStatus = fs.listStatus(new Path("hdfs:/"));
			Arrays.stream(fsStatus).forEach(v -> showDir(fs, v));
		}catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	private void showDir(FileSystem fs, FileStatus fileStatus) {
		Path path = fileStatus.getPath();
		System.out.println(path);
		try {
			// 如果是目录，则进行递归
			if (fileStatus.isDirectory()) {
				FileStatus[] fileStatuses = fs.listStatus(path);
				Arrays.stream(fileStatuses).forEach(v -> showDir(fs, v));
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
}
