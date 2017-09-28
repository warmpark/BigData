package com.t3q.hadoop.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 * Could not locate executable null\bin\winutils.exe 에러 시. 
 * https://stackoverflow.com/questions/35652665/java-io-ioexception-could-not-locate-executable-null-bin-winutils-exe-in-the-ha
 * 
 * 
 * @author warmpark
 * 
 *  hdfs dfs -help
 *  hdfs dfs -ls -R /
 *
 */
public class HDFSClient {

	public static void main(String[] args) {
		
		
		try {
			
			//System.setProperty("hadoop.home.dir", "PATH/TO/THE/DIR");
			
			FileSystem hdfs = null;
			Configuration config = new Configuration();

			File hdfsSiteXml = new HDFSClient().getFile("conf/hadoop/hdfs-site.xml");
			config.addResource(new Path(hdfsSiteXml.getPath()));
			//config.addResource(new Path("/JavaOneShot/IDE/64/workspace/BigData/src/main/resources/conf/hadoop/hdfs-site.xml"));

			
			//CASE #1 core-site.xml fs.defaultFS = hdfs://big-cluster, 8020 = namenode port
			//hdfs = FileSystem.get(new URI("hdfs://big-cluster:8020"), config, "hdfs");
			
			//CASE #2 -- local
			hdfs = FileSystem.get(FileSystem.getDefaultUri(config), config ,"hdfs");

			System.out.println("Home Path : " + hdfs.getHomeDirectory());
			System.out.println("Work Path : " + hdfs.getWorkingDirectory());
			
			
			
			// A-1. 파일 생성 
			Path filenamePath = new Path("/tmp/hello.txt");
			System.out.println("File Exists : " + hdfs.exists(filenamePath));

			if (hdfs.exists(filenamePath)) {
				hdfs.delete(filenamePath, true);
			}

			FSDataOutputStream out = hdfs.create(filenamePath);
			out.writeUTF("Hello, world!\n");
			out.close();

			FSDataInputStream in = hdfs.open(filenamePath);
			String messageIn = in.readUTF();
			System.out.print(messageIn);
			in.close();
			
			// A-2 파일생성 복사 (local to hads)
			Path fromLocalPath = new Path("/JavaOneShot/IDE/64/workspace/BigData/src/main/resources/conf/hadoop/hdfs-site.xml");
			Path toHdfsPath = new Path("/tmp/hdfs-site.xml");
			hdfs.copyFromLocalFile(fromLocalPath,toHdfsPath);

		
			//B.  읽기
			new HDFSClient().readFromHDFS(config, hdfs,toHdfsPath );
			hdfs.close();
			

		} catch (Exception e) {
			e.printStackTrace();
		}
		

	}

	private  File getFile(String fileInClasspath) {

		StringBuilder result = new StringBuilder("");

		// Get file from resources folder
		ClassLoader classLoader = getClass().getClassLoader();
		URL uri = classLoader.getResource(fileInClasspath);
		File file = new File(uri.getFile());

		try {
			Scanner scanner = new Scanner(file);
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				result.append(line).append("\n");
			}

			scanner.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println(result.toString());
		return file;

	}
	
	/**
	 * hdfs dfs -cat /tmp/hdfs-site.xml
	 * @param config
	 * @param hdfs
	 * @param path
	 */
	public void readFromHDFS(Configuration config, FileSystem hdfs, Path path) {
		try {
//			Configuration config = new Configuration();
//			config.addResource(new Path("/JavaOneShot/IDE/64/workspace/BigData/src/main/resources/conf/hadoop/hdfs-site.xml"));
//			FileSystem hdfs = FileSystem.get(new URI("hdfs://big-cluster:8020"), config, "hdfs");
//			Path path = new Path("/tmp/hdfs-site.xml.org");

			
			BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(path)));
			
			String line;
			line = br.readLine();
			while (line != null) {
				System.out.println(line);
				line = br.readLine();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
