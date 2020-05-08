package dataclean.interaction;

import dataclean.fileprocess.EntryHandler;
import dataclean.fileprocess.FileOperator;
import dataclean.fileprocess.XmlProcess;
import dataclean.personalclass.PersonalClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * xml文件处理, MapReduce 实现部分
 */
public class Client {
	private String globalHdfsPath;
	private String localFilePath;
	private String profilePath;
	private String jarPath;
	private String inputFilePath;
	private String outputFilePath;
	private String profile;
	private String key;

	/**
	 * 实例化Client时需要输入 environment.xml 文件的绝对路径, 进行解析
	 *
	 * @param Path environment.xml 文件的绝对路径
	 * @throws IOException 文件读写异常时抛出IO异常
	 */
	public Client(String Path) throws IOException, IllegalArgumentException {
		Map<String, String> environmentPath = XmlProcess.getPath(Path);
		globalHdfsPath = environmentPath.get("globalHdfsPath");
		localFilePath = environmentPath.get("localFilePath");
		profilePath = environmentPath.get("profilePath");
		jarPath = environmentPath.get("jarPath");
		inputFilePath = environmentPath.get("inputFilePath");
		outputFilePath = environmentPath.get("outputFilePath");
		key = environmentPath.get("key");
		init();
	}

	/**
	 * 初始化文件并且设置条目的属性, 在hdfs上检测是否存在输入文件路径和输出文件路径, 如果存在抛出错误参数异常
	 *
	 * @throws IOException              上传源文件或者读入配置文件出错时抛出IO异常
	 * @throws IllegalArgumentException 如果hdfs上存在输入文件或者输出文件路径抛出错误参数异常
	 */
	private void init() throws IOException, IllegalArgumentException {
		if (FileOperator.testExit(globalHdfsPath, inputFilePath)) {
			FileOperator.rm(globalHdfsPath, inputFilePath);
			System.out.println("输入文件路径已存在, 请删除该文件或选择新的文件夹");
			throw new IllegalArgumentException();
		}
		if (FileOperator.testExit(globalHdfsPath, outputFilePath)) {
			FileOperator.rmdir(globalHdfsPath, outputFilePath);
			System.out.println("输出文件路径已存在, 请删除该文件或者选择新的文件夹");
			throw new IllegalArgumentException();
		}
		FileOperator.putFileToHDFS(globalHdfsPath, localFilePath, inputFilePath);
		FileInputStream is = new FileInputStream(new File(profilePath));
		profile = new String(is.readAllBytes(), "UTF-8");
	}

	/**
	 * 配置job, 定义Map方法, Reduce方法, 启动MapReduce 程序
	 *
	 * @throws InterruptedException MapReduce 程序运行时出错抛出中断异常
	 */
	public void run() throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		conf.set("personal_profile", profile);
		conf.set("key", key);
		Job job = Job.getInstance(conf, "DataClean");
		job.setJar(jarPath);
		//Map设置
		job.setMapperClass(DataCleanMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PersonalClass.class);
		//Reduce设置
		job.setReducerClass(DataCleanReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		//多路输出设置
		MultipleOutputs.addNamedOutput(job, "BaseOnKey", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "All", TextOutputFormat.class, NullWritable.class, Text.class);
		//输入设置
		job.setInputFormatClass(TextInputFormat.class);
		//取消part-r-00000形式文件输出
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		//设置输入文件路径和输出文件路径
		FileInputFormat.addInputPath(job, new Path(inputFilePath));
		FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(outputFilePath));

		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}

	static class DataCleanMapper extends Mapper<LongWritable, Text, Text, PersonalClass> {
		PersonalClass entryTable;

		/**
		 * 根据配置文件初始化Mapper中的自定义类, 完成类型, 格式, 顺序等基本设置
		 *
		 * @param profile
		 */
		protected void setProfile(String profile) {
			SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
			SAXParser saxParser;
			try {
				saxParser = saxParserFactory.newSAXParser();
				InputStream is = new ByteArrayInputStream(profile.getBytes());
				EntryHandler handler = new EntryHandler();
				saxParser.parse(is, handler);
				Map<String, String> type = handler.getTuples();
				Map<String, String> format = handler.getFormat();
				ArrayList<String> order = handler.getOrder();
				entryTable = new PersonalClass(type);
				entryTable.setFormat(format);
				entryTable.setOrder(order);
			} catch (SAXException | ParserConfigurationException | IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String profile = conf.get("personal_profile");
			setProfile(profile);
			String sortKey = conf.get("key");
			String keyValues = value.toString();
			List<String> keyValue = new ArrayList<>(Arrays.asList(keyValues.split(",")));
			entryTable.setValues(keyValue);
			Text keyToWrite = new Text();
			keyToWrite.set(entryTable.keyWords.get(sortKey).toString());
			context.write(keyToWrite, entryTable);
		}
	}

	static class DataCleanReducer extends Reducer<Text, PersonalClass, NullWritable, Text> {
		Text txtKey = new Text();
		private MultipleOutputs<NullWritable, Text> mos;

		@Override
		protected void setup(Context context) {
			mos = new MultipleOutputs<>(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}

		@Override
		/**
		 * 一个reduce输出到一个文件内, 完成多路输出
		 */
		protected void reduce(Text key, Iterable<PersonalClass> values, Context context) throws IOException, InterruptedException {
			for (PersonalClass value : values) {
				if (value.check()) {
					txtKey.set(value.toString());
					mos.write("BaseOnKey", NullWritable.get(), txtKey, key.toString() + "/" + key.toString());
					mos.write("All", NullWritable.get(), txtKey);
				}
			}
		}
	}
}
