/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/

package setest;

import java.io.IOException;

import java.util.Iterator;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import Comm.ConstVar;
import FormatStorage.FieldMap;
import FormatStorage.Head;
import FormatStorage.Unit;
import FormatStorage.FieldMap.Field;
import StorageEngineClient.FormatStorageInputFormat;
import StorageEngineClient.FormatStorageOutputFormat;
import StorageEngineClient.FormatStorageSplit;


public class FormatStorageMR
{
	public static final Log LOG = LogFactory.getLog("FormatStorageMR");
	
	public static class FormatStorageTestMapper extends MapReduceBase implements
			Mapper
	{
		public void configure(JobConf job)
		{
		}

		public void map(Object key, Object value, OutputCollector output,
				Reporter reporter) throws IOException
		{
			LongWritable lw = new LongWritable(0);			
		
			output.collect(lw, value);
		}

		public void close()
		{

		}

	}

	public static class FormatStorageTestReducer extends MapReduceBase implements Reducer
	{

		public void configure(JobConf job)
		{
		}

		public void reduce(Object key, Iterator values, OutputCollector output, Reporter reporter) throws IOException
		{
			LongWritable lw = new LongWritable(0);
			while (values.hasNext())
			{
				output.collect(lw, (Writable) values.next());
			}
		}

		public void close()
		{

		}
	}

	public static int readFormatFile(JobConf conf, String inputPath, int lineNum) throws Exception
	{

		RecordReader<WritableComparable, Writable> currRecReader;

		

		conf.set("mapred.input.dir", inputPath);

		InputFormat inputFormat = new FormatStorageInputFormat();
		InputSplit[] inputSplits = inputFormat.getSplits(conf, 1);
		if (inputSplits.length == 0)
		{
			System.out.println("inputSplits is empty");
			return -1;
		}

		currRecReader = inputFormat.getRecordReader(inputSplits[0],
													conf,
													Reporter.NULL);

		WritableComparable key;
		Writable value;

		key = currRecReader.createKey();
		value = currRecReader.createValue();

		int num = 0;

		while (true)
		{
			boolean ret = currRecReader.next(key, value);
			if (ret)
			{
				Text Line = (Text) key;
				System.out.println(Line.toString());
				num++;
				if (num >= lineNum)
					break;

			} else
				break;
		}

		return 0;
	}

	public static void initHead(Head head)
	{		
		short fieldNum = 7;
		FieldMap fieldMap = new FieldMap();
        fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)1));
        fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)3));
        fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)5));
        fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)7));
        fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)9));
        fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)11));
        fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)13));
        
		head.setFieldMap(fieldMap);
		
		head.setVar(ConstVar.VarFlag);
	}
	
	public static void showSplits(JobConf conf) throws IOException
	{
		FormatStorageInputFormat inputFormat = new FormatStorageInputFormat();
		InputSplit[] splits = inputFormat.getSplits(conf, 1);
		int size = splits.length;
		System.out.println("getSplits return size:"+size);
		for(int i = 0; i < size; i++)
		{
			FormatStorageSplit split = (FormatStorageSplit) splits[i];
			System.out.printf("split:"+i+"offset:"+split.getStart() + "len:"+split.getLength()+"path:"+conf.get(ConstVar.InputPath) +"beginLine:"+split.getBeginLine()+"endLine:"+split.getEndLine() + "\n");			
		}
	}
	public static void main(String[] args) throws Exception
	{

		if (args.length != 2)
		{
			System.out.println("FormatStorageMR <input> <output>");
			System.exit(-1);
		}

		JobConf conf = new JobConf(FormatStorageMR.class);

		conf.setJobName("FormatStorageMR");

		conf.setNumMapTasks(1);

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Unit.Record.class);

		conf.setMapperClass(FormatStorageTestMapper.class);
		conf.setReducerClass(FormatStorageTestReducer.class);

		conf.setInputFormat(FormatStorageInputFormat.class);
		conf.setOutputFormat(FormatStorageOutputFormat.class);
		conf.set("mapred.output.compress", "flase");

		Head head = new Head();
		initHead(head);
		
		head.toJobConf(conf);
		
				
		FileInputFormat.setInputPaths(conf, args[0]);
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(conf, outputPath);
	
		FileSystem fs = outputPath.getFileSystem(conf);
		fs.delete(outputPath, true);

		JobClient jc = new JobClient(conf);
		RunningJob rj = null;
		rj = jc.submitJob(conf);

		String lastReport = "";
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyy-MM-dd hh:mm:ss,SSS");
		long reportTime = System.currentTimeMillis();
		long maxReportInterval = 3 * 1000; 
		while (!rj.isComplete())
		{
		    try
			{
				Thread.sleep(1000);
			} catch (InterruptedException e)
			{
			}

			int mapProgress = Math.round(rj.mapProgress() * 100);
			int reduceProgress = Math.round(rj.reduceProgress() * 100);

			String report = " map = " + mapProgress + "%,  reduce = "
					+ reduceProgress + "%";

			if (!report.equals(lastReport)
					|| System.currentTimeMillis() >= reportTime
							+ maxReportInterval)
			{

				String output = dateFormat.format(Calendar.getInstance()
						.getTime())
						+ report;
				System.out.println(output);
				lastReport = report;
				reportTime = System.currentTimeMillis();
			}
		}


		
		System.exit(0);
		
	}

}
