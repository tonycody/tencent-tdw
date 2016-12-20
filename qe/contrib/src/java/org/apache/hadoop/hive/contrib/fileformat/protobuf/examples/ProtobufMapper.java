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
package protobuf.examples;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;

import protobuf.mapred.ProtobufInputFormat;
import protobuf.mapred.ProtobufRecordReader;


public class ProtobufMapper extends MapReduceBase implements
		Mapper<LongWritable, BytesWritable, Text, IntWritable> {
	private static final Log LOG = LogFactory.getLog(ProtobufMapper.class);

	public void map(LongWritable key, BytesWritable value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		LOG.info("In Mapper Get Data: " + value.toString());
		
			
		int bufferSize = value.getLength();
		byte buffer[] = new byte[bufferSize];
		System.arraycopy(value.getBytes(),0,buffer,0,bufferSize);
		
		
		output.collect(new Text("msg.getEmail()"), new IntWritable(1));
	}
}
