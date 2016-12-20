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

package protobuf.mapred;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.*;

 
public class ProtobufOutputFormat<K, V> extends FileOutputFormat<K, V> {

    protected static class ProtobufRecordWriter<K, V> implements
	RecordWriter<K, V> {

	    protected recordio.RecordWriter recordWriter;
	    protected DataOutputStream out;

	    public ProtobufRecordWriter(DataOutputStream out) {
		this.out = out;
		this.recordWriter = new recordio.RecordWriter(this.out);
	    }

	    
	    private void writeProtobufObject(BytesWritable value) throws IOException {
		    recordWriter.write(value.getBytes(),0,value.getLength());
		}

	    public synchronized void write(K key, V value) throws IOException {
		boolean nullValue = value == null || value instanceof NullWritable;
		if (nullValue) {
		    return;
		} else {
		    if (value instanceof BytesWritable) {
			writeProtobufObject((BytesWritable) value);
		    }
		}
	    }

	    public synchronized void close(Reporter reporter) throws IOException {
	    	recordWriter.flush();
	    	out.close();
	    }

    }

    public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
	    String name, Progressable progress) throws IOException {

	Path file = FileOutputFormat.getTaskOutputPath(job, name);
	FileSystem fs = file.getFileSystem(job);
	FSDataOutputStream fileOut = fs.create(file, progress);
	return new ProtobufRecordWriter<K, V>(fileOut);
    }
}
