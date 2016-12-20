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

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;




public class ProtobufRecordReader implements RecordReader<LongWritable, BytesWritable> {

    private static final Log LOG = LogFactory.getLog(ProtobufRecordReader.class);

    private recordio.RecordReader recordReader;

    private long start;
    private long end;
    private long splitLength;
    private long pos;
    private FSDataInputStream in;
    protected Configuration conf;

    public ProtobufRecordReader(Configuration conf, FileSplit split) throws IOException {

	this.conf = conf;

	start = split.getStart();
        splitLength = split.getLength();
	end = start + splitLength;

	final Path file = split.getPath();

	FileSystem fs = file.getFileSystem(conf);
	in = fs.open(split.getPath());

	if (start != 0) 
	{
	    in.seek(start);
	}

        recordReader = new recordio.RecordReader(in);
    }

    public LongWritable createKey() {
	return new LongWritable();
    }

    public BytesWritable createValue() {
	return new BytesWritable();
    }

        
    public long getPos(){
	return pos;
    }

    
    public synchronized boolean next(LongWritable key, BytesWritable value) throws IOException {

        if(recordReader.isBlockConsumed(splitLength)){
            LOG.info("Consumed all the split");
            key = null;
            value = null;
            return false; 
        }
	recordio.RecordReader.Buffer data = recordReader.read();
	if(data == null){
            LOG.info("get EOF, consumed all the file");
            key = null;
            value = null;
            return false; 
        }
        pos = recordReader.getConsumedBytes() + start;
	key.set(pos);
        value.set(data.buffer,data.offset,data.length);
	return true;
    }



    

    public float getProgress() {
	if (start == end) {
	    return 0.0f;
	} else {
	    return Math.min(1.0f, (pos - start) / (float) (end - start));
	}
    }

    public synchronized void close() throws IOException {
	if (in != null) {
	    in.close();
	}
    }
}

