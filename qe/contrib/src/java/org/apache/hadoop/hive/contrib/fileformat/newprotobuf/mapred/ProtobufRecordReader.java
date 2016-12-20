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

package newprotobuf.mapred;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.google.protobuf.CodedInputStream;




public class ProtobufRecordReader implements RecordReader<LongWritable, BytesWritable> {
	
	public static enum Counter {
		BADFORMAT_FILE_COUNT
    }

    private static final Log LOG = LogFactory.getLog(ProtobufRecordReader.class);

    private recordio.RecordReader recordReader;
    private CompressionCodecFactory compressionCodecs = null;
    
    private int size;
    private long start;
    private long end;
    private long splitLength;
    private long pos;
    private InputStream in;
    protected Configuration conf;
	byte[] buffer = new byte[4096];
	private Path file;
	private Reporter reporter;
	private boolean skipbad = false;

    public ProtobufRecordReader(Configuration conf, FileSplit split, Reporter reporter) throws IOException {

	    this.conf = conf;

	    start = split.getStart();
	    pos = start;
        splitLength = split.getLength();
	    end = start + splitLength;

	    file = split.getPath();

	    FileSystem fs = file.getFileSystem(conf);
	    in = fs.open(split.getPath());
	    compressionCodecs = new CompressionCodecFactory(conf);
	    final CompressionCodec codec = compressionCodecs.getCodec(file);
	    if(codec != null){
	    	in  = codec.createInputStream(in);
	    	end = Long.MAX_VALUE;
	    }
	    this.reporter = reporter;
	    skipbad = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEPBBADFILESKIP);
	    LOG.info("Skip bad is set to " + skipbad);
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

	private boolean readLittleEndianInt(InputStream din) throws IOException{
		final int b1 = din.read();
		if(b1 == -1)
			return true; 
		
		final int b2 = din.read();
		if(b2 == -1){
			LOG.info("Parse the pbfile error:" + file.toUri().toString());
     	    LOG.info("Reading b2");
     	    if(skipbad){
     	    	LOG.info("Skip the bad file");
     	    	reporter.incrCounter(Counter.BADFORMAT_FILE_COUNT, 1);
     	    	return true;
     	    }
     	    else{
     	    	throw(new IOException("Bad format pbfile"));  		
     	    }
		}
		
		final int b3 = din.read();
		if(b3 == -1){
			LOG.info("Parse the pbfile error:" + file.toUri().toString());
     	    LOG.info("Reading b3");
     	    if(skipbad){
     	    	LOG.info("Skip the bad file");
     	    	reporter.incrCounter(Counter.BADFORMAT_FILE_COUNT, 1);
     	    	return true;
     	    }
     	    else{
     	    	throw(new IOException("Bad format pbfile"));  		
     	    }
		}
		
		final int b4 = din.read();
		if(b4 == -1){
			LOG.info("Parse the pbfile error:" + file.toUri().toString());
     	    LOG.info("Reading b4");
     	    if(skipbad){
     	    	LOG.info("Skip the bad file");
     	    	reporter.incrCounter(Counter.BADFORMAT_FILE_COUNT, 1);
     	    	return true;
     	    }
     	    else{
     	    	throw(new IOException("Bad format pbfile"));  		
     	    }
		}
		
		 size = ((b1 & 0xff)      )|
		        ((b2 & 0xff) <<  8)|
		        ((b3 & 0xff) << 16)|
		        ((b4 & 0xff) << 24);
		 return false;
	}
	
    
    public synchronized boolean next(LongWritable key, BytesWritable value) throws IOException {

        size = 0;
    	boolean readend = readLittleEndianInt(in);
    	if(readend){
    		LOG.info("read the pb file completely");
    		return false;
    	}
    	
    	if(size < 0){
    	    LOG.info("Parse the pbfile error:" + file.toUri().toString());
    	    LOG.info("get size " + size);
    	    if(skipbad){
    	    	LOG.info("Skip the bad file");
    	    	reporter.incrCounter(Counter.BADFORMAT_FILE_COUNT, 1);
    	    	return false;
    	    }
    	    else{
    	    	throw(new IOException("Bad format pbfile"));  		
    	    }
    	}
    	
    	pos += 2;
    	if(size == 0){
    		value.set(buffer, 0 , 0);
    		return true;
    	}
    	
        pos += size;
	    key.set(pos);
		
	    int readlen = 0;
	    if(size < buffer.length){
	    	int already_read = 0;
	    	while(already_read < size){
				readlen = in.read(buffer, already_read, size - already_read);
				if(readlen == -1){
					if(already_read < size){
		        	    LOG.info("Parse the pbfile error:" + file.toUri().toString());
		        	    LOG.info("current read size" + readlen + " but expected size:" + size);
		        	    if(skipbad){
		        	    	LOG.info("Skip the bad file");
		        	    	reporter.incrCounter(Counter.BADFORMAT_FILE_COUNT, 1);
		        	    	return false;
		        	    }
		        	    else{
		        	    	throw(new IOException("Bad format pbfile"));  		
		        	    }
				    }
					else
					    break;
				}

				already_read += readlen; 
	    	}
	        value.set(buffer, 0 , size);
		}
		else{
			byte[] tmp = new byte[size];
	    	int already_read = 0;
	    	while(already_read < size){
				readlen = in.read(tmp, already_read, size - already_read);
				if(readlen == -1){
					if(already_read < size){
		        	    LOG.info("Parse the pbfile error:" + file.toUri().toString());
		        	    LOG.info("current read size" + readlen + " but expected size:" + size);
		        	    if(skipbad){
		        	    	LOG.info("Skip the bad file");
		        	    	reporter.incrCounter(Counter.BADFORMAT_FILE_COUNT, 1);
		        	    	return false;
		        	    }
		        	    else{
		        	    	throw(new IOException("Bad format pbfile"));  
		        	    }
				    }
					else
					    break;
				}

				already_read += readlen; 
	    	}
	        value.set(tmp, 0 , size);
		}
		
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

