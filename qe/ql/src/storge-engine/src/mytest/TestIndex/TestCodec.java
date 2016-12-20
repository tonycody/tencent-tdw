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

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.DefaultCodec;

import IndexStorage.SeekDIBuffer;

public class TestCodec {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		DefaultCodec codec = new DefaultCodec();
		codec.setConf(conf);
		DataOutputBuffer chunksWriteBuffer = new DataOutputBuffer();
		CompressionOutputStream compressionOutputStream = codec
				.createOutputStream(chunksWriteBuffer);

		DataInputBuffer chunkReadBuffer = new DataInputBuffer();
		CompressionInputStream compressionInputStream = codec
				.createInputStream(chunkReadBuffer);
		String str = "laksjldfkjalskdjfl;aksjdflkajsldkfjalksjdflkajlsdkfjlaksjdflka";
		compressionOutputStream.write(str.getBytes());
		compressionOutputStream.finish();
		byte[] data = chunksWriteBuffer.getData();
		System.out.println(str.length());
		System.out.println(chunksWriteBuffer.getLength());

		chunkReadBuffer.reset(data, chunksWriteBuffer.getLength());

		DataOutputBuffer dob = new DataOutputBuffer();
		IOUtils.copyBytes(compressionInputStream, dob, conf);
		System.out.println(dob.getData());

	}
}
