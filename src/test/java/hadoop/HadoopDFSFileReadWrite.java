package hadoop;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class HadoopDFSFileReadWrite {
  
@Test
  public void read() throws IOException{
	  Configuration conf = new Configuration();
	    String uri = "hdfs://10.150.27.218:8020/";
	    FileSystem fs = FileSystem.get(URI.create(uri),conf);
	    Path inFile = new Path("hdfs://10.150.27.218:8020/yangx/123.txt");
	    FSDataInputStream in = fs.open(inFile);
	    IOUtils.copyBytes(in, System.out, 1024*8);
	    in.close();
  }

@Test
 public void write() throws IOException{
	  Configuration conf = new Configuration();
	    String uri = "hdfs://10.150.27.218:8020/";
	    FileSystem fs = FileSystem.get(URI.create(uri),conf);
	    Path outFile = new Path("hdfs://10.150.27.218:8020/yangx/124.txt");
	    FSDataOutputStream out = fs.create(outFile);
	    ByteArrayInputStream in =  new ByteArrayInputStream("yangx3".getBytes());
	    IOUtils.copyBytes(in, out, 1024*8);
	    in.close();
	    out.close();
}
  
  
}