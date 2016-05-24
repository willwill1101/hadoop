package hadoop.hbase;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ReadHFileJob4Image {
	
	public static void main(String[] args) throws Exception {
		if(args!=null){
			System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.0");
			System.setProperty("HADOOP_USER_NAME", "hdfs");
		}
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = new Job(conf, "readHfile4image");
		job.setInputFormatClass(HFileInputFormat.class);
		job.setJarByClass(ReadMapperImage.class);
		job.setMapperClass(ReadMapperImage.class);
		job.setReducerClass(ReadReducerImage.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for(String str:ContentUtil.imagePaths){
			FileInputFormat.addInputPath(job, new Path(str));
		}
		FileOutputFormat.setOutputPath(job, new Path(ContentUtil.getValue("imageResultPath")+"/"+UUID.randomUUID()));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

 class ReadMapperImage extends Mapper<ImmutableBytesWritable, KeyValue,Text, IntWritable>
{
	 private static FileSystem fs = null;
    private Text mapKey = new Text();
    private final static IntWritable one = new IntWritable(1);
	@Override
	protected void map(ImmutableBytesWritable key, KeyValue value, Mapper<ImmutableBytesWritable, KeyValue,Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		try {
			fs =FileSystem.get(context.getConfiguration());
			String localimageUrl = Bytes.toHex(key.get());
			long time = Bytes.toLong(Bytes.copy(key.get(), key.get().length-8, 8));
			DateFormat df = new SimpleDateFormat("yyyyMM");
			SimpleDateFormat df2 = new SimpleDateFormat("yyyy/MM/dd/hh/mm");
			String yyyyMMdd =df.format(new Date(time));
			if(!ContentUtil.getValue("imageSaveDate").contains(yyyyMMdd)){
				return ;
			}
			
			    Path outFile = new Path(ContentUtil.getValue("imagePath")+"/"+df2.format(new Date(time))+"/hbase/tr_image/"+localimageUrl+".jpg");
			    System.out.println(ContentUtil.getValue("imagePath")+"/"+df2.format(new Date(time))+"/hbase/tr_image/"+localimageUrl+".jpg");
			    FSDataOutputStream out=null;
			    try {
					 out = fs.create(outFile);
					IOUtils.write(value.getValue(), out);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}finally{
					try {
						out.close();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			
			mapKey.set(yyyyMMdd);
			context.write(mapKey,one);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
 
 
  class ReadReducerImage extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
				result.set(sum);
			}
			context.write(key, result);
		}
}
