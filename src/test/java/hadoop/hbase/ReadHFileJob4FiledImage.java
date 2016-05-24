package hadoop.hbase;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import javax.swing.text.DateFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
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


public class ReadHFileJob4FiledImage {
	
	public static void main(String[] args) throws Exception {
		if(args!=null){
			System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.0");
			System.setProperty("HADOOP_USER_NAME", "hdfs");
		}
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = new Job(conf, "readHfile4fieldimage");
		job.setInputFormatClass(HFileInputFormat.class);
		job.setJarByClass(ReadMapperFieldImage.class);
		job.setMapperClass(ReadMapperFieldImage.class);
		job.setReducerClass(ReadReducerFieldImage.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for(String str:ContentUtil.fieldImagePaths){
			FileInputFormat.addInputPath(job, new Path(str));
		}
		FileOutputFormat.setOutputPath(job, new Path(ContentUtil.getValue("fieldImageResultPath")+"/"+UUID.randomUUID()));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

 class ReadMapperFieldImage extends Mapper<ImmutableBytesWritable, KeyValue,Text, IntWritable>
{
    private Text mapKey = new Text();
    private final static IntWritable one = new IntWritable(1);
    private static Table tr_bay =  HTableProxy.getHTable("tr_bay");
    private static Table tr_plat =  HTableProxy.getHTable("tr_plate");
	@Override
	protected void map(ImmutableBytesWritable key, KeyValue value, Mapper<ImmutableBytesWritable, KeyValue,Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		try {
			
			String picUrl="";
			if(!"".equals(new String(value.getValue()))){
				SimpleDateFormat df2 = new SimpleDateFormat("yyyy/MM/dd/hh/mm");
				String timePath = df2.format(new Date(ByteUtil.getLong(key.get(),2)));
				picUrl = ContentUtil.getValue("ftpurl")+"/"+timePath+new String(value.getValue())+".jpg";
				System.out.println(picUrl);
			}
			
			Put putbay = new Put(key.get());
			putbay.addColumn(value.getFamily(), value.getQualifier(), Bytes.toBytes(picUrl));
			//插入hbase数据
			byte[] rowkey = new byte[22];
			ByteUtil.putString(rowkey, ByteUtil.getString(key.get(),10), 0);
			ByteUtil.putLong(rowkey, ByteUtil.getLong(key.get(),2), 12);
			ByteUtil.putShort(rowkey, ByteUtil.getShort(key.get(), 0), 20);
			Put putplate = new Put(rowkey);
			putplate.addColumn(value.getFamily(), value.getQualifier(), Bytes.toBytes(picUrl));
			try {
				tr_bay.put(putbay);
				tr_plat.put(putplate);
			} catch (Exception e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
				try {
					tr_bay.close();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					tr_plat.close();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				tr_bay =  HTableProxy.getHTable("tr_bay");
				tr_plat =  HTableProxy.getHTable("tr_plat");
			}
			
			
			DateFormat df = new SimpleDateFormat("yyyyMM");
			String yyyyMMdd =df.format(new Date(ByteUtil.getLong(key.get(),2)));
			mapKey.set(yyyyMMdd);
			context.write(mapKey,one);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
 
 
  class ReadReducerFieldImage extends Reducer<Text, IntWritable, Text, IntWritable> {
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
