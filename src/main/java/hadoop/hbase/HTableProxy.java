package hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

/**
  * @ClassName: HTableProxy 
  * @Description: hbase连接词 
  * @author 卢海友 
  * @date 2015-3-26 上午9:54:44
 */
public class HTableProxy {
	private static Logger logger = Logger.getLogger(HTableProxy.class);

	private static Configuration conf;
	private static Connection con;
	private static boolean init;
	public static void init(){
		//初始化HBase链接
		try {
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", ContentUtil.getValue("zookeeperIp"));				// 设置zooKeeper访问地址
			con = ConnectionFactory.createConnection(conf);
			init = true;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static void reset(){
		if(con != null){
			try {
				con.close();
			} catch (IOException e) {
				logger.error("Htable线程池关闭失败：" + e.getMessage());
				e.printStackTrace();
			}
		}
		try {
			con = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param name
	 * @return
	 */
	public static Table getHTable(String tableName){
		if(!init){
			init();
		}
		Table table = null;
		try {
			table = con.getTable(TableName.valueOf(tableName));
			table.setWriteBufferSize(1024*1024*24);
		} catch (IOException e) {
			e.printStackTrace();
		} //从HTable池中获取链接
		
		return table;
	}
}

