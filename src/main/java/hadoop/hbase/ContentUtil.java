package hadoop.hbase;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.corba.se.impl.oa.poa.ActiveObjectMap.Key;


/**
 * Hello world!
 *
 */
public class ContentUtil 
{	
	private static	Logger log = LoggerFactory.getLogger(ContentUtil.class);
	private static  Properties properties = new Properties();
	public static List<String> fieldPaths =  new ArrayList<String>();
	public static List<String> fieldImagePaths =  new ArrayList<String>();
	public static List<String> imagePaths =  new ArrayList<String>();
	
   static{
	   InputStream input = null;
	   try {
		input=Thread.currentThread().getContextClassLoader().getResourceAsStream("conf.properties");
		   properties.load(input);
		   log.info("配置文件加载成功！");
	} catch (IOException e) {
		 log.debug("配置文件加载失败！",e);
	}finally {
		if(input!=null){
			try {
				input.close();
			} catch (IOException e) {
			}
		}
	}
	   
	   try {
			input=Thread.currentThread().getContextClassLoader().getResourceAsStream("FieldPaths");
			fieldPaths=   IOUtils.readLines(input);
			   log.info("配置FieldPaths文件加载成功！");
		} catch (IOException e) {
			 log.debug("配置FieldPaths文件加载失败！",e);
		}finally {
			if(input!=null){
				try {
					input.close();
				} catch (IOException e) {
				}
			}
		}  
	   
	   try {
			input=Thread.currentThread().getContextClassLoader().getResourceAsStream("FieldImagePaths");
			fieldImagePaths=   IOUtils.readLines(input);
			   log.info("配置FieldImagePaths文件加载成功！");
		} catch (IOException e) {
			 log.debug("配置FieldImagePaths文件加载失败！",e);
		}finally {
			if(input!=null){
				try {
					input.close();
				} catch (IOException e) {
				}
			}
		} 
	   
	   
	   try {
			input=Thread.currentThread().getContextClassLoader().getResourceAsStream("ImagePaths");
			imagePaths=   IOUtils.readLines(input);
			   log.info("配置ImagePaths文件加载成功！");
		} catch (IOException e) {
			 log.debug("配置ImagePaths文件加载失败！",e);
		}finally {
			if(input!=null){
				try {
					input.close();
				} catch (IOException e) {
				}
			}
		} 
   }
   /**
    * 获取配置文件
    * @param key
    * @return
    */
    public static  String getValue(String key){
    	return properties.getProperty(key.toString());
    }
   
}
