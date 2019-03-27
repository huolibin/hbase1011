package org.haoxin.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.Before;
import org.junit.Test;

public class HbaseClientDemo {
	Connection conn = null;
	
	@Before
	public void connHbase() throws Exception {
		//构建一个连接对象
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hdp1:2181,hdp2:2181,hdp3:2181");
		conn = ConnectionFactory.createConnection(conf);
		
	}
	
	
	/**
	 * 创建表
	 * @throws Exception
	 */
	@Test
	public void testCreateTable() throws Exception {
		
		//从连接中构造一个ddl操作器
		Admin admin = conn.getAdmin();
		
		//创建个表定义信息 描述对象
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("user_info"));
		//创建列族定义描述对象
		HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor("base_info");
		hColumnDescriptor1.setMaxVersions(3);
		
		HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor("extra_info");
		
		//将列族定义信息对象放入对象中
		hTableDescriptor.addFamily(hColumnDescriptor1);
		hTableDescriptor.addFamily(hColumnDescriptor2);
		
		
		//用ddl操作器建表
		admin.createTable(hTableDescriptor);
		
		//关闭连接
		admin.close();
		conn.close();
		
	}
	
	
	/**
	 * 删除表
	 * @throws Exception 
	 */

	@Test
	public void testDropTable() throws Exception {
		
		Admin admin = conn.getAdmin();
		
		admin.disableTable(TableName.valueOf("user_info"));
		admin.deleteTable(TableName.valueOf("user_info"));
		
		admin.close();
		conn.close();
	}
	
	
	/**
	 * 修改表
	 * @throws IOException 
	 */
	
	@Test
	public void testAlterTable() throws IOException {
		
		Admin admin = conn.getAdmin();
		//取出旧表定义信息
		HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf("user_info"));
		
		//构建一个新的列族
		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("other_info");
		hColumnDescriptor.setBloomFilterType(BloomType.ROW);//设置该列族的布隆过滤器
		
		tableDescriptor.addFamily(hColumnDescriptor);
		
		
		admin.modifyTable(TableName.valueOf("user_info"), tableDescriptor);
		
		
		admin.close();
		conn.close();
		
		
		
	}
}
