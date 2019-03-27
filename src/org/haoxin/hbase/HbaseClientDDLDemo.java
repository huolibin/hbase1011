package org.haoxin.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.generated.master.table_jsp;
import org.apache.hadoop.hbase.util.Bytes;
import org.jruby.compiler.ir.operands.Array;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * DDL数据的增删改查
 * 
 * @author acer
 *
 */
public class HbaseClientDDLDemo {

	Connection conn = null;

	@Before
	public void connHbase() throws Exception {
		// 构建一个连接对象
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hdp1:2181,hdp2:2181,hdp3:2181");
		conn = ConnectionFactory.createConnection(conf);

	}

	/**
	 * 增 改
	 * 
	 * @throws IOException
	 */

	@Test
	public void testPut() throws IOException {
		// 获取一个表的对象
		Table table = conn.getTable(TableName.valueOf("user_info"));

		// 构造要插入的数据为put类型
		Put put = new Put(Bytes.toBytes("002"));
		put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhangsan"));
		put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("18"));
		put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("sex"), Bytes.toBytes("male"));

		Put put2 = new Put(Bytes.toBytes("003"));
		put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhangsan"));
		put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("18"));
		put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("sex"), Bytes.toBytes("male"));

		ArrayList<Put> arrayList = new ArrayList<>();
		arrayList.add(put);
		arrayList.add(put2);

		// 数据插入
		table.put(arrayList);

		table.close();
		conn.close();

	}

	/**
	 * 删
	 * 
	 * @throws IOException
	 */
	@Test
	public void testDelete() throws IOException {
		// 获取一个表的对象
		Table table = conn.getTable(TableName.valueOf("user_info"));

		Delete delete = new Delete(Bytes.toBytes("001"));

		Delete delete2 = new Delete(Bytes.toBytes("003"));
		delete2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"));

		ArrayList<Delete> arrayList = new ArrayList<>();
		arrayList.add(delete);
		arrayList.add(delete2);

		table.delete(arrayList);

		table.close();
		conn.close();

	}

	/**
	 * 查
	 * 
	 * @throws IOException
	 */
	@Test
	public void testGet() throws IOException {
		// 获取一个表的对象
		Table table = conn.getTable(TableName.valueOf("user_info"));

		Get get = new Get("002".getBytes());

		Result result = table.get(get);

		// 从结果中取指定某个key的value
		byte[] value = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("username"));
		System.out.println(new String(value));

		System.out.println("---------------------------------------------------------");
		// 遍历结果 取出所有的kv
		CellScanner cellScanner = result.cellScanner();

		while (cellScanner.advance()) {
			Cell cell = cellScanner.current();

			byte[] rowArray = cell.getRowArray(); // 行键
			byte[] familyArray = cell.getFamilyArray(); // 列族
			byte[] qualifierArray = cell.getQualifierArray(); // 列名
			byte[] valueArray = cell.getValueArray(); // value值

			System.out.println("行键:" + new String(rowArray, cell.getRowOffset(), cell.getRowLength()));
			System.out.println("列族名:" + new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()));
			System.out
					.println("列名:" + new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()));
			System.out.println("value:" + new String(valueArray, cell.getValueOffset(), cell.getValueLength()));

		}

		table.close();
		conn.close();
	}

	/**
	 * row的范围查询
	 * 
	 * @throws IOException
	 * 
	 */

	@Test
	public void testRowRange() throws IOException {
		// 获取一个表的对象
		Table table = conn.getTable(TableName.valueOf("user_info"));
		Scan scan = new Scan("001".getBytes(),"005".getBytes());
		
		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> iterator = scanner.iterator();
		while (iterator.hasNext()) {
			Result result = (Result) iterator.next();
			// 遍历结果 取出所有的kv
			CellScanner cellScanner = result.cellScanner();

			while (cellScanner.advance()) {
				Cell cell = cellScanner.current();

				byte[] rowArray = cell.getRowArray(); // 行键
				byte[] familyArray = cell.getFamilyArray(); // 列族
				byte[] qualifierArray = cell.getQualifierArray(); // 列名
				byte[] valueArray = cell.getValueArray(); // value值

				System.out.println("行键:" + new String(rowArray, cell.getRowOffset(), cell.getRowLength()));
				System.out.println("列族名:" + new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()));
				System.out
						.println("列名:" + new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()));
				System.out.println("value:" + new String(valueArray, cell.getValueOffset(), cell.getValueLength()));

			}

			System.out.println("------------------------------");
		}
		table.close();
		conn.close();

	}
	

}
