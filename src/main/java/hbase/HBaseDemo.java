package hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HBaseDemo {
	Configuration conf;

	@Test
	public void createTable() throws Exception {

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "it01:2181,it02:2181,it03:2181");
		HBaseAdmin admin = new HBaseAdmin(conf);
		TableName name = TableName.valueOf("nvshen");
		HTableDescriptor desc = new HTableDescriptor(name);
		HColumnDescriptor base_info = new HColumnDescriptor("base_info");
		HColumnDescriptor extra_info = new HColumnDescriptor("extra_info");
		base_info.setMaxVersions(5);
		desc.addFamily(base_info);
		desc.addFamily(extra_info);
		admin.createTable(desc); // createTable
		admin.close();
	}

	@Before
	public void init() {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "it01:2181,it02:2181,it03:2181");
	}

	@Test
	/**
	 * insert data
	 * 
	 * @throws Exception
	 */
	public void testInset() throws Exception {
		HTable nvshen = new HTable(conf, "nvshen");
		Put name = new Put(Bytes.toBytes("rs0001"));
		name.add(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("angelababy"));
		Put age = new Put(Bytes.toBytes("rs0001"));
		age.add(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("18"));
		ArrayList<Put> puts = new ArrayList<Put>();
		puts.add(name);
		puts.add(age);
		nvshen.put(puts);
		nvshen.close();
	}

	@Test
	public void testDrop() throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.disableTable("nvshen");
		admin.deleteTable("nvshen");
		admin.close();
	}

	@Test
	public void testPut() throws Exception {
		HTable nvshen = new HTable(conf, "nvshen");
		Put id = new Put(Bytes.toBytes("rs0002"));
		id.add(Bytes.toBytes("base_info"), Bytes.toBytes("ID"), Bytes.toBytes("911843198809182345"));

		Put name = new Put(Bytes.toBytes("rs0002"));
		name.add(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("angelababy_v1"));

		Put sex = new Put(Bytes.toBytes("rs0002"));
		sex.add(Bytes.toBytes("base_info"), Bytes.toBytes("sex"), Bytes.toBytes("woman"));

		Put age = new Put(Bytes.toBytes("rs0002"));
		age.add(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("28"));

		ArrayList<Put> puts = new ArrayList<Put>();
		puts.add(id);
		puts.add(name);
		puts.add(sex);
		puts.add(age);
		nvshen.put(puts);
		nvshen.close();
	}

	@Test
	public void testGet() throws Exception {
		HTable nvshen = new HTable(conf, "nvshen");
		Get get = new Get(Bytes.toBytes("rs0002"));
		get.setMaxVersions(5);
		Result result = nvshen.get(get);
//		byte[] value = result.getValue(Bytes.toBytes("base_info"),Bytes.toBytes("name"));
//		System.out.println(new String(value));
		List<Cell> listCells = result.listCells();
		for (Cell cell : listCells) {
			String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
            String family= Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
            String column= Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            System.out.println(row+"\n---"+family+":"+column+" = "+value);
		}
		nvshen.close();
	}

	@Test
	public void testScan() throws Exception {
		HTable nvshen = new HTable(conf,"nvshen");
		Scan scan = new Scan();
		ResultScanner scanner = nvshen.getScanner(scan);
		for (Result rs : scanner) {
			/**
			 * for(KeyValue kv : r.list()){ String family = new String(kv.getFamily());
			 * System.out.println(family); String qualifier = new String(kv.getQualifier());
			 * System.out.println(qualifier); System.out.println(new String(kv.getValue()));
			 * }
			 */
			for(Cell cell:rs.rawCells()){
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String family= Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                String column= Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(row+"---"+family+":"+column+" = "+value);
           }
		}
		nvshen.close();
	}

	@Test
	public void testDel() throws Exception {
		HTable nvshen = new HTable(conf, "nvshen");
		Delete del = new Delete(Bytes.toBytes("rs0002"));// row(行)
		del.deleteColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age")); // column(列)
		nvshen.delete(del);
		nvshen.close();
	}

}
