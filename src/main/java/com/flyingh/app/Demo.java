package com.flyingh.app;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.hadoop.hbase.CellUtil.*;

/**
 * Created by Flycoding on 2016/4/2.
 */
public class Demo {
    public static void main(final String[] args) throws IOException {
        try (Table table = ConnectionFactory.createConnection(HBaseConfiguration.create()).getTable(TableName.valueOf("user"))) {
            Scan scan = new Scan().setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("r[32]")));
            final Stream<Cell> stream = StreamSupport.stream(table.getScanner(scan).spliterator(), false).flatMap(result -> Arrays.stream(result.rawCells()));
            stream.forEach(cell -> System.out.printf("row:%s,column family:%s,qualifier:%s,value:%s%n",
                    Bytes.toString(cloneRow(cell)),
                    Bytes.toString(cloneFamily(cell)),
                    Bytes.toString(cloneQualifier(cell)),
                    Bytes.toString(cloneValue(cell))
            ));
        }
    }

    private static void get() throws IOException {
        try (Table table = ConnectionFactory.createConnection(HBaseConfiguration.create()).getTable(TableName.valueOf("user"))) {
            final Result result = table.get(new Get(Bytes.toBytes("r2")));
            if (result.isEmpty()) {
                return;
            }
            Arrays.stream(result.rawCells()).forEach(cell ->
                    System.out.printf("row:%s,column family:%s,qualifier:%s,value:%s%n",
                            Bytes.toString(cloneRow(cell)),
                            Bytes.toString(cloneFamily(cell)),
                            Bytes.toString(cloneQualifier(cell)),
                            Bytes.toString(cloneValue(cell))
                    ));
        }
    }

    private static void iterate() throws IOException {
        try (Table table = ConnectionFactory.createConnection(HBaseConfiguration.create()).getTable(TableName.valueOf("user"))) {
            final Stream<Cell> stream = StreamSupport.stream(table.getScanner(new Scan()).spliterator(), false).flatMap(result -> Arrays.stream(result.rawCells()));
            stream.forEach(cell -> System.out.printf("row:%s,column family:%s,qualifier:%s,value:%s%n",
                    Bytes.toString(cloneRow(cell)),
                    Bytes.toString(cloneFamily(cell)),
                    Bytes.toString(cloneQualifier(cell)),
                    Bytes.toString(cloneValue(cell))
            ));
        }
    }

    private static void put() throws IOException {
        try (Table table = ConnectionFactory.createConnection(HBaseConfiguration.create()).getTable(TableName.valueOf("user"))) {
            final Put put = new Put(Bytes.toBytes("r1"));
            put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("age"), Bytes.toBytes(22));
            table.put(put);
        }
    }

    private static void createTable() throws IOException {
        try (Admin admin = ConnectionFactory.createConnection(HBaseConfiguration.create()).getAdmin()) {
            admin.createTable(new HTableDescriptor(TableName.valueOf("user")).addFamily(new HColumnDescriptor("base")));
        }
    }
}
