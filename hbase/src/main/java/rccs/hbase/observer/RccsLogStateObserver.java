package rccs.hbase.observer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;

import java.io.IOException;

public class RccsLogStateObserver extends BaseRegionObserver {

    private static final String TABLE_CLIENTLOG = "logstate";

    private static final Logger logger = Logger.getLogger(RccsLogStateObserver.class);
    private static Table clientlog;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
                "AWSOR-DCAS-PC-SVC-Hadoop-8-46,AWSOR-DCAS-PC-SVC-Hadoop-8-50,AWSOR-DCAS-PC-SVC-Hadoop-8-59");
        Connection conn = ConnectionFactory.createConnection(conf);
        clientlog = conn.getTable(TableName.valueOf(TABLE_CLIENTLOG));
    }


    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        super.stop(e);
        clientlog.close();
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.prePut(e, put, edit, durability);
        byte[] rowKey = put.getRow();
        Put state = new Put(rowKey);
        state.addColumn("state".getBytes(), "salt".getBytes(), "abcdefg".getBytes());
        state.addColumn("day".getBytes(), "181102".getBytes(), "asdf;lkj".getBytes());
        clientlog.put(state);
        Get get = new Get(rowKey);
    }
}
