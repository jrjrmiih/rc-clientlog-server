package rccs.restful;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.validation.constraints.NotNull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class HbaseConnection {

    private static final String T_CLIENTLOG = "clientlog";
    private static final int BUFFER_LEN = 1024 * 1024;
    private static final int SHOW_MAX_LEN = 2 * 1024 * 1024;

    private Table clientlog;

    private static class SingletonHolder {
        private static final HbaseConnection INSTANCE = new HbaseConnection();
    }

    private HbaseConnection() {
        try {
            Configuration conf = HBaseConfiguration.create();
//            conf.set("hbase.zookeeper.quorum", "192.168.1.100");
            Connection conn = ConnectionFactory.createConnection(conf);
            clientlog = conn.getTable(TableName.valueOf(T_CLIENTLOG));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static HbaseConnection getInstance() {
        return SingletonHolder.INSTANCE;
    }

    /**
     * 获取指定 appKey，userId 指定时间范围的日志。
     *
     * @param tarStart 起始时间。
     * @param tarEnd   结束时间。
     * @return 用户日志，如遇意外则返回 null。
     */
    String getLog(String appKey, String userId, String tarStart, String tarEnd) throws IOException {
        if (appKey == null || userId == null) {
            return null;
        }

        String rowPrefix = appKey + "^" + userId + "^";
        StringBuilder logSb = new StringBuilder(BUFFER_LEN);
        Scan scan = new Scan();
        scan.setRowPrefixFilter(rowPrefix.getBytes());
        scan.setFilter(new PrefixFilter(rowPrefix.getBytes()));
        ResultScanner results = clientlog.getScanner(scan);
        for (Result res : results) {
            String[] keys = new String(res.getRow()).split("\\^");
            String start = keys[3];
            String end = keys[4];
            if ((tarStart == null || tarEnd == null) || (start.compareTo(tarEnd) < 0 && end.compareTo(tarStart) > 0)) {
                byte[] gzByte = res.getValue("data".getBytes(), "gz".getBytes());
                GZIPInputStream unGZip = new GZIPInputStream(new ByteArrayInputStream(gzByte));
                byte[] buffer = new byte[1024];
                int ret;
                while ((ret = unGZip.read(buffer)) > 0) {
                    logSb.append(new String(buffer, 0, ret));
                }
                if (logSb.length() > SHOW_MAX_LEN) {
                    logSb.append("... Too many logs, please narrow down the search ...");
                    break;
                }
            }
        }
        return logSb.length() > 0 ? logSb.toString() : null;
    }

    void getLogTest(String appKey) throws IOException {
        System.out.println("start = " + System.currentTimeMillis());
        String subKey = "^" + appKey + "^";
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(subKey));
        Scan scan = new Scan();
        scan.setFilter(rowFilter);
        ResultScanner results = clientlog.getScanner(scan);
        System.out.println("mid = " + System.currentTimeMillis());
        int i = 0;
        for (Result res : results) {
            if (i++ % 1000 == 0) {
                System.out.println("count = " + i);
            }
        }
        System.out.println("end = " + System.currentTimeMillis());
    }

    String getSum() throws IOException {
        Scan scan = new Scan();
        ResultScanner results = clientlog.getScanner(scan);
        long sum = 0;
        for (Result res : results) {
            for (Cell cell : res.rawCells()) {
                long count = Bytes.toLong(cell.getRowArray(), cell.getValueOffset(), cell.getValueLength());
                sum += count;
            }
        }
        return String.valueOf(sum);
    }

    int delSum() {
        int total;
        String rowPrefix = "sum_";
        Filter filter = new PrefixFilter(rowPrefix.getBytes());
        Scan scan = new Scan();
        scan.withStartRow(rowPrefix.getBytes());
        scan.setFilter(filter);
        try (ResultScanner results = clientlog.getScanner(scan)) {
            List<Delete> delList = new ArrayList<>();
            for (Result res : results) {
                Delete del = new Delete(res.getRow());
                delList.add(del);
            }
            total = delList.size();
            clientlog.delete(delList);
        } catch (IOException e) {
            e.printStackTrace();
            total = -1;
        }
        return total;
    }

    private String getSaltStr(@NotNull String userId) {
        return String.format("%02d", Math.abs(userId.hashCode() % 100));
    }
}
