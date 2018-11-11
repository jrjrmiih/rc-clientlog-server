package rccs.restful;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class HbaseConnection {

    private static final int BUFFER_LEN = 10 * 1024;
    private static final int MAX_LOG_LENGTH = 1024 * 1024;

    private static final String T_CLIENTLOG = "clientlog";
    private static final String T_USER_RECORD = "user_record";
    private static final byte[] CF_DATA = "data".getBytes();
    private static final byte[] CQ_GZ = "gz".getBytes();
    private static final byte[] CQ_OS = "os".getBytes();
    private static final byte[] CQ_VER = "ver".getBytes();
    private static final byte[] CQ_IP = "ip".getBytes();

    private Table tClientLog;
    private Table tUserRecord;

    private static class SingletonHolder {
        private static final HbaseConnection INSTANCE = new HbaseConnection();
    }

    private HbaseConnection() {
        try {
            Configuration conf = HBaseConfiguration.create();
            Connection conn = ConnectionFactory.createConnection(conf);
            tClientLog = conn.getTable(TableName.valueOf(T_CLIENTLOG));
            tUserRecord = conn.getTable(TableName.valueOf(T_USER_RECORD));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static HbaseConnection getInstance() {
        return SingletonHolder.INSTANCE;
    }

    /**
     * @param aid AppKey
     * @param uid UserId
     * @return Json string of log info list.
     * @throws IOException if HBase encounter problems.
     */
    String getLogInfo(@NotNull String aid, @NotNull String uid) throws IOException {
        String rowPrefix = getSaltStr(uid) + "^" + aid + "^" + uid + "^";
        Scan scan = new Scan();
        scan.setRowPrefixFilter(rowPrefix.getBytes());
        scan.setFilter(new PrefixFilter(rowPrefix.getBytes()));

        ResultScanner results = tClientLog.getScanner(scan);
        List<LogRecord> recordList = new ArrayList<>();
        for (Result res : results) {
            String[] split = new String(res.getRow()).split("\\^");
            LogRecord log = new LogRecord(split[1], split[2], Long.parseLong(split[3]), Long.parseLong(split[4]));
            log.setSize(res.getValue(CF_DATA, CQ_GZ).length);
            log.setOs(Bytes.toString(res.getValue(CF_DATA, CQ_OS)));
            log.setVer(Bytes.toString(res.getValue(CF_DATA, CQ_VER)));
            log.setIp(Bytes.toString(res.getValue(CF_DATA, CQ_IP)));
            recordList.add(log);
        }
        return new Gson().toJson(recordList);
    }

    /**
     * Get log data from specific one record.
     *
     * @param aid   AppKey
     * @param uid   UserId
     * @param start StartTime。
     * @param end   EndTime。
     * @return String of the log data.
     */
    String getLogData(@NotNull String aid, @NotNull String uid, @NotNull String start, @NotNull String end)
            throws IOException {
        String rowKey = getSaltStr(uid) + "^" + aid + "^" + uid + "^" + start + "^" + end;
        Get get = new Get(rowKey.getBytes());
        Result res = tClientLog.get(get);
        if (res.isEmpty()) {
            return null;
        }

        byte[] gzByte = res.getValue("data".getBytes(), "gz".getBytes());
        GZIPInputStream unGZip = new GZIPInputStream(new ByteArrayInputStream(gzByte));

        byte[] buffer = new byte[1024];
        StringBuilder logSb = new StringBuilder(BUFFER_LEN);
        int ret;
        while ((ret = unGZip.read(buffer)) > 0) {
            logSb.append(new String(buffer, 0, ret));
            if (logSb.length() > MAX_LOG_LENGTH) {
                logSb.append("... Too many logs, please contact administrator for help ...");
                break;
            }
        }
        return logSb.toString();
    }

    /**
     * Get user data from specific one appKey.
     *
     * @param aid AppKey
     * @return String of the user data.
     */
    String getUserList(@NotNull String aid) throws IOException {
        Scan scan = new Scan();
        scan.setRowPrefixFilter(aid.getBytes());
        scan.setFilter(new PrefixFilter(aid.getBytes()));

        ResultScanner results = tUserRecord.getScanner(scan);
        Map<String, List<String>> recList = new LinkedHashMap<>();
        for (Result res : results) {
            String[] split = new String(res.getRow()).split("_");
            List<String> users = new ArrayList<>();
            for (Cell cell : res.listCells()) {
                String uid = new String(CellUtil.cloneQualifier(cell));
                users.add(uid);
            }
            recList.put(split[1], users);
        }
        return new Gson().toJson(recList);
    }

    private String getSaltStr(@NotNull String userId) {
        return String.format("%02d", Math.abs(userId.hashCode() % 100));
    }
}
