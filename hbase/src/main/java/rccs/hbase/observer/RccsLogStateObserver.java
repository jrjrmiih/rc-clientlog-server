package rccs.hbase.observer;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class RccsLogStateObserver extends BaseRegionObserver {

    private static final String T_LOGSTATE = "logstate";
    private static final byte[] CF_DAY = "day".getBytes();

    private static final Logger logger = Logger.getLogger(RccsLogStateObserver.class);
    private static Table tLogState;

    private HashMap<String, AtomicLong> countMap = new HashMap<String, AtomicLong>(1000);

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
        doStart();
    }


    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        super.stop(e);
        doStop();
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.prePut(e, put, edit, durability);
        put.addColumn("data".getBytes(), "ip".getBytes(), "121231".getBytes());
        doPrePut(put);
    }

    static String timestamp2Date(String timestamp) throws NumberFormatException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd");
        return sdf.format(new Date(Long.valueOf(timestamp + "000")));
    }

    static boolean isValidTime(String day) {
        DateFormat df = new SimpleDateFormat("yyMMdd");
        try {
            Date date = df.parse(day);
            Date now = new Date();
            long delta = now.getTime() - date.getTime();
            return (delta > -24 * 60 * 60 * 1000) && (delta < 7 * 24 * 60 * 60 * 1000);
        } catch (ParseException e) {
            return false;
        }
    }

    public static void main(String[] args) {
        System.out.println("asdfo3".hashCode());
    }

    public void doStart() throws IOException {
        Connection conn = ConnectionFactory.createConnection();
        tLogState = conn.getTable(TableName.valueOf(T_LOGSTATE));

        Scan scan = new Scan();
        ResultScanner rs = tLogState.getScanner(scan);
        for (Result r : rs) {
            for (Cell c : r.rawCells()) {
                String rowKey = Bytes.toString(CellUtil.cloneRow(c));
                String family = Bytes.toString(CellUtil.cloneFamily(c));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(c));
                String value = Bytes.toString(CellUtil.cloneValue(c));
                if (family.equals("day")) {
                    countMap.put(rowKey + "_" + qualifier, new AtomicLong(Long.valueOf(value)));
                }
            }
        }
        logger.info("start size = " + countMap.size());
    }


    public void doStop() throws IOException {
        logger.info("end size = " + countMap.size());
        List<Put> puts = new ArrayList<Put>(1000);
        for (Map.Entry<String, AtomicLong> entry : countMap.entrySet()) {
            String mapKey = entry.getKey();
            String[] split = mapKey.split("_");
            String appKey = split[0];
            String day = split[1];
            long count = entry.getValue().get();
            if (isValidTime(day)) {
                Put state = new Put(appKey.getBytes());
                state.addColumn(CF_DAY, day.getBytes(), Bytes.toBytes(count));
                puts.add(state);
                if (puts.size() >= 10000) {
                    tLogState.put(puts);
                    puts.clear();
                }
            }
        }
        logger.info("puts.size = " + puts.size());
        if (puts.size() > 0) {
            tLogState.put(puts);
            logger.info("puts done");
        }
        tLogState.close();
        logger.info("table close");
    }

    public void doPrePut(Put put) throws IOException {
        String[] split = new String(put.getRow()).split("\\^");
        String appKey = split[1];
        String day = timestamp2Date(split[3]);
        String mapKey = appKey + "_" + day;
        AtomicLong count = countMap.get(mapKey);
        if (count == null) {
            synchronized (this) {
                count = countMap.get(mapKey);
                if (count == null) {
                    Get get = new Get(appKey.getBytes());
                    get.addColumn(CF_DAY, day.getBytes());
                    Result result = tLogState.get(get);
                    List<Cell> cells = result.getColumnCells(CF_DAY, day.getBytes());
                    if (cells.size() > 0) {
                        long total = Bytes.toLong(CellUtil.cloneValue(cells.get(0)));
                        count = new AtomicLong(total);
                    } else {
                        count = new AtomicLong();
                    }
                    countMap.put(mapKey, count);
                }
            }
        }
        long after = count.incrementAndGet();
        logger.info("put after = " + after);
        if (after % 1000 == 0 && isValidTime(day)) {
            Put state = new Put(appKey.getBytes());
            state.addColumn(CF_DAY, day.getBytes(), Bytes.toBytes(after));
            tLogState.put(state);
        }
    }
}
