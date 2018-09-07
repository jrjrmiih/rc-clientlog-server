package rc.flume.interceptor;

import org.apache.flume.instrumentation.MonitoredCounterGroup;

public class RCHttpInterCounter extends MonitoredCounterGroup implements RCHttpInterCounterMBean {

    private static final String COUNTER_SDKVER_NULL = "inter.sdkver.null";
    private static final String COUNTER_SDKVER_OLDER = "inter.sdkver.older";
    private static final String COUNTER_UNZIP_FAILED = "inter.unzip.failed";

    private static final String[] ATTRIBUTES = {
            COUNTER_SDKVER_NULL, COUNTER_SDKVER_OLDER, COUNTER_UNZIP_FAILED
    };

    public RCHttpInterCounter(String name) {
        super(MonitoredCounterGroup.Type.INTERCEPTOR, name, ATTRIBUTES);
    }

    public long getSdkVerNullCount() {
        return get(COUNTER_SDKVER_NULL);
    }

    public long incrementSdkVerNullCount() {
        return increment(COUNTER_SDKVER_NULL);
    }

    public long getSdkVerOlderCount() {
        return get(COUNTER_SDKVER_OLDER);
    }

    public long incrementSdkVerOlderCount() {
        return increment(COUNTER_SDKVER_OLDER);
    }

    public long getUnzipFailedCount() {
        return get(COUNTER_UNZIP_FAILED);
    }

    public long incrementUnzipFailedCount() {
        return increment(COUNTER_UNZIP_FAILED);
    }
}
