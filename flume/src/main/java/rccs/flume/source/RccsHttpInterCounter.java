package rccs.flume.source;

import org.apache.flume.instrumentation.MonitoredCounterGroup;

public class RccsHttpInterCounter extends MonitoredCounterGroup implements RccsHttpInterCounterMBean {

    private static final String COUNTER_PLATFORM_NULL = "inter.platform.null";
    private static final String COUNTER_SDKVER_NULL = "inter.sdkver.null";
    private static final String COUNTER_SDKVER_OLDER = "inter.sdkver.older";
    private static final String COUNTER_UNZIP_FAILED = "inter.unzip.failed";
    private static final String COUNTER_LATEST_ANDROID_UNZIP_FAILED = "inter.latest.android.unzip.failed";
    private static final String COUNTER_LATEST_IOS_UNZIP_FAILED = "inter.latest.ios.unzip.failed";
    private static final String COUNTER_USERID_NULL = "inter.userid.null";
    private static final String COUNTER_LATEST_ANDROID_USERID_NULL = "inter.latest.android.userid.null";
    private static final String COUNTER_LATEST_IOS_USERID_NULL = "inter.latest.ios.userid.null";
    private static final String COUNTER_TIME_FAILED = "inter.time.failed";
    private static final String COUNTER_LATEST_ANDROID_TIME_FAILED = "inter.latest.android.time.failed";
    private static final String COUNTER_LATEST_IOS_TIME_FAILED = "inter.latest.ios.time.failed";

    private static final String[] ATTRIBUTES = {
            COUNTER_PLATFORM_NULL, COUNTER_SDKVER_NULL, COUNTER_SDKVER_OLDER,
            COUNTER_UNZIP_FAILED, COUNTER_LATEST_ANDROID_UNZIP_FAILED, COUNTER_LATEST_IOS_UNZIP_FAILED,
            COUNTER_USERID_NULL, COUNTER_LATEST_ANDROID_USERID_NULL, COUNTER_LATEST_IOS_USERID_NULL,
            COUNTER_TIME_FAILED, COUNTER_LATEST_ANDROID_TIME_FAILED, COUNTER_LATEST_IOS_TIME_FAILED
    };

    public RccsHttpInterCounter(String name) {
        super(Type.INTERCEPTOR, name, ATTRIBUTES);
    }

    public long getPlatformNullCount() {
        return get(COUNTER_PLATFORM_NULL);
    }

    public long incrementPlatformNullCount() {
        return increment(COUNTER_PLATFORM_NULL);
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

    public long getLatestAndroidUnzipFailedCount() {
        return get(COUNTER_LATEST_ANDROID_UNZIP_FAILED);
    }

    public long incrementLatestAndroidUnzipFailedCount() {
        return increment(COUNTER_LATEST_ANDROID_UNZIP_FAILED);
    }

    public long getLatestIosUnzipFailedCount() {
        return get(COUNTER_LATEST_IOS_UNZIP_FAILED);
    }

    public long incrementLatestIosUnzipFailedCount() {
        return increment(COUNTER_LATEST_IOS_UNZIP_FAILED);
    }

    public long getUserIdNullCount() {
        return get(COUNTER_USERID_NULL);
    }

    public long incrementUserIdNullCount() {
        return increment(COUNTER_USERID_NULL);
    }

    public long getLatestAndroidUserIdNullCount() {
        return get(COUNTER_LATEST_ANDROID_USERID_NULL);
    }

    public long incrementLatestAndroidUserIdNullCount() {
        return increment(COUNTER_LATEST_ANDROID_USERID_NULL);
    }

    public long getLatestIosUserIdNullCount() {
        return get(COUNTER_LATEST_IOS_USERID_NULL);
    }

    public long incrementLatestIosUserIdNullCount() {
        return increment(COUNTER_LATEST_IOS_USERID_NULL);
    }

    public long getTimeFailedCount() {
        return get(COUNTER_TIME_FAILED);
    }

    public long incrementTimeFailedCount() {
        return increment(COUNTER_TIME_FAILED);
    }

    public long getLatestAndroidTimeFailedCount() {
        return get(COUNTER_LATEST_ANDROID_TIME_FAILED);
    }

    public long incrementLatestAndroidTimeFailedCount() {
        return increment(COUNTER_LATEST_ANDROID_TIME_FAILED);
    }

    public long getLatestIosTimeFailedCount() {
        return get(COUNTER_LATEST_IOS_TIME_FAILED);
    }

    public long incrementLatestIosTimeFailedCount() {
        return increment(COUNTER_LATEST_IOS_TIME_FAILED);
    }
}
