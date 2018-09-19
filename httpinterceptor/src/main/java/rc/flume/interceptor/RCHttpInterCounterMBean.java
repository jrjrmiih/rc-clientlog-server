package rc.flume.interceptor;

public interface RCHttpInterCounterMBean {

    long getPlatformNullCount();

    long incrementPlatformNullCount();

    long getSdkVerNullCount();

    long incrementSdkVerNullCount();

    long getSdkVerOlderCount();

    long incrementSdkVerOlderCount();

    long getUnzipFailedCount();

    long incrementUnzipFailedCount();

    long getLatestAndroidUnzipFailedCount();

    long incrementLatestAndroidUnzipFailedCount();

    long getLatestIosUnzipFailedCount();

    long incrementLatestIosUnzipFailedCount();

    long getUserIdNullCount();

    long incrementUserIdNullCount();

    long getLatestAndroidUserIdNullCount();

    long incrementLatestAndroidUserIdNullCount();

    long getLatestIosUserIdNullCount();

    long incrementLatestIosUserIdNullCount();

    long getTimeFailedCount();

    long incrementTimeFailedCount();

    long getLatestAndroidTimeFailedCount();

    long incrementLatestAndroidTimeFailedCount();

    long getLatestIosTimeFailedCount();

    long incrementLatestIosTimeFailedCount();
}
