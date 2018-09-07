package rc.flume.interceptor;

public interface RCHttpInterCounterMBean {

    long getSdkVerNullCount();

    long incrementSdkVerNullCount();

    long getSdkVerOlderCount();

    long incrementSdkVerOlderCount();

    long getUnzipFailedCount();

    long incrementUnzipFailedCount();
}
