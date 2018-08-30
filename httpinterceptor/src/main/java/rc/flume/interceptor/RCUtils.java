package rc.flume.interceptor;

class RCUtils {

    static boolean isValidVer(String sdkVer, String minVer) {
        String[] sdkVers = sdkVer.split("\\.");
        String[] minVers = minVer.split("\\.");
        if (sdkVers.length != 3 || minVers.length != 3) {
            return false;
        }
        return Integer.parseInt(sdkVers[0]) >= Integer.parseInt(minVers[0]) &&
                Integer.parseInt(sdkVers[1]) >= Integer.parseInt(minVers[1]) &&
                Integer.parseInt(sdkVers[2]) >= Integer.parseInt(minVers[2]);
    }
}
