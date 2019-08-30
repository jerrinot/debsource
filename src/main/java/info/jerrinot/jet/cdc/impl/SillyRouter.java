package info.jerrinot.jet.cdc.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class SillyRouter {
    private final static ConcurrentMap<String, Context> map = new ConcurrentHashMap<>();

    private SillyRouter() {

    }

    static void registerContext(String uuid, Context context) {
        Context put = map.put(uuid, context);
        assert put == null : "Duplicated context registration";
    }

    static void removeContext(String uuid) {
        map.remove(uuid);
    }

    static Context getContext(String uuid) {
        return map.get(uuid);
    }
}
