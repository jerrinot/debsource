package info.jerrinot.jet.cdc.impl;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static java.util.Collections.emptyMap;

public final class SillyOffsetBackingStore implements OffsetBackingStore {
    private Context context;

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys, Callback<Map<ByteBuffer, ByteBuffer>> callback) {
        OffsetSnapshot snapshot = context.restoreSnapshot();
        Map<ByteBuffer, ByteBuffer> resultingMap;
        if (snapshot == null) {
            resultingMap = emptyMap();
        } else {
            resultingMap = new HashMap<>();
            Map<ByteBuffer, ByteBuffer> snapshotMap = snapshot.getMaps();
            for (ByteBuffer key : keys) {
                resultingMap.put(key, snapshotMap.get(key));
            }
        }
        if (callback != null) {
            callback.onCompletion(null, resultingMap);
        }
        return CompletableFuture.completedFuture(resultingMap);
    }

    @Override
    public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback) {
        OffsetSnapshot snapshot = new OffsetSnapshot(values);
        context.enqueueSnapshot(snapshot);
        if (callback != null) {
            callback.onCompletion(null, null);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void configure(WorkerConfig config) {
        String jobId = config.getString(Constants.JOB_ID_KEY);
        context = SillyRouter.getContext(jobId);
    }
}
