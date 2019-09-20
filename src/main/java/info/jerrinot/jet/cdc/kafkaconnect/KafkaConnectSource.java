package info.jerrinot.jet.cdc.kafkaconnect;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.TimestampedSourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.HistoryRecord;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.hazelcast.util.ExceptionUtil.rethrow;

public final class KafkaConnectSource {

    private KafkaConnectSource() {

    }

    public static StreamSource<SourceRecord> kafkaConnectStream(Map<String, String> config) {
        String name = config.get("name");
        config.put("database.history", DBHistory.class.getName());
        return SourceBuilder.timestampedStream(name, ctx -> new Context(ctx, config))
                            .fillBufferFn(Context::fillBuffer)
                            .createSnapshotFn(Context::createSnapshot)
                            .restoreSnapshotFn(Context::restoreSnapshot)
                            .destroyFn(Context::destroy)
                            .build();
    }

    private static class Context {

        private final SourceConnector connector;
        private final SourceTask task;
        private final Map<String, String> taskConfig;

        private boolean taskInit = false;

        public Context(Processor.Context ctx, Map<String, String> config) {
            try {
                Class<?> connectorClass = Class.forName(config.get("connector.class"));
                connector = (SourceConnector) connectorClass.getConstructor().newInstance();
                connector.initialize(new JetConnectorContext());
                connector.start(config);

                taskConfig = connector.taskConfigs(1).get(0);
                task = (SourceTask) connector.taskClass().getConstructor().newInstance();

            } catch (Exception e) {
                throw rethrow(e);
            }
        }

        public void fillBuffer(TimestampedSourceBuffer<SourceRecord> buf) {
            if (!taskInit) {
                task.initialize(new JetSourceTaskContext());
                task.start(taskConfig);
                taskInit = true;
            }
            try {
                List<SourceRecord> records = task.poll();
                if (records == null) {
                    return;
                }

                for (SourceRecord record : records) {
                    long ts = record.timestamp() == null ?  0 :
                            record.timestamp();
                    buf.add(record, ts);
                }
            } catch (InterruptedException e) {
                throw rethrow(e);
            }
        }

        public void destroy() {
            try {
                task.stop();
            } finally {
                connector.stop();
            }
        }

        public Object createSnapshot() {
            return null;
        }

        public void restoreSnapshot(List<Object> snapshots) {

        }

        private static class JetConnectorContext implements ConnectorContext {
            @Override
            public void requestTaskReconfiguration() {
                //TODO;
                System.out.println("requestTaskReconfiguration");
            }

            @Override
            public void raiseError(Exception e) {
                //TODO
                System.out.println(e);
            }
        }

        private class SourceOffsetStorageReader implements OffsetStorageReader {
            @Override
            public <T> Map<String, Object> offset(Map<String, T> partition) {
                return offsets(Collections.singletonList(partition)).get(partition);
            }

            @Override
            public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
                Map<Map<String, T>, Map<String, Object>> map = new HashMap<>();
                for (Map<String, T> partition : partitions) {
                    //TODO, these should be saved / restored from snapshot
                    map.put(partition, null);
                }
                return map;
            }
        }

        private class JetSourceTaskContext implements SourceTaskContext {
            @Override
            public Map<String, String> configs() {
                return taskConfig;
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new SourceOffsetStorageReader();
            }
        }
    }

    //TODO, what is this useful for?
    public static class DBHistory extends AbstractDatabaseHistory {

        @Override
        protected void storeRecord(HistoryRecord historyRecord) throws DatabaseHistoryException {
            System.out.println("storeHistoryRecord: " + historyRecord);
        }

        @Override
        protected void recoverRecords(Consumer<HistoryRecord> consumer) {
            System.out.println("recoverRecords:");
        }

        @Override
        public boolean exists() {
            return true;
        }
    }
}
