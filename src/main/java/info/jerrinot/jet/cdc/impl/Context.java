package info.jerrinot.jet.cdc.impl;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.util.UuidUtil;
import io.debezium.config.Configuration;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.spi.OffsetCommitPolicy;
import io.debezium.relational.history.HistoryRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

public final class Context {
    static {
        UglyHacks.addJobIdFieldIntoEmbeddedConfig();
    }


    private OffsetSnapshot latestOfferedSnapshot;
    private OffsetSnapshot latestCompletedSnapshot;

    private final Queue<Object> eventQueue = new ConcurrentLinkedQueue<>();
    private final JsonConverter jsonConverter = new JsonConverter();
    private final EmbeddedEngine engine;
    private final String uuid;
    private ExecutorService executorService;
    private List<String> historyRecordEvents = new CopyOnWriteArrayList<>();
    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();

    public Context(String connectorClassname, String host, int port, String username, String password) {
        this.uuid = UuidUtil.newSecureUuidString();
        SillyRouter.registerContext(uuid, this);

        Map<String, String> jsonConvertorConfig = Map.of(
                "converter.type", "value",
                "schemas.enable", "false");
        jsonConverter.configure(jsonConvertorConfig);

        Configuration config = Configuration.create()
                .with(Constants.HISTORY_JOB_ID_KEY, uuid)
                .with(Constants.JOB_ID_KEY, uuid)
                .with("connector.class", connectorClassname)
                .with("offset.storage", SillyOffsetBackingStore.class.getName())

                // todo - externalize
                .with("name", "my-sql-connector")
                .with("database.hostname", host)
                .with("database.port", port)
                .with("database.user", username)
                .with("database.password", password)

                // todo - externalize
                .with("database.server.id", 85744)

                // todo - externalize
                .with("database.server.name", "my-app-connector")
                .with("database.history", SillyDatabaseHistory.class.getName())
                .build();

        engine = EmbeddedEngine.create()
                .using(config)
                .using(OffsetCommitPolicy.always())
                .notifying(eventQueue::offer)
                .build();
    }

    public void populateBuffer(SourceBuilder.TimestampedSourceBuffer<String> buffer) {
        if (!engine.isRunning()) {
            executorService = Executors.newSingleThreadExecutor();
            executorService.execute(engine);
        }

        Object item;
        while ((item = eventQueue.poll()) != null) {
            if (UglyHacks.shouldSkipMessage(item)) {
                continue;
            }
            if (item instanceof SourceRecord) {
                SourceRecord sourceRecord = (SourceRecord) item;
                Struct value = (Struct) sourceRecord.value();
                byte[] jsonBlob = jsonConverter.fromConnectData("ignored", sourceRecord.valueSchema(), value);
                Long ts = value.getInt64("ts_ms");
                buffer.add(new String(jsonBlob), ts);
            } else if (item instanceof OffsetSnapshot) {
                latestOfferedSnapshot = (OffsetSnapshot) item;
            } else if (item instanceof HistoryRecordEvent) {
                HistoryRecord record = ((HistoryRecordEvent) item).getRecord();
                Document document = record.document();
                try {
                    String s = writer.write(document);
                    historyRecordEvents.add(s);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new UnsupportedOperationException("Received unknown event " + item);
            }
        }
    }

    public void stop() {
        engine.stop();
        executorService.shutdownNow();
        SillyRouter.removeContext(uuid);
    }

    public Tuple2<OffsetSnapshot, List<String>> snapshotToJet() {
        OffsetSnapshot snapshot = latestOfferedSnapshot;
        Tuple2<OffsetSnapshot, List<String>> tuple = tuple2(snapshot, historyRecordEvents);
        latestCompletedSnapshot = snapshot;
        return tuple;
    }

    public void restoreFromJetSnapshot(List<Tuple2<OffsetSnapshot, List<String>>> objects) {
        if (objects.isEmpty()) {
            latestCompletedSnapshot = null;
            historyRecordEvents.clear();
            return;
        }
        Tuple2<OffsetSnapshot, List<String>> tuple = objects.get(0);
        latestCompletedSnapshot = tuple.f0();
        latestOfferedSnapshot = latestCompletedSnapshot;
        historyRecordEvents = tuple.f1();
    }


    void enqueueSnapshot(OffsetSnapshot snapshot) {
        eventQueue.offer(snapshot);
    }

    OffsetSnapshot restoreSnapshot() {
        return latestCompletedSnapshot;
    }

    void enqueueHistoryEvent(HistoryRecordEvent recordEvent) {
        eventQueue.offer(recordEvent);
    }

    void recoverHistoryEvents(Consumer<HistoryRecord> recordConsumer) {
        for (String s : historyRecordEvents) {
            try {
                Document doc = reader.read(s);
                HistoryRecord historyRecord = new HistoryRecord(doc);
                recordConsumer.accept(historyRecord);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    boolean hasHistoryRecords() {
        return !historyRecordEvents.isEmpty();
    }
}
