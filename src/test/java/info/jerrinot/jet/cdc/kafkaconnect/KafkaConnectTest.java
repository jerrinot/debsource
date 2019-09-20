package info.jerrinot.jet.cdc.kafkaconnect;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import io.debezium.config.Configuration;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaConnectTest {

    public static final Configuration CONFIG = Configuration
            .create()
            .with("name", "inventory-connector")
            .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
            /* begin connector properties */
            .with("database.hostname", "localhost")
            .with("database.port", 3306)
            .with("database.user", "debezium")
            .with("database.password", "dbz")
            .with("database.server.id", "184054")
            .with("database.server.name", "dbserver1")
            .with("database.whitelist", "inventory")
            .with("database.server.id", 184054)
            .with("database.server.name", "my-inventory-connector")
            .with("database.history", KafkaConnectSource.DBHistory.class.getName())
            .build();

    @Test
    public void testWithJet() {
        System.setProperty("hazelcast.logging.type", "slf4j");
        JetInstance jet = Jet.newJetInstance();

        Pipeline p = Pipeline.create();
        p.drawFrom(KafkaConnectSource.kafkaConnectStream(CONFIG.asMap()))
         .withNativeTimestamps(0)
         .drainTo(Sinks.logger());

        jet.newJob(p).join();
    }

    @Test
    public void testPlain() throws Exception {
        Map<String, String> map = CONFIG.asMap();

        Class<?> connectorClass = Class.forName(CONFIG.getString("connector.class"));
        SourceConnector connector = (SourceConnector) connectorClass.getConstructor().newInstance();
        connector.initialize(new ConnectorContext() {
            @Override
            public void requestTaskReconfiguration() {
                System.out.println("requestTaskReconfiguration");
            }

            @Override
            public void raiseError(Exception e) {
                System.out.println(e);
            }
        });
        connector.start(map);

        List<Map<String, String>> maps = connector.taskConfigs(1);
        SourceTask task = (SourceTask) connector.taskClass().getConstructor().newInstance();
        task.initialize(new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return maps.get(0);
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override
                    public <T> Map<String, Object> offset(Map<String, T> partition) {
                        System.out.println(Thread.currentThread().getName());
                        return offsets(Collections.singletonList(partition)).get(partition);
                    }

                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
                        Map<Map<String, T>, Map<String, Object>> map = new HashMap<>();
                        for (Map<String, T> partition : partitions) {
                            map.put(partition, null);
                        }
                        return map;
                    }
                };
            }
        });
        task.start(maps.get(0));
        while (true) {
            List<SourceRecord> records = task.poll();
            if (records != null) {
                System.out.println("Returned " + records.size());
                records.forEach(r -> {

                    System.out.println("Offset=" + r.sourceOffset());
                    System.out.println("Key=" + r.key());
                    System.out.println("Value=" + r.value());
                });
            } else {
                Thread.sleep(1000);
            }
        }
    }
}
