package info.jerrinot.jet.cdc.kafkaconnect;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import io.debezium.config.Configuration;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;

import java.io.IOException;
import java.util.function.Consumer;

public class HazelcastDatabaseHistory extends AbstractDatabaseHistory {

    public static final String LIST_NAME = "database.history.hazelcast.list-name";
    public static final String HOSTS = "database.history.hazelcast.hosts";
    public static final String GROUP_NAME = "database.history.hazelcast.group";

    private String listName;
    private HazelcastInstance client;
    private ClientConfig clientConfig;
    private IList<byte[]> list;

    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();

    public HazelcastDatabaseHistory() {
    }

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator) {
        super.configure(config, comparator);
        clientConfig = new ClientConfig();
        String host = config.getString(HOSTS);
        if (host != null) {
            for (String addr : host.split(",")) {
                clientConfig.getNetworkConfig().addAddress(addr);
            }
        }
        String group = config.getString(GROUP_NAME);
        if (group != null) {
            clientConfig.getGroupConfig().setName(group);
        }

        listName = config.getString(LIST_NAME);
    }

    @Override
    public void start() {
        super.start();
        client = HazelcastClient.newHazelcastClient(clientConfig);
        list = client.getList(listName);
    }

    @Override
    protected void storeRecord(HistoryRecord historyRecord) throws DatabaseHistoryException {
        list.add(writer.writeAsBytes(historyRecord.document()));
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> consumer) {
        try {
            for (byte[] r : list) {
                Document doc = reader.read(r);
                consumer.accept(new HistoryRecord(doc));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        super.stop();
        client.shutdown();
    }

    @Override
    public boolean exists() {
        return !client.getList(listName).isEmpty();
    }

    @Override
    public void initializeStorage() {
        client.getList(listName).clear();
    }
}
