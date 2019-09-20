package info.jerrinot.jet.cdc.kafkaconnect;

import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.HistoryRecord;

import java.util.function.Consumer;

//TODO, what is this useful for?
public class DBHistory extends AbstractDatabaseHistory {

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
