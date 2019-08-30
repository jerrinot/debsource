package info.jerrinot.jet.cdc.impl;

import io.debezium.config.Configuration;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;

import java.util.function.Consumer;

public class SillyDatabaseHistory extends AbstractDatabaseHistory {

    private Context context;

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator) {
        super.configure(config, comparator);
        String jobId = config.getString(Constants.HISTORY_JOB_ID_KEY);
        context = SillyRouter.getContext(jobId);
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        HistoryRecordEvent recordEvent = new HistoryRecordEvent(record);
        context.enqueueHistoryEvent(recordEvent);
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        context.recoverHistoryEvents(records);
    }

    @Override
    public boolean exists() {
        return context.hasHistoryRecords();
    }
}
