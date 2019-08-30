package info.jerrinot.jet.cdc.impl;

import io.debezium.relational.history.HistoryRecord;

final class HistoryRecordEvent {
    private final HistoryRecord record;

    HistoryRecordEvent(HistoryRecord record) {
        this.record = record;
    }

    HistoryRecord getRecord() {
        return record;
    }
}
