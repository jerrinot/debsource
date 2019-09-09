package info.jerrinot.jet.cdc.impl;

import io.debezium.embedded.EmbeddedEngine;

public final class Constants {
    public static final String HISTORY_JOB_ID_KEY = "database.history.job.id";

    // we have to re-used one of the existing fields configured at
    // https://github.com/jerrinot/debezium/blob/a41a43197ceebaf84e3e1c668c47a12664466cd0/debezium-embedded/src/main/java/io/debezium/embedded/EmbeddedEngine.java#L182-L184
    // otherwise the job id will not be copied into WorkerConfig at
    // https://github.com/jerrinot/debezium/blob/a41a43197ceebaf84e3e1c668c47a12664466cd0/debezium-embedded/src/main/java/io/debezium/embedded/EmbeddedEngine.java#L639-L642
    // and then the SillyOffsetBacking store won't find the job id.
    public static final String JOB_ID_KEY = EmbeddedEngine.OFFSET_STORAGE_FILE_FILENAME.name();

    private Constants() {

    }
}
