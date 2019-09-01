package info.jerrinot.jet.cdc.impl;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import java.lang.reflect.Field;

import static io.debezium.config.Field.create;

final class UglyHacks {
    private static final io.debezium.config.Field CONTEXT_JOB_ID = create(Constants.JOB_ID_KEY);

    static void addJobIdFieldIntoEmbeddedConfig() {
        try {
            Class<?> aClass = Class.forName("io.debezium.embedded.EmbeddedEngine$EmbeddedConfig");
            Field configField = aClass.getDeclaredField("CONFIG");
            configField.setAccessible(true);
            ConfigDef configDef = (ConfigDef) configField.get(null);

            if (configDef.names().contains(Constants.JOB_ID_KEY)) {
                return;
            }
            synchronized (UglyHacks.class) { //kill me and my classloader
                if (configDef.names().contains(Constants.JOB_ID_KEY)) {
                    return;
                }
                io.debezium.config.Field.group(configDef, null, CONTEXT_JOB_ID);
            }
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
            throw new AssertionError("Cannot hack EmbeddedConfig", e);
        }
    }
}
