package info.jerrinot.jet.cdc;


import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.MySQLContainer;

import java.io.File;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;

public class SillyTest {
    private static final String SOURCE_NAME = "mysqlSource";
    private static final String JOB_NAME = "read from mysql";

    private static final String DB_HOST = "localhost";
    private static final int DB_PORT = 3306;
    private static final String DB_USERNAME = "root";
    private static final String DB_PASSWORD = "example";

    private static final int ALLOWED_LAG = 5000;
    private static final ProcessingGuarantee PROCESSING_GUARANTEE = EXACTLY_ONCE;
    private static final boolean USE_PERSISTENCE = false;

    private static final File HOT_RESTART_BASE = new File(System.getProperty("java.io.tmpdir"));
    private static final String HOT_RESTART_DIR_NAME = "hot-restart";

    @Rule
    public MySQLContainer mysql = new MySQLContainer()
            .withPassword("example")
            .withUsername("root")
            .withDatabaseName("debezium");

    @Test
    public void foo() {
        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(
                Debsource.mysql(SOURCE_NAME)
                        .host(mysql.getContainerIpAddress())
                        .port(mysql.getMappedPort(MySQLContainer.MYSQL_PORT))
                        .username(mysql.getUsername())
                        .password(mysql.getPassword())
                        .build())
                .withNativeTimestamps(ALLOWED_LAG)
                .drainTo(Sinks.logger());

        JobConfig jobConfig = new JobConfig()
                .setName(JOB_NAME)
                .setProcessingGuarantee(PROCESSING_GUARANTEE);

        JetInstance jetInstance = Jet.newJetInstance(createJetConfig());
        jetInstance.newJobIfAbsent(pipeline, jobConfig).join();
    }

    private static JetConfig createJetConfig() {
        JetConfig cfg = new JetConfig();

        if (USE_PERSISTENCE) {
            File baseDir = new File(HOT_RESTART_BASE, HOT_RESTART_DIR_NAME);
            baseDir.mkdir();
            cfg.getInstanceConfig().setLosslessRestartEnabled(true);
            cfg.getHazelcastConfig().getHotRestartPersistenceConfig()
                    .setEnabled(true)
                    .setBaseDir(baseDir);
        }
        return cfg;
    }
}