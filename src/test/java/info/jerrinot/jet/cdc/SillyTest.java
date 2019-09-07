package info.jerrinot.jet.cdc;


import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import info.jerrinot.jet.cdc.support.JdbcTemplate;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SillyTest {
    private static final String SOURCE_NAME = "mysqlSource";
    private static final String JOB_NAME = "read from mysql";

    private static final int DB_PORT = 3306;
    private static final String DB_USERNAME = "root";
    private static final String DB_PASSWORD = "example";
    private static String DB_NAME = "DEBSOURCE";

    private static final int ALLOWED_LAG = 5000;
    private static final ProcessingGuarantee PROCESSING_GUARANTEE = EXACTLY_ONCE;
    private static final boolean USE_PERSISTENCE = false;

    private static final File HOT_RESTART_BASE = new File(System.getProperty("java.io.tmpdir"));
    private static final String HOT_RESTART_DIR_NAME = "hot-restart";


    @Rule
    public GenericContainer container = new GenericContainer("mysql:latest")
            .withEnv("MYSQL_ROOT_PASSWORD", "example")
            .withExposedPorts(DB_PORT)
            .withCommand("--default-authentication-plugin=mysql_native_password");

    @Test
    public void smokeTest() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(container.getFirstMappedPort(), DB_USERNAME, DB_PASSWORD);

        jdbcTemplate.newDatabase(DB_NAME);
        jdbcTemplate.switchToDatabase(DB_NAME);
        jdbcTemplate.executeUpdate("CREATE TABLE users (\n" +
                "    id INT,\n" +
                "    firstname VARCHAR(255) NOT NULL,\n" +
                "    lastname VARCHAR(255) NOT NULL,\n" +
                "    PRIMARY KEY (id)\n" +
                ")");

        Sink<String> sink = AssertionSinks.assertCollectedEventually(60, received -> {
            assertEquals(1, received.size());
            String item = received.get(0);
            assertTrue(item.contains("{\"before\":null,\"after\":{\"id\":0,\"firstname\":\"Joe\",\"lastname\":\"Plumber\"}"));
        });
        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(
                Debsource.mysql(SOURCE_NAME)
                        .host(container.getContainerIpAddress())
                        .port(container.getMappedPort(DB_PORT))
                        .username(DB_USERNAME)
                        .password(DB_PASSWORD)
                        .build())
                .withNativeTimestamps(ALLOWED_LAG)
                .peek()
                .drainTo(sink);

        JobConfig jobConfig = new JobConfig()
                .setName(JOB_NAME)
                .setProcessingGuarantee(PROCESSING_GUARANTEE);

        JetInstance jetInstance = Jet.newJetInstance(createJetConfig());
        Job job = jetInstance.newJobIfAbsent(pipeline, jobConfig);

        jdbcTemplate.executeUpdate("insert into users (id, firstname, lastname) values (0, 'Joe', 'Plumber')");

        assertPipelineCompletion(job);
    }

    private static void assertPipelineCompletion(Job job) {
        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but instead completed normally" );
        } catch (CompletionException e) {
            assertTrue(e.toString().contains(AssertionCompletedException.class.getName()));
        }
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