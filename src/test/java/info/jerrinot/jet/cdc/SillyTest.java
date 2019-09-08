package info.jerrinot.jet.cdc;


import com.hazelcast.core.IAtomicLong;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import info.jerrinot.jet.cdc.support.Assertions;
import info.jerrinot.jet.cdc.support.JdbcTemplate;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.util.List;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertCollectedEventually;
import static info.jerrinot.jet.cdc.support.PipelineSupport.incrementCounter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    private JdbcTemplate jdbcTemplate;

    @Rule
    public GenericContainer container = new GenericContainer("mysql:8.0.17")
            .withEnv("MYSQL_ROOT_PASSWORD", DB_PASSWORD)
            .withExposedPorts(DB_PORT)
            .withCommand("--default-authentication-plugin=mysql_native_password");

    @Before
    public void setUp() {
        jdbcTemplate = new JdbcTemplate(container.getFirstMappedPort(), DB_USERNAME, DB_PASSWORD);
        jdbcTemplate.newDatabase(DB_NAME);
        jdbcTemplate.switchToDatabase(DB_NAME);
        jdbcTemplate.executeUpdate("CREATE TABLE users (\n" +
                "    id INT,\n" +
                "    firstname VARCHAR(255) NOT NULL,\n" +
                "    lastname VARCHAR(255) NOT NULL,\n" +
                "    PRIMARY KEY (id)\n" +
                ")");
    }

    @After
    public void tearDown() throws Exception {
        jdbcTemplate.dropDatabase(DB_NAME);
        jdbcTemplate.close();
    }

    @Test
    public void smokeTest() {
        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(
                Debsource.mysql(SOURCE_NAME)
                        .host(container.getContainerIpAddress())
                        .port(container.getMappedPort(DB_PORT))
                        .username(DB_USERNAME)
                        .password(DB_PASSWORD)
                        .build())
                .withNativeTimestamps(ALLOWED_LAG)
                .apply(incrementCounter("counter"))
                .peek()
                .drainTo(assertCollectedEventually(30, SillyTest::assertAllUpdatesCollected));

        JobConfig jobConfig = new JobConfig()
                .setName(JOB_NAME)
                .setProcessingGuarantee(PROCESSING_GUARANTEE);

        JetInstance jetInstance = Jet.newJetInstance(createJetConfig());
        Job job = jetInstance.newJobIfAbsent(pipeline, jobConfig);

        jdbcTemplate.executeUpdate("insert into users (id, firstname, lastname) values (0, 'Joe', 'Plumber')");
        jdbcTemplate.executeUpdate("insert into users (id, firstname, lastname) values (42, 'Arthur', 'Dent')");

        // wait for first 2 inserts to be captured by Debezium
        // otherwise the initial mysql data snapshot might include deletes and updates from bellow
        IAtomicLong counter = jetInstance.getHazelcastInstance().getAtomicLong("counter");
        Assertions.assertEqualsEventually(2, counter);

        jdbcTemplate.executeUpdate("delete from users where id = ?", 0);
        jdbcTemplate.executeUpdate("update users set firstname = ?, lastname = ? where id = ?", "Ford", "Prefect", 42);

        Assertions.assertPipelineCompletion(job);
    }

    private static void assertAllUpdatesCollected(List<String> received) {
        assertEquals(4, received.size());
        String item = received.get(0);
        assertTrue(item.contains("{\"before\":null,\"after\":{\"id\":0,\"firstname\":\"Joe\",\"lastname\":\"Plumber\"}"));
        item = received.get(1);
        assertTrue(item.contains("{\"before\":null,\"after\":{\"id\":42,\"firstname\":\"Arthur\",\"lastname\":\"Dent\"}"));
        item = received.get(2);
        assertTrue(item.contains("{\"before\":{\"id\":0,\"firstname\":\"Joe\",\"lastname\":\"Plumber\"},\"after\":null,"));
        item = received.get(3);
        assertTrue(item.contains("{\"before\":{\"id\":42,\"firstname\":\"Arthur\",\"lastname\":\"Dent\"},\"after\":{\"id\":42,\"firstname\":\"Ford\",\"lastname\":\"Prefect\"}"));
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