package info.jerrinot.jet.cdc;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Test;

import java.util.concurrent.CompletionException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestTest {
    @Test
    public void foo() {
        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(TestSources.itemStream(1))
                .withIngestionTimestamps()
                .drainTo(AssertionSinks.assertCollectedEventually(60, e -> {} /*always accepted*/));

        JetInstance instance = Jet.newJetInstance();

        Job job = instance.newJob(pipeline);

        try {
            job.join();
        } catch (CompletionException e) {
            assertTrue(e.toString().contains(AssertionCompletedException.class.getName()));
        }
    }

    private static <E> void assertInstanceOf(Class<E> expected, Object actual) {
        assertNotNull(actual);
        assertTrue(actual + " is not an instanceof " + expected.getName(), expected.isAssignableFrom(actual.getClass()));
    }
}
