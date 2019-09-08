package info.jerrinot.jet.cdc.support;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;

import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class Assertions {
    private Assertions() {

    }

    public static void assertEqualsEventually(long expected, IAtomicLong atomicLong) {
        int timeoutSeconds = 20;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() <= deadline) {
            long currentValue = atomicLong.get();
            if (expected == currentValue) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
        throw new AssertionError(atomicLong + " wasn't set to " + expected + " within timeout of "
                + timeoutSeconds + " seconds");
    }

    public static void assertPipelineCompletion(Job job) {
        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but instead completed normally" );
        } catch (CompletionException e) {
            assertTrue(e.toString().contains(AssertionCompletedException.class.getName()));
        }
    }
}
