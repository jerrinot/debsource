package info.jerrinot.jet.cdc.support;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.StreamStage;

public final class PipelineSupport {

    private PipelineSupport() {

    }

    private static ContextFactory<IAtomicLong> atomicLongContext(String name) {
        return ContextFactory.withCreateFn((jet) -> jet.getHazelcastInstance().getAtomicLong(name));
    }

    public static <T> FunctionEx<StreamStage<T>, StreamStage<T>> incrementCounter(String name) {
        return new FunctionEx<StreamStage<T>, StreamStage<T>>() {
            ContextFactory<IAtomicLong> counterFactory = atomicLongContext(name);

            @Override
            public StreamStage<T> applyEx(StreamStage<T> stage) {
                return stage.mapUsingContext(counterFactory, (counter, item) -> {
                    counter.incrementAndGet();
                    return item;
                });
            }
        };
    }
}
