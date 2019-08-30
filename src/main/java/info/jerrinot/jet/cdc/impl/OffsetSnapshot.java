package info.jerrinot.jet.cdc.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class OffsetSnapshot implements DataSerializable {
    private final Map<ByteBuffer, ByteBuffer> maps;

    //0-arg constructor needed for (de-)serialization
    public OffsetSnapshot() {
        maps = new ConcurrentHashMap<>();
    }

    public OffsetSnapshot(Map<ByteBuffer, ByteBuffer> maps) {
        this.maps = maps;
    }

    public Map<ByteBuffer, ByteBuffer> getMaps() {
        return maps;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(maps.size());
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : maps.entrySet()) {
            out.writeByteArray(entry.getKey().array());
            out.writeByteArray(entry.getValue().array());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        maps.clear();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            byte[] keyBytes = in.readByteArray();
            byte[] valueBytes = in.readByteArray();
            maps.put(ByteBuffer.wrap(keyBytes), ByteBuffer.wrap(valueBytes));
        }
    }
}
