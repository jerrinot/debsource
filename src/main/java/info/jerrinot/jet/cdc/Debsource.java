package info.jerrinot.jet.cdc;

import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import info.jerrinot.jet.cdc.impl.Context;

public final class Debsource {

    private Debsource() {

    }

    public static MySqlBuilder mysql(String name) {
        return new MySqlBuilder(name);
    }

    private static StreamSource<String> newSource(String name, String connectorClassname, String host, int port, String username, String password) {
        return SourceBuilder.timestampedStream(name, c -> new Context(connectorClassname, host, port, username, password))
                .fillBufferFn(Context::populateBuffer)
                .destroyFn(Context::stop)
                .createSnapshotFn(Context::snapshotToJet)
                .restoreSnapshotFn(Context::restoreFromJetSnapshot)
                .build();
    }

    public static class MySqlBuilder {
        private static final String MYSQL_CONNECTOR_CLASSNAME = "io.debezium.connector.mysql.MySqlConnector";

        private final String name;
        private String host;
        private int port;
        private String username;
        private String password;

        public MySqlBuilder(String name) {
            this.name = name;
        }

        public MySqlBuilder host(String host) {
            this.host = host;
            return this;
        }

        public MySqlBuilder port(int port) {
            this.port = port;
            return this;
        }

        public MySqlBuilder username(String username) {
            this.username = username;
            return this;
        }

        public MySqlBuilder password(String password) {
            this.password = password;
            return this;
        }

        public StreamSource<String> build() {
            return Debsource.newSource(name, MYSQL_CONNECTOR_CLASSNAME, host, port, username, password);
        }
    }
}
