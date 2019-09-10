package info.jerrinot.jet.cdc;

import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import info.jerrinot.jet.cdc.impl.Context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public final class Debsource {

    private Debsource() {

    }

    public static MySqlBuilder mysql(String name) {
        return new MySqlBuilder(name);
    }

    private static StreamSource<String> newSource(String name, String connectorClassname, String host, int port,
                                                  String username, String password, List<String> whitelist,
                                                  List<String> blacklist) {

        return SourceBuilder.timestampedStream(name,
                c -> new Context(connectorClassname, host, port, username, password, whitelist, blacklist))
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
        private List<String> whitelist;
        private List<String> blacklist;

        public MySqlBuilder(String name) {
            this.name = name;
        }

        public MySqlBuilder host(String host) {
            this.host = host;
            return this;
        }

        public MySqlBuilder whitelist(String database) {
            if (blacklist != null) {
                throw new IllegalStateException("Cannot add a database " + database + " to a whitelist" +
                        " because there is already a database blacklist defined: " + blacklist);
            }
            if (whitelist == null) {
                whitelist = new ArrayList<>();
            }
            whitelist.add(database);
            return this;
        }

        public MySqlBuilder blacklist(String database) {
            if (whitelist != null) {
                throw new IllegalStateException("Cannot add a database " + database + " to a blacklist" +
                        " because there is already a database whitelist defined: " + whitelist);
            }
            if (blacklist == null) {
                blacklist = new ArrayList<>();
            }
            blacklist.add(database);
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
            return Debsource.newSource(name, MYSQL_CONNECTOR_CLASSNAME, host, port, username, password,
                    whitelist, blacklist);
        }
    }
}
