# debsource - [Hazelcast Jet](https://github.com/hazelcast/hazelcast-jet) married with [Debezium](https://github.com/debezium/debezium)

Toying with Change Data Capture and Hazelcast Jet. Everything is in progress. 
[Example](https://github.com/jerrinot/debsource/blob/0135a784abaabdce5925dca9197a3740e42b5035/src/main/java/info/jerrinot/jet/cdc/Main.java#L34-L42):
```java
        pipeline.drawFrom(
                mysql(SOURCE_NAME)
                        .host(DB_HOST)
                        .port(DB_PORT)
                        .username(DB_USERNAME)
                        .password(DB_PASSWORD)
                        .build())
                .withNativeTimestamps(ALLOWED_LAG)
                .drainTo(logger());
```

- `docker-compose -f stack.yml up` (see `./src/test/resources/stack.yml`)
- Set `-Dhazelcast.enterprise.license.key=<license>` env. variable
- When running under Java9+ then add `--add-exports java.base/jdk.internal.ref=ALL-UNNAMED` to JVM params
- Start `Main` class

Docker starts MySQL Adminer at http://localhost:8080/ - add a new database, add a table, new rows and watch for Jet job output. 
