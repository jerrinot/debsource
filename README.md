# debsource

- `docker-compose -f stack.yml up` (see `./src/test/resources/stack.yml`)
- Set `-Dhazelcast.enterprise.license.key=<license>` env. variable
- When running under Java9+ then add `--add-exports java.base/jdk.internal.ref=ALL-UNNAMED` to JVM params
- Start `Main` class

Docker starts MySQL Adminer at http://localhost:8080/ - add a new database, add table, new row and watch for Jet job output. 
