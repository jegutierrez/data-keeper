# DataKeeper

#### In memory fault tolerant key-value store, with a Leader-Replica model, built on top of zookeeper
---

#### Motivation
DataKeeper was built with learning purposes, its not intended to be used in production in any way.

#### Features

- In memory key value store, data will be lost when a node dies.
- Leader-Replica model, for every cluster there is a single Leader node and multiple Replicas. Every node on the cluster will be either Leader or Replica.
- A Leader election process is going to be executed when the cluster starts and when the current Leader is not longer available. In the last scenario a Replica node must be elected as Leader.
- The data must always be writen first in the Leader and the this will be responsible to replicate the data to all the live Replicas. If a write request is sent to a Replica, the request must be redirected to the Leader.
- Read requests could be served by any node in the cluster, including Replicas.
- Data will be strongly consistent in the Leader node and eventually consistent on Replica nodes.
- Zookeeper will be responsible to manage the leader election, keep the live nodes directory and current status info.
- Every DataKeeper node will mantain an in memory copy of the cluster status.


#### Features


#### Pre-requisites to run DataKeeper:
- Java 8+
- Zookeeper 3+

#### How to start the DataKeeper locally

1. Run zookeeper, for example: using docker:
```bash
docker run -p 2181:2181 -p 2888:2888 -p 3888:3888 -p 8087:8080 --name db-zookeeper --restart always -d zookeeper
```
2. Run `mvn clean install` to build the application.
3. To make it fault tolearant, let's run 3 instances of data keeper:

- As a parameter pass a zookeeper config file: `config.yml`
- To run a multiples nodes we must override multiple a couple of properties from the zookeeper config file:
    - `server.applicationConnectors[0].port`
    - `server.adminConnectors[0].port`

Run node 1:
```bash
java -Ddw.nodeName=node1.io '-Ddw.server.applicationConnectors[0].port=8080' '-Ddw.server.adminConnectors[0].port=8081' -jar target/data-keeper-1.0-SNAPSHOT.jar server config.yml
```

Run node 2:
```bash
java -Ddw.nodeName=node2.io '-Ddw.server.applicationConnectors[0].port=8082' '-Ddw.server.adminConnectors[0].port=8083' -jar target/data-keeper-1.0-SNAPSHOT.jar server config.yml
```

Run node 3:
```bash
java -Ddw.nodeName=node3.io '-Ddw.server.applicationConnectors[0].port=8084' '-Ddw.server.adminConnectors[0].port=8085' -jar target/data-keeper-1.0-SNAPSHOT.jar server config.yml
```

