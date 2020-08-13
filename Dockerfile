FROM openjdk:11
COPY . /usr/src/myapp
WORKDIR /usr/src/myapp
CMD java -Ddw.nodeName=node1.io ‘-Ddw.server.applicationConnectors[0].port=8080’ ‘-Ddw.server.adminConnectors[0].port=8081’ -jar target/data-keeper-1.0-SNAPSHOT.jar server config.yml

EXPOSE 8080