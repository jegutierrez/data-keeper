package io.jegutierrez.resources;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/cluster")
@Produces(MediaType.APPLICATION_JSON)
public class ClusterResource {
    private static final Logger log = LoggerFactory.getLogger(ClusterResource.class);
    private ZooKeeper zk;

    public ClusterResource(ZooKeeper zk) {
        this.zk = zk;
    }

    @GET
    @Timed
    @Path("/status")
    public Response getClusterInfo() {
        log.debug("checking cluster info");
        Map<String, List<Map<String, String>>> status = new HashMap<>();
        try {
            List<String> nodes = new ArrayList<>();
            List<String> liveNodes = new ArrayList<>();
            if(zk.exists("/cluster/nodes", true) != null) {
                nodes = zk.getChildren("/cluster/nodes", false);
            }
            if(zk.exists("/cluster/live-nodes", true) != null) {
                liveNodes = zk.getChildren("/cluster/live-nodes", false);
            }
            List<Map<String, String>> nodesStatus = new ArrayList<>();
            for(String n: nodes) {
                boolean live = liveNodes.contains(n);
                nodesStatus.add(Map.of(n, live ? "live" : "down"));
            }
            status = Map.of("cluster-status", nodesStatus);
        } catch (KeeperException | InterruptedException e) {
            log.error("could not get live nodes ", e.getMessage());
            throw new WebApplicationException("could not get live nodes", Status.INTERNAL_SERVER_ERROR); 
        }
        return Response.status(Status.OK).entity(status).build();
    }

}