package io.jegutierrez.resources;

import java.util.ArrayList;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jegutierrez.core.DataKeeperClusterInfo;

@Path("/cluster")
@Produces(MediaType.APPLICATION_JSON)
public class ClusterResource {
    private static final Logger log = LoggerFactory.getLogger(ClusterResource.class);
    private DataKeeperClusterInfo clusterInfo;

    public ClusterResource(DataKeeperClusterInfo clusterInfo) {
        assert clusterInfo != null;
        this.clusterInfo = clusterInfo;
    }

    @GET
    @Timed
    @Path("/status")
    public Response getClusterInfo() {
        return Response.status(Status.OK).entity(clusterInfo.getLiveNodesMapData()).build();
    }

    @GET
    @Timed
    @Path("/leader")
    public Response getClusterLeader() {
        if(clusterInfo.getLeaderHostName() == null) {
            log.error("could find an elected leader");
            throw new WebApplicationException("could find an elected leader", Status.NOT_FOUND); 
        }
        return Response.status(Status.OK).entity(clusterInfo.getLeaderMapData()).build();
    }

}