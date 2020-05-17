package io.jegutierrez.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/node")
@Produces(MediaType.APPLICATION_JSON)
public class NodeResource {
    private static final Logger log = LoggerFactory.getLogger(NodeResource.class);

    @GET
    @Timed
    @Path("/info")
    public Response getClusterInfo() {
        log.debug("checking cluster info");
        String info = "{\"nodes\": \"[]\"}";
        return Response.status(Status.OK).entity(info).build();
    }

    @GET
    @Timed
    @Path("/status")
    public Response getClusterStatus() {
        log.debug("checking cluster status");
        String info = "{\"status\": \"healty\"}";
        return Response.status(Status.OK).entity(info).build();
    }

}