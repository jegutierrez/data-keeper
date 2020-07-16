package io.jegutierrez.resources;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jegutierrez.core.ClusterNode;
import io.jegutierrez.core.DataKeeperClusterInfo;
import io.jegutierrez.db.DatabaseRepository;

@Path("/data")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DatabaseResource {
    private static final Logger log = LoggerFactory.getLogger(DatabaseResource.class);
    private DatabaseRepository kvs;
    private ObjectMapper objectMapper;
    private DataKeeperClusterInfo clusterInfo;
    private HttpClient httpClient;
    private ExecutorService executor;

    public DatabaseResource(DatabaseRepository kvs, DataKeeperClusterInfo clusterInfo, HttpClient httpClient) {
        this.kvs = kvs;
        this.clusterInfo = clusterInfo;
        this.httpClient = httpClient;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        this.objectMapper.enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
        executor = Executors.newFixedThreadPool(5);
    }

    @GET
    @Timed
    @Path("/{key}")
    public Response getData(@PathParam("key") String key) {
        if (kvs.get(key) == null) {
            throw new WebApplicationException("key not found", Status.NOT_FOUND);
        }
        return Response.status(Response.Status.OK).entity(kvs.get(key)).type(MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Timed
    @Path("/sync")
    public Map<String, String> getData() {
        return kvs.getDataToSync();
    }

    @PUT
    @Timed
    @Path("/{key}")
    public Response putData(@PathParam("key") String key, @NotNull @Valid String value) throws IOException {
        try {
            objectMapper.readTree(value);
        } catch (JsonProcessingException e) {
            log.error("invalid json body given", e.getMessage());
            throw new WebApplicationException("invalid json body given", Status.BAD_REQUEST);
        }
        if (clusterInfo.imILeader()) {
            kvs.put(key, value);
            broadCastWriteToReplicas(key, value);
            log.info("data replicated successfully");
        } else {
            redirectWriteToMaster(key, value);
            log.info("data written successfully");
        }
        return Response.created(UriBuilder.fromResource(DatabaseResource.class).build(value)).build();
    }

    private void redirectWriteToMaster(String key, String value) throws ClientProtocolException, IOException {
        String url = String.format("http://%s:%d/data/%s", clusterInfo.getLeaderAddress(), clusterInfo.getLeaderPort(),
                key);
        HttpPut request = new HttpPut(url);
        request.setEntity(new StringEntity(value));
        request.addHeader("content-type", "application/json");
        HttpResponse response = httpClient.execute(request);

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_CREATED) {
            log.error("error redirecting write request to the leader " + statusCode + " "
                    + response.getStatusLine().getReasonPhrase());
            throw new WebApplicationException("could not redirect write to master");
        }
    }

    private void broadCastWriteToReplicas(String key, String value) throws ClientProtocolException, IOException {
        // broadcast writes to live replicas
        for (ClusterNode node : clusterInfo.getLiveNodes()) {
            if (node.getHostName().equals(clusterInfo.getNodeName())) {
                continue;
            }
            executor.submit(() -> {
                try {
                    String url = String.format("http://%s:%d/data/sync/%s", node.getAddress(), node.getPort(), key);
                    HttpPut request = new HttpPut(url);
                    request.setEntity(new StringEntity(value));
                    request.addHeader("content-type", "application/json");
                    HttpResponse response = httpClient.execute(request);

                    int statusCode = response.getStatusLine().getStatusCode();
                    if (statusCode != HttpStatus.SC_CREATED) {
                        log.error("error replicating write request to node " + node.getHostName() + " "
                                + response.getStatusLine().getReasonPhrase());
                    }
                } catch (IOException e) {
                    log.error("error replicating write request " + e.getMessage());
                }
            });
        }
    }

    @PUT
    @Timed
    @Path("/sync/{key}")
    public Response dataSync(@PathParam("key") String key, @NotNull @Valid String value) {
        kvs.put(key, value);
        return Response.created(UriBuilder.fromResource(DatabaseResource.class).build(value)).build();
    }
}