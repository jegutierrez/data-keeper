package io.jegutierrez.resources;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public DatabaseResource(DatabaseRepository kvs, DataKeeperClusterInfo clusterInfo, HttpClient httpClient) {
        this.kvs = kvs;
        this.clusterInfo = clusterInfo;
        this.httpClient = httpClient;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        this.objectMapper.enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
    }

    @GET
    @Timed
    @Path("/{key}")
    public Map<String, String> getData(@PathParam("key") String key) {
        if (kvs.get(key) != null) {
            throw new WebApplicationException("key not found", Status.NOT_FOUND);
        }
        Map<String, String> value = new HashMap<>();
        value.put(key, new String(kvs.get(key)));
        return value;
    }

    @GET
    @Timed
    @Path("/sync")
    public Map<String, String> getData() {
        log.info("sync data request");
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
            // TODO: broadcast writes to live replicas
            // clusterInfo.getLiveNodes();
        } else {
            // redirect write to the leader node.
            String url = String.format("http://%s:%d/data/%s", clusterInfo.getLeaderAddress(),
                    clusterInfo.getLeaderPort(), key);
            HttpPut request = new HttpPut(url);
            request.setEntity(new StringEntity(value));
            request.addHeader("content-type", "application/json");
            HttpResponse response = httpClient.execute(request);

            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                log.error("error redirecting write request to the leader " + statusCode + " "
                        + response.getStatusLine().getReasonPhrase());
            }
            log.info("data writren successfuly");
        }
        return Response.created(UriBuilder.fromResource(DatabaseResource.class).build(value)).build();
    }
}