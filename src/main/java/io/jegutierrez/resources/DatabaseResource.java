package io.jegutierrez.resources;

import java.io.IOException;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jegutierrez.db.DatabaseRepository;

@Path("/data")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DatabaseResource {
    private static final Logger log = LoggerFactory.getLogger(DatabaseResource.class);
    DatabaseRepository kvs;
    ObjectMapper objectMapper;

    public DatabaseResource(DatabaseRepository kvs) {
        this.kvs = kvs;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        this.objectMapper.enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
    }

    @GET
    @Timed
    @Path("/{key}")
    public byte[] getData(@PathParam("key") String key) {
        final byte[] value = kvs.get(key);
        if(value == null) {
            throw new WebApplicationException("key not found", Status.NOT_FOUND);
        }
        return value;
    }

    @PUT
    @Timed
    @Path("/{key}")
    public Response putData(@PathParam("key") String key, @NotNull @Valid byte[] value) throws IOException {
        try {
            objectMapper.readTree(value);
        } catch (JsonProcessingException e) {
            log.error("invalid json body given", e.getMessage());
            throw new WebApplicationException("invalid json body given", Status.BAD_REQUEST); 
        }
        kvs.put(key, value);
        return Response.created(UriBuilder.fromResource(DatabaseResource.class).build(value)).build();
    }
}