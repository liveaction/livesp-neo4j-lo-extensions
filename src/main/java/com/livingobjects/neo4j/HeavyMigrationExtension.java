package com.livingobjects.neo4j;

import com.livingobjects.neo4j.migrations.Migration_2_00_0;
import org.neo4j.graphdb.GraphDatabaseService;

import javax.servlet.ServletException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Path("/migration")
public class HeavyMigrationExtension {
    private final GraphDatabaseService graphDb;

    private final Executor executor = Executors.newSingleThreadExecutor();

    public HeavyMigrationExtension(@Context GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    @GET
    @Path("v2_00_0")
    public Response v2_00_0() throws IOException, ServletException {
        executor.execute(() -> Migration_2_00_0.cleanUpgrade(graphDb));
        return Response.ok("Migration v2.00.0 started !").build();
    }
}
