package com.livingobjects.neo4j;

import com.google.common.base.Joiner;
import com.livingobjects.neo4j.migrations.MigrationProgress;
import com.livingobjects.neo4j.migrations.Migration_2_00_0;
import org.eclipse.jetty.http.HttpStatus;
import org.neo4j.graphdb.GraphDatabaseService;

import javax.servlet.ServletException;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

@Path("/migration")
public class HeavyMigrationExtension {
    private final GraphDatabaseService graphDb;

    private final Executor executor = Executors.newSingleThreadExecutor();

    private static final AtomicReference<Migration_2_00_0> migration = new AtomicReference<>();

    public HeavyMigrationExtension(@Context GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    @GET
    @Path("v2_00_0")
    public Response v2_00_0() throws IOException, ServletException {
        executor.execute(() -> Migration_2_00_0.cleanUpgrade(graphDb));
        return Response.ok("Migration v2.00.0 started !").build();
    }

    @POST
    @Path("v2_00_0")
    public Response v2_00_0_dyn() throws IOException, ServletException {
        migration.getAndUpdate((x) -> {
            if (x != null && x.getProgression().getStatus() == HttpStatus.PARTIAL_CONTENT_206)
                return x;
            Migration_2_00_0 m2 = Migration_2_00_0.prepareMigration(graphDb);
            executor.execute(m2::cleanUpgrade);
            return m2;
        });
        return Response.status(Response.Status.ACCEPTED)
                .entity("Migration v2.00.0 started !")
                .build();
    }

    @GET
    @Path("status")
    public Response status() throws IOException, ServletException {
        Migration_2_00_0 m2 = migration.get();
        if (m2 == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        MigrationProgress progression = m2.getProgression();
        String messages = Joiner.on('\n').join(progression.getAllUnreadMessages());
        return Response.status(progression.getStatus())
                .entity(messages)
                .build();
    }
}
