package org.ajbrown.archaius.gcloud.test;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.datastore.DatastoreV1.CommitRequest;
import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.Key;
import com.google.api.services.datastore.DatastoreV1.LookupRequest;
import com.google.api.services.datastore.DatastoreV1.LookupResponse;
import com.google.api.services.datastore.DatastoreV1.Mutation;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreException;
import com.google.api.services.datastore.client.DatastoreFactory;
import com.google.api.services.datastore.client.DatastoreHelper;
import com.netflix.config.AbstractPollingScheduler;
import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicConfiguration;
import com.netflix.config.DynamicLongProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.config.FixedDelayPollingScheduler;
import com.netflix.config.PolledConfigurationSource;
import com.netflix.config.source.GCloudDatastoreConfigurationSource;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.Collections;

import static com.google.api.services.datastore.client.DatastoreHelper.makeProperty;
import static com.google.api.services.datastore.client.DatastoreHelper.makeValue;

/**
 * @author A.J. Brown <aj@ajbrown.org>
 */
public class TestServlet extends HttpServlet {

    final DynamicStringProperty foo  = DynamicPropertyFactory.getInstance().getStringProperty("foo", null);
    final DynamicLongProperty bar    = DynamicPropertyFactory.getInstance().getLongProperty("foo.bar", 0L);
    final DynamicBooleanProperty baz = DynamicPropertyFactory.getInstance().getBooleanProperty( "baz", false );

    final Datastore datastore;
    final DynamicConfiguration configuration;

    public TestServlet() {
        try {

            Collection<String> scopes = Collections.singleton("https://www.googleapis.com/auth/devstorage.full_control");
            datastore = DatastoreFactory.get().create(DatastoreHelper.getOptionsfromEnv()
                            .dataset("archaius-gcloud-test")
                            .credential(GoogleCredential.getApplicationDefault().createScoped(scopes))
                            .build()
            );
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException( "Could not initialize datastore.", e );
        } catch (IOException e) {
            throw new IllegalStateException( "Could not initialize datastore.", e );
        }

        PolledConfigurationSource source  = new GCloudDatastoreConfigurationSource( datastore );
        AbstractPollingScheduler scheduler = new FixedDelayPollingScheduler( 1000, 5000, false );

        configuration = new DynamicConfiguration( source, scheduler );
        ConfigurationManager.install( configuration );
    }


    @Override
    public void doGet( HttpServletRequest req, HttpServletResponse resp ) throws IOException {

        String fooValue = foo.getValue();
        Long barValue   = bar.getValue();
        Boolean bazValue = baz.getValue();

        PrintWriter writer = resp.getWriter();

        writer.append("foo = ").append(fooValue).append("\n");
        writer.append("foo.bar = ").append(barValue.toString()).append("\n");
        writer.append("baz = ").append(bazValue.toString()).append("\n");

        resp.setStatus( 200 );

        writer.flush();
        writer.close();
    }

    @Override
    public void doPost( HttpServletRequest req, HttpServletResponse resp ) throws IOException {

        //Get values from post request.
        String foo = req.getParameter( "foo" );
        Long bar = Long.parseLong( req.getParameter( "foo.bar" ) );
        Boolean baz = Boolean.parseBoolean( req.getParameter( "baz") );

        //Lookup cofiguration entity in Google DataStore
        GCloudDatastoreConfigurationSource source = (GCloudDatastoreConfigurationSource) configuration.getSource();
        Key key = DatastoreHelper.makeKey( source.getConfigEntityKind(), source.getConfigEntityKey() ).build();
        LookupRequest lookupRequest = LookupRequest.newBuilder().addKey(key).build();
        LookupResponse lookupResponse = null;

        try {
            lookupResponse = datastore.lookup( lookupRequest );
        } catch (DatastoreException e) {
            e.printStackTrace(resp.getWriter());
            resp.setStatus(500);
            resp.getWriter().flush();
            return;
        }

        Entity entity  = null;

        if ( lookupResponse.getFoundCount() > 0 ) {
             entity = lookupResponse.getFound( 0 ).getEntity();
        }

        if( entity == null ) {
            entity = Entity.newBuilder().setKey( key ).build();
        }

        entity.toBuilder()
                .addProperty( makeProperty("foo", makeValue( foo ) ) )
                .addProperty( makeProperty("foo.bar", makeValue( bar ) ) )
                .addProperty( makeProperty("baz", makeValue( baz ) ) )
        ;

        //Save the configuration entity
        Mutation.Builder mutation = Mutation.newBuilder();
        mutation.addUpsert( entity );

        CommitRequest commit = CommitRequest.newBuilder()
                .setMutation(mutation)
                .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
                .build();

        try {
            datastore.commit( commit );
        } catch (DatastoreException e) {
            e.printStackTrace(resp.getWriter());
            resp.setStatus(500);
            resp.getWriter().flush();
            return;
        }

        resp.setStatus( 200 );
    }

}
