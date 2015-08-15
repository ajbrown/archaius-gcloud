package com.netflix.config.source;

import com.google.api.services.datastore.DatastoreV1.EntityResult;
import com.google.api.services.datastore.DatastoreV1.Key;
import com.google.api.services.datastore.DatastoreV1.LookupRequest;
import com.google.api.services.datastore.DatastoreV1.LookupResponse;
import com.google.api.services.datastore.DatastoreV1.Property;
import com.google.api.services.datastore.DatastoreV1.Value;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreException;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.config.PollResult;
import com.netflix.config.PolledConfigurationSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.google.api.services.datastore.client.DatastoreHelper.makeKey;

/**
 * @author A.J. Brown <aj@ajbrown.org>
 */
public class GCloudDatastoreConfigurationSource implements PolledConfigurationSource {
    private static final Logger log = LoggerFactory.getLogger(GCloudDatastoreConfigurationSource.class);

    protected final Datastore datastore;

    //Property names
    public static final String configEntityKindPropertyName = "com.netflix.config.gcloud.configEntityKind";
    public static final String configEntityKeyPropertyName = "com.netflix.config.gcloud.configEntityKey";

    //Property defaults
    public static final String defaultConfigEntityKind = "ArchaiusProperties";
    public static final String defaultConfigEntityKey  = "latest";

    //Dynamic Properties
    protected DynamicStringProperty configEntityKind = DynamicPropertyFactory.getInstance()
            .getStringProperty(configEntityKindPropertyName, defaultConfigEntityKind );

    protected DynamicStringProperty configEntityKey = DynamicPropertyFactory.getInstance()
            .getStringProperty(configEntityKeyPropertyName, defaultConfigEntityKey);


    public GCloudDatastoreConfigurationSource( Datastore datastore ) {
        this.datastore = datastore;
    }

    @Override
    public PollResult poll(boolean initial, Object checkPoint) throws Exception {

        Map<String,Object> properties = loadPropertiesFromEntity( configEntityKind.get(), configEntityKey.get() );

        log.info( "Successfully loaded new configuration from GCloud Datastore." );

        return PollResult.createFull( properties );
    }

    /**
     * Get the config entity kind for this source.
     * @return
     */
    public String getConfigEntityKind() {
        return this.configEntityKind.getValue();
    }

    /**
     * Get the configuration entity key for this source.
     * @return
     */
    public String getConfigEntityKey() {
        return this.configEntityKey.getValue();
    }

    /**
     * Load the configuration from a single Datastore entity of the specified kind and key.  Each property of the entity
     * will be treated as a configuration property key/name pair.
     *
     * @param entityKind
     * @param entityKey
     * @return
     * @throws DatastoreException
     */
    protected synchronized Map<String, Object> loadPropertiesFromEntity( String entityKind, String entityKey ) throws DatastoreException {
        Map<String, Object> properties = new HashMap<String, Object>();

        Key key = makeKey( entityKind, entityKey ).build();
        LookupRequest request = LookupRequest.newBuilder().addKey(key).build();

        log.debug( "Attempting configuration lookup using GCloud Datastore entity with kind '{}' and key '{}'", entityKey, entityKey );

        LookupResponse response = datastore.lookup( request );

        if( response.getFoundCount() == 0 ) {
            log.warn( "Could not find archaius configuration entity of kind '{}' with key '{}'.", entityKey, entityKey );
            return properties;
        }

        EntityResult result = response.getFound(0);
        if( !result.hasEntity() ) {
            log.warn( "Archaius entity lookup of kind '{}' with key '{}' succeeded, but no entity returned.", entityKind, entityKey );
            return properties;
        }

        for( Property prop : result.getEntity().getPropertyList() ) {
            if( !prop.hasValue() || prop.getValue().hasKeyValue() ) {
                continue;
            }

            properties.put( prop.getName(), getPropertyValue( prop ) );
        }

        return properties;
    }

    /**
     * Determine the correct typed property value to return.
     *
     * @param property
     * @return
     */
    protected Object getPropertyValue( Property property ) {
        if( !property.hasValue() ) {
            return null;
        }

        Value val = property.getValue();

        if ( val.hasBooleanValue() ) return val.getBooleanValue();
        if ( val.hasIntegerValue() ) return val.getIntegerValue();
        if ( val.hasDoubleValue() )  return val.getDoubleValue();
        if ( val.hasStringValue() ) return val.getStringValue();
        if ( val.hasTimestampMicrosecondsValue() ) return  val.getTimestampMicrosecondsValue();

        //Unsupported value type.
        return null;
    }
}
