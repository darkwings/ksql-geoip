
package io.geoip.udf;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.AbstractCityResponse;
import com.maxmind.geoip2.model.AbstractCountryResponse;
import com.maxmind.geoip2.model.DomainResponse;
import com.maxmind.geoip2.record.AbstractNamedRecord;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;


@UdfDescription(name = "getDataFromIp", description = "GeoIP Informations")
public class GetDataFromIpUdf implements Configurable {

    private DatabaseReader reader;

    private final Schema VALUE_SCHEMA = SchemaBuilder.struct().optional()
            .field("CITY", Schema.OPTIONAL_STRING_SCHEMA)
            .field("COUNTRY", Schema.OPTIONAL_STRING_SCHEMA)
            .field("LATITUDE", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("LONGITUDE", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .build();

    private static final Logger log = LoggerFactory.getLogger(GetDataFromIpUdf.class);

    @Override
    public void configure(Map<String, ?> props) {
        String geoIpCityPathConfig = "ksql.functions._global_.geocity.db.path";
        if (!props.containsKey(geoIpCityPathConfig)) {
            throw new ConfigException("Required property " + geoIpCityPathConfig + " not found!");
        }
        else {
            String geoCityDbPath = (String) props.get(geoIpCityPathConfig);
            try {
                File database = new File(geoCityDbPath);
                this.reader = (new DatabaseReader.Builder(database)).build();
                log.info("Loaded GeoIP database from " + geoCityDbPath);
            }
            catch (Exception e) {
                // e.printStackTrace();
                log.error("Problem loading GeoIP database: ", e);
                throw new ExceptionInInitializerError(e);
            }
        }
    }


    @Udf(description = "Returns location data from IP input",
        schema = "STRUCT<CITY STRING, COUNTRY STRING, LATITUDE DOUBLE, LONGITUDE DOUBLE>")
    public Struct getDataFromIp(@UdfParameter(value = "ip",
            description = "the IP address to lookup in the geoip database") String ip) {
        if (reader == null) {
            log.error("No DB configured");
            return null;
        }

        try {
            log.debug("Lookup up City for IP: " + ip);
            InetAddress ipAddress = InetAddress.getByName(ip);

            String city = reader.tryCity(ipAddress)
                    .map(AbstractCityResponse::getCity)
                    .map(AbstractNamedRecord::getName)
                    .orElse(null);

//            String domain = reader.tryDomain(ipAddress)
//                    .map(DomainResponse::getDomain)
//                    .orElse(null);

            String country = reader.tryCountry(ipAddress)
                    .map(AbstractCountryResponse::getCountry)
                    .map(AbstractNamedRecord::getName)
                    .orElse(null);

            Double[] location = reader.tryCity(ipAddress)
                    .map(AbstractCityResponse::getLocation)
                    .map(l -> new Double[]{l.getLatitude(), l.getLongitude()})
                    .orElse( new Double[]{0.0, 0.0});

            Struct s = new Struct(VALUE_SCHEMA);
            s.put("CITY", city)
                    .put("COUNTRY", country)
                    //.put("DOMAIN", domain)
                    .put("LATITUDE", location[0])
                    .put("LONGITUDE", location[1]);
            return s;
        }
        catch (IOException | GeoIp2Exception e) {
            // e.printStackTrace();
            log.error("Error looking up City for IP: ", e);
            return null;
        }
        catch (Exception e) {
            // e.printStackTrace();
            log.error("Generic Error looking up City for IP: ", e);
            return null;
        }
    }
}
