package com.nttdata.poc;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.AbstractCityResponse;
import com.maxmind.geoip2.record.AbstractNamedRecord;
import org.apache.kafka.common.Configurable;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import io.confluent.ksql.function.udf.UdfParameter;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@UdfDescription(name = "getCityFromIp", description = "GeoIP Informations")
public class GetCityFromIpUdf implements Configurable {

    private DatabaseReader reader;
    private static final Logger log = LoggerFactory.getLogger(GetCityFromIpUdf.class);

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
            catch (IOException e) {
                log.error("Problem loading GeoIP database: ", e);
                throw new ExceptionInInitializerError(e);
            }
        }
    }


    @Udf(description = "Returns city from IP input")
    public String getCityFromIp(@UdfParameter(value = "ip",
            description = "the IP address to lookup in the geoip database") final String ip) {
        if (reader == null) {
            log.error("No DB configured");
            return null;
        }

        try {
            log.debug("Lookup up City for IP: " + ip);
            InetAddress ipAddress = InetAddress.getByName(ip);

            return reader.tryCity(ipAddress)
                    .map(AbstractCityResponse::getCity)
                    .map(AbstractNamedRecord::getName)
                    .orElse(null);
        }
        catch (IOException | GeoIp2Exception e) {
            log.error("Error looking up City for IP: ", e);
            return null;
        }
        catch (Exception e) {
            log.error("Generic Error looking up City for IP: ", e);
            return null;
        }
    }
}
