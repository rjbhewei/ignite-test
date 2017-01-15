package com.shuyun.demo.ignite.jdbc;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.*;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.log4j2.Log4J2Logger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import javax.cache.configuration.FactoryBuilder;
import java.io.IOException;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author hewei
 * @version 5.0
 * @date 2017/1/4  14:31
 * @desc
 */
@Slf4j
public class PersonStoreExample {

    public static void main(String[] args) throws IgniteException, IOException, IgniteCheckedException {

        URL logUrl = ClassLoader.getSystemClassLoader().getResource("log4j2.xml");

        if(logUrl == null) {
            System.exit(0);
        }

        System.setProperty("log4j.configuration", logUrl.getPath());

        IgniteLogger igniteLogger = new Log4J2Logger(logUrl);

        IgniteConfiguration configuration = new IgniteConfiguration();

        configuration.setGridLogger(igniteLogger);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Lists.newArrayList("172.18.53.124","172.18.2.40"));

        discoverySpi.setIpFinder(ipFinder);

        configuration.setDiscoverySpi(discoverySpi);

        configuration.setPeerClassLoadingEnabled(true);

        configuration.setClientMode(true);

        //Ignition.setClientMode(true);

        try(Ignite ignite = Ignition.start(configuration)) {

            CacheConfiguration<Long, Person> cacheCfg = new CacheConfiguration<>("personCache");

            cacheCfg.setReadThrough(true);

            cacheCfg.setWriteThrough(true);

            cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(PersonStore.class));

            cacheCfg.setCacheMode(CacheMode.PARTITIONED);

            QueryEntity entity = new QueryEntity();

            entity.setKeyType(Long.class.getName());

            entity.setValueType(Person.class.getName());

            LinkedHashMap<String, String> fields = new LinkedHashMap<>();

            fields.put("id", Long.class.getName());

            fields.put("name", String.class.getName());

            fields.put("orgId", Long.class.getName());

            fields.put("salary", Integer.class.getName());

            entity.setFields(fields);

            cacheCfg.setQueryEntities(Lists.newArrayList(entity));

            try(IgniteCache<Long, Person> cache = ignite.getOrCreateCache(cacheCfg)) {

                cache.loadCache(null);

                QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select id, name from person"));
                log.info("cursor-----{}", cursor.getAll());

                cursor = cache.query(new SqlFieldsQuery("select id, name from person"));
                log.info("cursor-----{}", cursor.getAll());

                cursor = cache.query(new SqlFieldsQuery("select id, name from person"));
                log.info("cursor-----{}", cursor.getAll());

                QueryMetrics metrics = cache.queryMetrics();
                log.info("metrics-----{}", metrics.toString());
            }

        }

    }

}
