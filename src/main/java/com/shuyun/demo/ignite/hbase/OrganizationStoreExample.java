package com.shuyun.demo.ignite.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.*;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.log4j2.Log4J2Logger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CompletionListenerFuture;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author hewei
 * @version 5.0
 * @date 2017/1/4  14:31
 * @desc
 */
@Slf4j
public class OrganizationStoreExample {

    public static void main(String[] args) throws IgniteException, IgniteCheckedException {

        URL logUrl = ClassLoader.getSystemClassLoader().getResource("log4j2.xml");

        if(logUrl == null) {
            System.exit(0);
        }

        System.setProperty("log4j.configuration", logUrl.getPath());

        IgniteLogger igniteLogger = new Log4J2Logger(logUrl);

        IgniteConfiguration configuration = new IgniteConfiguration();

        configuration.setGridLogger(igniteLogger);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();

        ipFinder.setAddresses(Lists.newArrayList("172.18.53.124","172.18.2.40"));

        discoverySpi.setIpFinder(ipFinder);

        configuration.setDiscoverySpi(discoverySpi);

        configuration.setPeerClassLoadingEnabled(true);

        Ignition.setClientMode(true);

        try(Ignite ignite = Ignition.start(configuration)) {

            CacheConfiguration<Long, Organization> cacheCfg = new CacheConfiguration<>("organizationCache");

            cacheCfg.setReadThrough(true);

            cacheCfg.setWriteThrough(true);

            //cacheCfg.setEvictionPolicy(new LruEvictionPolicy(2)) ;

            cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(OrganizationStore.class));

            cacheCfg.setCacheMode(CacheMode.PARTITIONED);

            QueryEntity entity=new QueryEntity();

            entity.setKeyType(Long.class.getName());

            entity.setValueType(Organization.class.getName());

            LinkedHashMap<String, String> fields = new LinkedHashMap<>();

            fields.put("id", Long.class.getName());

            fields.put("name", String.class.getName());

            fields.put("city", String.class.getName());

            entity.setFields(fields);

            cacheCfg.setQueryEntities(Lists.newArrayList(entity));

            try (IgniteCache<Long, Organization> cache = ignite.getOrCreateCache(cacheCfg)) {

                CompletionListenerFuture listener = new CompletionListenerFuture();

                cache.loadAll(Sets.newHashSet(1L,2L,3L,4L),true,listener);

                while(!listener.isDone()){
                    try {
                        Thread.sleep(100);
                    } catch(InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select id, name from organization"));

                log.info("cursor-----{}", cursor.getAll());


                SqlQuery<Long,Organization> sql = new SqlQuery<>(Organization.class, "from organization");

                log.info("cursor-----{}", cache.query(sql).getAll());

                Collection<CacheEntry<Long, Organization>> cacheEntries = cache.getEntries(Sets.newHashSet(1L, 2L, 3L,4L));

                for(CacheEntry<Long, Organization> entry : cacheEntries) {
                    log.info("cursor-----{}", entry);
                }

                cursor = cache.query(new SqlFieldsQuery("select id, name from organization"));

                log.info("cursor-----{}", cursor.getAll());

                cacheEntries = cache.getEntries(Sets.newHashSet(1L, 2L, 3L));

                for(CacheEntry<Long, Organization> entry : cacheEntries) {
                    log.info("cursor-----{}", entry);
                }

                QueryMetrics metrics=cache.queryMetrics();

                log.info("metrics-----{}", metrics.toString());

            }

        }

    }

}
