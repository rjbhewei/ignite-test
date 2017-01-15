package com.shuyun.demo;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.shuyun.demo.ignite.hbase.Organization;
import com.shuyun.demo.ignite.hbase.OrganizationStore;
import com.shuyun.demo.ignite.jdbc.Person;
import com.shuyun.demo.ignite.jdbc.PersonStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.*;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.QueryCursor;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author hewei
 * @version 5.0
 * @date 2017/1/8  15:28
 * @desc
 */
@Slf4j
public class Launch2 {

    private static final String ORGANIZATION_CACHE_NAME = "organizationCache";

    private static final String PERSON_CACHE_NAME = "personCache";

    public static void main(String[] args) throws IgniteException, IgniteCheckedException {

        try(Ignite ignite = Ignition.start(getConfiguration())) {

            CacheConfiguration<Long, Organization> organizationCacheCfg = getOrganizationCache();

            CacheConfiguration<Long, Person> personCacheCfg = getPersonCache();

            try (IgniteCache<Long, Organization> organizationCache = ignite.getOrCreateCache(organizationCacheCfg);
                 IgniteCache<Long, Person> personCache = ignite.getOrCreateCache(personCacheCfg)) {

                CompletionListenerFuture listener = new CompletionListenerFuture();

                organizationCache.loadAll(Sets.newHashSet(5L,1L,3L,4L),true,listener);

                while(!listener.isDone()){
                    try {
                        Thread.sleep(100);
                    } catch(InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                QueryCursor<List<?>> cursor = organizationCache.query(new SqlFieldsQuery("select id, name ,city from organization"));

                log.info("organizationCache-----{}", cursor.getAll());

                personCache.loadCache(null);

                SqlQuery<Long,Person> sql = new SqlQuery<>(Person.class, "from person");

                personCache.query(sql);

                log.info("personCache-----{}", personCache.query(sql).getAll());

                select(personCache);

            }

        }

    }

    private static IgniteConfiguration getConfiguration() throws IgniteCheckedException {

        URL logUrl = ClassLoader.getSystemClassLoader().getResource("log4j2.xml");

        if(logUrl == null) {
            System.exit(0);
        }

        System.setProperty("log4j.configuration", logUrl.getPath());

        return new IgniteConfiguration()
                .setGridLogger(new Log4J2Logger(logUrl))
                .setPeerClassLoadingEnabled(true)
                .setClientMode(true)
                .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryMulticastIpFinder(){{
                    setAddresses(Lists.newArrayList("172.18.53.124","172.18.2.40"));
                }}));
    }

    private static void select(IgniteCache<Long, Person> personCache) {

        String sql = "select p.id, p.name, p.salary  from person as p";

        List<List<?>> res = personCache.query(new SqlFieldsQuery(sql).setDistributedJoins(true)).getAll();

        log.info("-----1----{}",res.size());

        sql = "select o.name, o.city from \"" + ORGANIZATION_CACHE_NAME + "\".organization as o" ;

        res = personCache.query(new SqlFieldsQuery(sql).setDistributedJoins(true)).getAll();

        log.info("-----2----{}",res.size());

         sql =
                "select p.id, p.name, p.salary, p.orgId ,o.name ,o.city " +
                        "from person as p, \"" + ORGANIZATION_CACHE_NAME + "\".organization as o " +
                        "where p.orgId = o.id";

        res = personCache.query(new SqlFieldsQuery(sql).setDistributedJoins(true)).getAll();

        log.info("-----3----{}",res.size());

        for (Object next : res)
            log.info(">>>next     {}", next);
    }

    private static CacheConfiguration<Long, Person> getPersonCache(){

        QueryEntity entity = new QueryEntity(){{

            setKeyType(Long.class.getName());

            setValueType(Person.class.getName());

            setFields(new LinkedHashMap<String, String>(){{
                put("id", Long.class.getName());
                put("name", String.class.getName());
                put("orgId", Long.class.getName());
                put("salary", Integer.class.getName());
            }});

            setIndexes(new ArrayList<QueryIndex>(3){{
                add(new QueryIndex("id"));
                add(new QueryIndex("orgId"));
            }});
        }};

        CacheConfiguration<Long, Person> cacheCfg = new CacheConfiguration<Long, Person>(PERSON_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setCacheStoreFactory(FactoryBuilder.factoryOf(PersonStore.class))
                .setQueryEntities(Lists.newArrayList(entity));

        cacheCfg.setReadThrough(true).setWriteThrough(true);

        return cacheCfg;
    }


    private static CacheConfiguration<Long, Organization> getOrganizationCache(){

        QueryEntity entity=new QueryEntity(){{

            setKeyType(Long.class.getName());

            setValueType(Organization.class.getName());

            setFields(new LinkedHashMap<String, String>(){{
                put("id", Long.class.getName());
                put("name", String.class.getName());
                put("city", String.class.getName());
            }});

            setIndexes(new ArrayList<QueryIndex>(1){{
                add(new QueryIndex("id"));
            }});
        }};

        CacheConfiguration<Long, Organization> cacheCfg = new CacheConfiguration<Long, Organization>(ORGANIZATION_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setCacheStoreFactory(FactoryBuilder.factoryOf(OrganizationStore.class))
                //.setEvictionPolicy(new LruEvictionPolicy(2))
                .setQueryEntities(Lists.newArrayList(entity));

        cacheCfg.setReadThrough(true).setWriteThrough(true);

        //cacheCfg.setIndexedTypes(Long.class, Organization.class);

        return cacheCfg;
    }
}
