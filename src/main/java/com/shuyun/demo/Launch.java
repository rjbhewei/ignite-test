//package com.shuyun.demo;
//
//import com.google.common.collect.Lists;
//import com.google.common.collect.Sets;
//import com.shuyun.demo.ignite.hbase.Organization;
//import com.shuyun.demo.ignite.hbase.OrganizationStore;
//import com.shuyun.demo.ignite.jdbc.Person;
//import com.shuyun.demo.ignite.jdbc.PersonStore;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.ignite.*;
//import org.apache.ignite.cache.CacheMode;
//import org.apache.ignite.cache.QueryEntity;
//import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
//import org.apache.ignite.cache.query.QueryCursor;
//import org.apache.ignite.cache.query.SqlFieldsQuery;
//import org.apache.ignite.cache.query.SqlQuery;
//import org.apache.ignite.configuration.CacheConfiguration;
//import org.apache.ignite.configuration.IgniteConfiguration;
//import org.apache.ignite.logger.log4j2.Log4J2Logger;
//import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
//import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
//
//import javax.cache.configuration.FactoryBuilder;
//import javax.cache.integration.CompletionListenerFuture;
//import java.net.URL;
//import java.util.LinkedHashMap;
//import java.util.List;
//
///**
// * @author hewei
// * @version 5.0
// * @date 2017/1/8  15:28
// * @desc
// */
//@Slf4j
//public class Launch {
//
//    public static void main(String[] args) throws IgniteException, IgniteCheckedException {
//
//        URL logUrl = ClassLoader.getSystemClassLoader().getResource("log4j2.xml");
//
//        if(logUrl == null) {
//            System.exit(0);
//        }
//
//        System.setProperty("log4j.configuration", logUrl.getPath());
//
//        IgniteLogger igniteLogger = new Log4J2Logger(logUrl);
//
//        IgniteConfiguration configuration = new IgniteConfiguration();
//
//        configuration.setGridLogger(igniteLogger);
//
//        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
//
//        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
//
//        ipFinder.setAddresses(Lists.newArrayList("172.18.53.124","172.18.2.40"));
//
//        discoverySpi.setIpFinder(ipFinder);
//
//        configuration.setDiscoverySpi(discoverySpi);
//
//        configuration.setPeerClassLoadingEnabled(true);
//
//        Ignition.setClientMode(true);
//
//        try(Ignite ignite = Ignition.start(configuration)) {
//
//
//            CacheConfiguration<Long, Organization> organizationCacheCfg = getOrganizationCache();
//
//            CacheConfiguration<Long, Person> personCacheCfg = getPersonCache();
//
//
//            try (IgniteCache<Long, Organization> organizationCache = ignite.getOrCreateCache(organizationCacheCfg);
//                 IgniteCache<Long, Person> personCache = ignite.getOrCreateCache(personCacheCfg)
//            ) {
//                CompletionListenerFuture listener = new CompletionListenerFuture();
//
//                organizationCache.loadAll(Sets.newHashSet(5L,1L,3L,4L),true,listener);
//
//                while(!listener.isDone()){
//                    try {
//                        Thread.sleep(100);
//                    } catch(InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//
//                QueryCursor<List<?>> cursor = organizationCache.query(new SqlFieldsQuery("select id, name ,city from organization"));
//
//                log.info("organizationCache-----{}", cursor.getAll());
//
//                personCache.loadCache(null);
//
//                SqlQuery<Long,Person> sql = new SqlQuery<>(Person.class, "from person");
//
//               personCache.query(sql);
//
//                log.info("personCache-----{}", personCache.query(sql).getAll());
//
//                select(personCache);
//
//            }
//
//        }
//
//    }
//
//    private static void select(IgniteCache<Long, Person> personCache) {
//        String s="organizationCache";
//        String sql =
//                "select p.id, p.name, p.salary ,o.name ,o.city " +
//                        "from person as p, \"" + s + "\".organization as o " +
//                        "where p.orgId = o.id";
//
//        List<List<?>> res = personCache.query(new SqlFieldsQuery(sql).setDistributedJoins(false)).getAll();
//
//        System.out.println("-----"+res.size());
//
//        for (Object next : res)
//            log.info(">>>next     {}", next);
//    }
//
//    private static CacheConfiguration<Long, Person> getPersonCache(){
//
//        CacheConfiguration<Long, Person> cacheCfg = new CacheConfiguration<>("personCache");
//
//        cacheCfg.setReadThrough(true);
//
//        cacheCfg.setWriteThrough(true);
//
//        cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(PersonStore.class));
//
//        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
//
//        QueryEntity entity = new QueryEntity();
//
//        entity.setKeyType(Long.class.getName());
//
//        entity.setValueType(Person.class.getName());
//
//        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
//
//        fields.put("id", Long.class.getName());
//
//        fields.put("name", String.class.getName());
//
//        fields.put("orgId", Long.class.getName());
//
//        fields.put("salary", Integer.class.getName());
//
//        entity.setFields(fields);
//
//        cacheCfg.setQueryEntities(Lists.newArrayList(entity));
//
//        cacheCfg.setIndexedTypes(Long.class, Person.class);
//
//        return cacheCfg;
//    }
//
//
//    private static CacheConfiguration<Long, Organization> getOrganizationCache(){
//
//        CacheConfiguration<Long, Organization> cacheCfg = new CacheConfiguration<>("organizationCache");
//
//        cacheCfg.setReadThrough(true);
//
//        cacheCfg.setWriteThrough(true);
//
//        cacheCfg.setEvictionPolicy(new LruEvictionPolicy(2)) ;
//
//        cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(OrganizationStore.class));
//
//        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
//
//        QueryEntity entity=new QueryEntity();
//
//        entity.setKeyType(Long.class.getName());
//
//        entity.setValueType(Organization.class.getName());
//
//        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
//
//        fields.put("id", Long.class.getName());
//
//        fields.put("name", String.class.getName());
//
//        fields.put("city", String.class.getName());
//
//        entity.setFields(fields);
//
//        cacheCfg.setQueryEntities(Lists.newArrayList(entity));
//
//        cacheCfg.setIndexedTypes(Long.class, OrganizationStore.class);
//
//        return cacheCfg;
//    }
//}
