package com.shuyun.demo.cluster;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.Ignition;

/**
 * @author hewei
 * @version 5.0
 * @date 2017/1/8  17:04
 * @desc
 */
public class IgniteClusterTest {

    public static void main(String[] args) {

        try(Ignite ignite = Ignition.start()) {
            IgniteCluster cluster = ignite.cluster();
            System.out.println(cluster);
            System.out.println(cluster.forRemotes().nodes().size());
        }

    }
}
