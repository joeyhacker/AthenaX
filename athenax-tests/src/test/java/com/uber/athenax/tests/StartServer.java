package com.uber.athenax.tests;

import com.uber.athenax.backend.server.AthenaXConfiguration;
import com.uber.athenax.backend.server.MiniAthenaXCluster;
import com.uber.athenax.backend.server.ServerContext;
import com.uber.athenax.backend.server.WebServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Scanner;

import static com.uber.athenax.tests.ITestUtil.generateConf;

public class StartServer {

    private static final Logger LOG = LoggerFactory.getLogger(StartServer.class);

    public static void main(String[] args) {
        try {
//            MiniKafkaCluster kafkaCluster = new MiniKafkaCluster.Builder().newServer("0").build();
//            kafkaCluster.start();
//            setUpKafka(kafkaCluster);

            MiniAthenaXCluster cluster = new MiniAthenaXCluster("MiniAthenaXServer");
            cluster.start();

            AthenaXConfiguration conf = generateConf(cluster);
            ServerContext.INSTANCE.initialize(conf);
            ServerContext.INSTANCE.start();

            WebServer server = new WebServer(URI.create("http://localhost:18080"));
            server.start();

            LOG.info("AthenaX server listening on http://localhost:{}", server.port());

            Scanner sc = new Scanner(System.in);
            System.out.println(sc.nextLine());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
