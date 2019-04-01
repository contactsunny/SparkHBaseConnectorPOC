package com.contactsunny.poc.SparkHBaseConnectorPOC;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class App implements CommandLineRunner {

    private static final Logger logger = Logger.getLogger(App.class);

    @Value("${spark.master}")
    private String sparkMaster;

    @Value("${spark.hbase.host}")
    private String sparkHbaseHost;

    @Value("${spark.hbase.port}")
    private String sparkHbasePort;

    public static void main( String[] args )
    {
        SpringApplication.run(App.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("SparkHbaseConnectorPOC")
                .setMaster(sparkMaster);

        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        javaSparkContext.hadoopConfiguration().set("spark.hbase.host", sparkHbaseHost);
        javaSparkContext.hadoopConfiguration().set("spark.hbase.port", sparkHbasePort);

        SQLContext sqlContext = new SQLContext(javaSparkContext);

        String catalog = "{\n" +
                "\t\"table\":{\"namespace\":\"default\", \"name\":\"test\", \"tableCoder\":\"PrimitiveType\"},\n" +
                "    \"rowkey\":\"key\",\n" +
                "    \"columns\":{\n" +
                "\t    \"rowkey\":{\"cf\":\"rowkey\", \"col\":\"key\", \"type\":\"string\"},\n" +
                "\t    \"value\":{\"cf\":\"value\", \"col\":\"value\", \"type\":\"string\"}\n" +
                "    }\n" +
                "}";

        Map<String, String> optionsMap = new HashMap<>();

        String htc = HBaseTableCatalog.tableCatalog();

        optionsMap.put(htc, catalog);
//        optionsMap.put(HBaseRelation.MIN_STAMP(), "123");
//        optionsMap.put(HBaseRelation.MAX_STAMP(), "456");

        Dataset dataset = sqlContext.read().options(optionsMap)
                .format("org.apache.spark.sql.execution.datasources.hbase").load();

        Dataset filteredDataset = dataset.filter(
                dataset.col("rowkey").$greater$eq("1")
                        .and(dataset.col("rowkey").$less$eq("10")))
                .filter(
                        dataset.col("rowKey").$greater$eq("3")
                                .and(dataset.col("rowkey").$less$eq("30"))
                )
                .select("rowkey");

        System.out.println("Count: " + filteredDataset.count());
        System.out.println("List: " + filteredDataset.collect());

    }
}
