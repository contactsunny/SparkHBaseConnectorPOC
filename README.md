# SparkHBaseConnectorPOC

This is a simple Sping Boot command line runner project, which creates a Spark Context, connects to an HBase database, and load the data into a SQLContext. From there, it's the regular SparkSQL stuff.

## The Spark HBase Connector Dependency

The ```shc-core``` dependency is available in th Hortonworks maven repository. You can find the source code and the usage [here](https://github.com/hortonworks-spark/shc).

## The Hortonworks Maven Repository

You can include the Hortonworks maven repository in your project with the following in your ```pom.xml``` file.

```xml
<repositories>
    <repository>
        <id>repository.hortonworks</id>
        <name>Hortonworks Repository</name>
        <url>http://repo.hortonworks.com/content/repositories/releases/</url>
    </repository>
</repositories>
```
