# SumVP
# 1. Overview
&ensp;&ensp;&ensp;&ensp;We propose a novel framework to accelerate complex SPARQL queries by summary-based vertical partitioning and statistics optimization over the Spark platform. We introduce vertical partitioning approach for RDF summary graph called SumVP, and complex SPARQL queries are made by translating them into Spark SQL queries based on binary statistics of tables (BT) and ternary tables (TT) to accelerate querying evaluation. We use Spark SQL API to execute query evaluation.
# 2. Requirements
- Java 8+
- Scala 2.11+
- Apache Hadoop 2.6+
- Apache Spark 2.4+
- IntelliJ IDEA 2019+
- Apache Maven 3.9+
# 3. How to use it?
&ensp;&ensp;&ensp;&ensp;In this package, we provide sources to process and query RDF datasets. For space reasons, we did not unload the datasets. You can get the corresponding RDF datasets from LUBM, WatDiv and other places. It is worth noting that all RDF datasets have to be in the N-Triples format. If your RDF data is in XML format, you can use [rdf2rdf-1.0.1-2.3.1.jar](http://www.l3s.de/~minack/rdf2rdf/) to convert it to NT format. There are four modules in SumVP. In each module, we provide a running jar package. Of course, you can also run our code in your own way.
