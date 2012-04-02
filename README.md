#hdfs-checksum

hdfs-checksum contains utility functions to calculate checksums
(both standard and distributed) of hdfs and local files.

##Usage
### Command line
Run the following commands
lein deps
lein compile
lein jar

Create a folder conf and add hadoop's site xmls in it. Be sure that atleast the following
properties are set
* dfs.blocksize

Defaults are taken for the following properties if nothing is specified
* dfs.checksum.type (default CRC32)
* io.bytes.per.checksum (default 512)

Calculate the distributed checksum of a local file
java -cp "lib/*:hdfs-checksum-1.0.jar:conf>" clojure.main src/clj/hdfs_checksum/command-line.clj /tmp/file

Calculate standard checksum of a hdfs file
java -cp "lib/*:hdfs-checksum-1.0.jar:conf clojure.main src/clj/hdfs_checksum/command-line.clj hdfs://<namenode>:<port>/tmp/file MD5


### Repl
      user> (hdfs-checksum "/tmp/file_1" configuration)
      "7b5166eb3abb113de7c7219872e7b1f4"
      user>(clojure.repl/doc hdfs-checksum)
      -------------------------
      hdfs-checksum.core/hdfs-checksum
      ([path configuration])
        Computes the checksum of a local file in a way
        which matches how hadoop/hdfs computes
        checksums for it's files.

configuration is Hadoop's Configuration class. It should atleast have the
following parameters set.

* dfs.blocksize

A good way to apply the necessary paramaters to configuration is to add hadoop's
core-site.xml, hdfs-site.xml and mapred-site.xml to classpath before constructing
Configuration object.

      user> (file-checksum "hdfs://localhost:63372/user/rdsr/hdfs_file" :MD5 configuration)
      "7622214b8536afe7b89b1c6606069b0d"
      user> (clojure.repl/doc file-checksum)
      -------------------------
      hdfs-checksum.core/file-checksum
      ([path algorithm configuration])
        Computes a standard checksum of a (hdfs) file.
        The file is accessed through the hadoop
        FileSystem api

Here too, configuration is Hadoop's configuration class, though the above parameters
are not necessary in this case.