#hdfs-checksum

hdfs-checksum contains utility functions to calculate checksums
(both standard and distributed) of hdfs and local files.

##Usage
Calculate distributed checksums (how hdfs filesystem calculates
for its files) of local files.

      user> (hdfs-checksum "/tmp/file_1" :CRC32C configuration)
      "7b5166eb3abb113de7c7219872e7b1f4"
      user> (clojure.repl/doc hdfs-checksum)
      -------------------------
      hdfs-checksum.core/hdfs-checksum
      ([path checksum-type configuration])
        Computes the checksum of a local file in a way
        which matches how hadoop/hdfs computes checksums
        for it's files.

configuration is Hadoop's Configuration class. It should have the
following parameters set.
dfs.checksum.type (default CRC32)
io.bytes.per.checksum (default 512)
dfs.blocksize

Calculate standard checksum of a hdfs file.

      user> (file-checksum "hdfs://localhost:63372/user/rdsr/hdfs_file" :MD5 conf)
      "7622214b8536afe7b89b1c6606069b0d"
      user> (clojure.repl/doc file-checksum)
      -------------------------
      hdfs-checksum.core/file-checksum
      ([path algorithm configuration])
        Computes a standard checksum of a (hdfs) file.
        The file is accessed through the hadoop
        FileSystem api

Here too, configuration is Hadoop configuration class, though the above parameters
are not necessary in this case.