(defproject hdfs-checksum "1.0"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :description "Utility to calculate both hadoop checksums of local files and standard checksum of hdfs files"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.apache.hadoop/hadoop-common "0.23.1.1201130103"]
                 ;[org.apache.hadoop/hadoop-common-test "0.23.1.1201130103"] not available yet.
                 [org.apache.hadoop/hadoop-hdfs "0.23.1.1201130103"]])
                 ;[org.apache.hadoop/hadoop-hdfs-test "0.23.1.1201130103"] not available yet.
