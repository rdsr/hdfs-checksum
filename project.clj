(defproject hdfs-chksum "1.0"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :resources-path "conf"
  :description "Utility to calculate both hadoop checksums and vanilla checksum of hdfs files"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [yahoo.yinst.hadoop_mvn_common/hadoop-common "0.23.1.1201130103"]
                 [yahoo.yinst.hadoop_mvn_hdfs/hadoop-hdfs "0.23.1.1201130103"]
                 [yahoo.yinst.hadoop_mvn_mapreduce/hadoop-mapreduce-client-core "0.23.1.1201130103"]
                 [yahoo.yinst.hadoop_mvn_mapreduce/hadoop-mapreduce-client-common "0.23.1.1201130103"]
                 [yahoo.yinst.hadoop_mvn_yarn/hadoop-yarn-common "0.23.1.1201130103"]])