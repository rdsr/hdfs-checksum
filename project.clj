(defproject hdfs-checksum "1.0"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :resource-paths ["conf"]
  :description "Utility to calculate both hadoop checksums of local files and standard checksum of hdfs files"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.apache.hadoop/hadoop-common "0.23.6"]
                 [org.apache.hadoop/hadoop-hdfs "0.23.6" :scope "test" :classifier "tests"]
                 [org.apache.hadoop/hadoop-hdfs "0.23.6"]])
