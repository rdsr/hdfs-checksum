(ns hdfs-checksum.core_test
  (:use [clojure.test]
        [clojure.java.io :as io]
        [hdfs-checksum.core]
        [hdfs-checksum.util])
  (:import [java.io File]
           [org.apache.hadoop.fs Path FileSystem]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.hdfs MiniDFSCluster]))

(defn- create-cluster [configuration]
  (-> configuration
      org.apache.hadoop.hdfs.MiniDFSCluster$Builder.
      .build))

(defn- destroy-cluster [cluster]
  (when (.isClusterUp cluster)
    (.shutdown cluster)))

(defn- setup-configuration
  [^Configuration configuration bytes-per-crc block-size]
  (doto configuration
    (.setInt "io.bytes.per.checksum" bytes-per-crc)
    (.setInt "dfs.blocksize" block-size)))

(defn- setup-file
  "Creates local file and also copies it over to hdfs"
  [name size configuration]
  (let [src-file (File. "/tmp" name)
        fs (FileSystem/get configuration)
        dst-path (Path. (.getWorkingDirectory fs) name)]
    (io/copy (byte-array size) src-file)
    (.copyFromLocalFile fs
                        (Path. (.getAbsolutePath src-file))
                        dst-path)
    [src-file dst-path]))

(defn- preprocess
  [filename file-size configuration bytes-per-crc block-size]
  (setup-configuration configuration bytes-per-crc block-size)
  (setup-file filename file-size configuration))

(def ^:dynamic *configuration* nil)

(deftest verify-hdfs-checksum
  (let [[local-file hdfs-path] (preprocess "test1" 1024 *configuration* 512 1024)
        fs (FileSystem/get *configuration*)]
    (is (= (-> fs (.getFileChecksum hdfs-path) checksum->str)      ; expected
           (hdfs-checksum local-file :crc32c *configuration*)))))  ; computed

(defn cluster-fixture [f]
  (binding [*configuration* (Configuration.)]
    (let [cluster (create-cluster *configuration*)]
      (f)
      (destroy-cluster cluster))))

(use-fixtures :once cluster-fixture)