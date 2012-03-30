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
  [^Configuration configuration bytes-per-crc block-size checksum-type]
  (doto configuration
    (.set "dfs.checksum.type" (name checksum-type))
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
  "basic setup before a test runs"
  [filename file-size configuration bytes-per-crc block-size checksum-type]
  (setup-configuration configuration bytes-per-crc block-size checksum-type)
  (setup-file filename file-size configuration))

(def ^:dynamic *configuration* nil)

(deftest verify-1
  "file size:          235
   bytes per checksum: 512
   block size:         1024
   algorithm:          CRC32"
  (let [[local-file hdfs-path] (preprocess "test1" 235 *configuration* 512 1024 :CRC32)
        fs (FileSystem/get *configuration*)]
    (is (= (-> fs (.getFileChecksum hdfs-path) checksum->str)     ; expected
           (hdfs-checksum local-file :CRC32 *configuration*)))))  ; computed

(deftest verify-2
  "file size:          1024
   bytes per checksum: 512
   block size:         1024
   algorithm:          CRC32C"
  (let [[local-file hdfs-path] (preprocess "test1" 1024 *configuration* 512 1024 :CRC32C)
        fs (FileSystem/get *configuration*)]
    (is (= (-> fs (.getFileChecksum hdfs-path) checksum->str)      ; expected
           (hdfs-checksum local-file :CRC32C *configuration*)))))  ; computed

(deftest verify-3
  "file size:          0
   bytes per checksum: 512
   block size:         1024
   algorithm:          CRC32C"
  (let [[local-file hdfs-path] (preprocess "test1" 0 *configuration* 512 1024 :CRC32C)
        fs (FileSystem/get *configuration*)]
    (is (= (-> fs (.getFileChecksum hdfs-path) checksum->str)      ; expected
           (hdfs-checksum local-file :CRC32C *configuration*)))))  ; computed

(deftest verify-4
  "file size:          1023
   bytes per checksum: 256
   block size:         1024
   algorithm:          CRC32C"
  (let [[local-file hdfs-path] (preprocess "test1" 1023 *configuration* 256 1024 :CRC32C)
        fs (FileSystem/get *configuration*)]
    (is (= (-> fs (.getFileChecksum hdfs-path) checksum->str)      ; expected
           (hdfs-checksum local-file :CRC32C *configuration*)))))  ; computed

(deftest verify-5
  "file size:          29
   bytes per checksum: 256
   block size:         256
   algorithm:          CRC32"
  (let [[local-file hdfs-path] (preprocess "test1" 1023 *configuration* 256 256 :CRC32C)
        fs (FileSystem/get *configuration*)]
    (is (= (-> fs (.getFileChecksum hdfs-path) checksum->str)      ; expected
           (hdfs-checksum local-file :CRC32C *configuration*)))))  ; computed

(deftest verify-6
  "file size:          29
   bytes per checksum: 29
   block size:         29
   algorithm:          CRC32"
  (let [[local-file hdfs-path] (preprocess "test1" 29 *configuration* 29 29 :CRC32C)
        fs (FileSystem/get *configuration*)]
    (is (= (-> fs (.getFileChecksum hdfs-path) checksum->str)      ; expected
           (hdfs-checksum local-file :CRC32C *configuration*)))))  ; computed

(defn cluster-fixture [f]
  (binding [*configuration* (Configuration.)]
    (let [cluster (create-cluster *configuration*)]
      (f)
      (destroy-cluster cluster))))

(use-fixtures :each cluster-fixture)