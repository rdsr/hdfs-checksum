(ns hdfs-checksum.core
  (:use [clojure.java.io :as io])
  (:import [java.io OutputStream]
           [java.security MessageDigest DigestInputStream]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs Path FileSystem]
           [org.apache.commons.codec.binary Hex]))

(defn compute-checksum
  [^InputStream in ^MessageDigest md]
  (with-open [in (DigestInputStream. in md)
              out (proxy [OutputStream] []
                    (write [_]))]
    (io/copy in out))
  (-> md .digest Hex/encodeHex))

(defn file-checksum
  "Computes a standard checksum of a (hdfs) file.
   The file is accessed through the hadoop
   FileSystem api"
  [^String path algorithm ^Configuration conf]
  (let [fs (FileSystem/get conf)
        path (Path. path)
        md (MessageDigest/getInstance algorithm)]
    (compute-checksum (.open fs path) md))

(defn hdfs-checksum
  "Computes the checksum of a local file in a way
   which matches how hadoop/hdfs computes
   checksums for it's files."
  [path configuration]
  (let [in (FileInputStream path)
        md (MessageDigest/getInstnace "MD5MD5CRC")]
    (compute-checksum in )))
