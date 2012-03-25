(ns hdfs-checksum.core
  (:use [clojure.java.io :as io])
  (:import [java.io OutputStream]
           [java.security MessageDigest DigestInputStream]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs Path FileSystem]
           [org.apache.commons.codec.binary Hex]))

(defn checksum
  "Computes a standard checksum of a file.
   The file is accessed through the hadoop
   FileSystem api"
  [file algorithm ^Configuration conf]
  (let [fs (FileSystem/get conf)
        path (Path. file)
        md (MessageDigest/getInstance algorithm)]
    (with-open [in (DigestInputStream. (.open fs path) md)
                out (proxy [OutputStream] []
                      (write [_]))]
      (io/copy in out))
    (-> md .digest Hex/encodeHex)))


(defn hdfs-checksum
  "Computes the checksum of a file in a way
   which matches how hadoop/hdfs computes
   checksums for it's files.
   The file is accessed through the hadoop
   FileSystem api"
    [file aglorithm ^Configuration conf])