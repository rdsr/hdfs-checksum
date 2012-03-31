(ns hdfs-checksum.core
  (:use [clojure.java.io :as io]
        [hdfs-checksum.util])
  (:import [hdfs_checksum MD5MD5CRCMessageDigest]
           [java.io InputStream FileInputStream OutputStream]
           [java.security MessageDigest DigestInputStream]
           [org.apache.hadoop.util DataChecksum]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs Path FileSystem]
           [org.apache.commons.codec.binary Hex]))

(defn- compute-checksum
  [^InputStream in ^MessageDigest md]
  (with-open [in (DigestInputStream. in md)
              out (proxy [OutputStream] []
                    (write
                      ([_])
                      ([_ _ _])))]
    (io/copy in out))
  (-> md .digest
      Hex/encodeHex
      String.))

(defn file-checksum
  "Computes a standard checksum of a (hdfs) file.
   The file is accessed through the hadoop
   FileSystem api"
  [path algorithm ^Configuration configuration]
  (let [fs (FileSystem/get configuration)
        path (Path. path)
        md (MessageDigest/getInstance (name algorithm))]
    (with-open [in (.open fs path)]
      (compute-checksum in md))))

(defn- checksum-type-keyword->int
  [key]
  (get {:CRC32C DataChecksum/CHECKSUM_CRC32C
        :CRC32 DataChecksum/CHECKSUM_CRC32}
       key
       DataChecksum/CHECKSUM_CRC32))

(defn hdfs-checksum
  "Computes the checksum of a local file in a way
   which matches how hadoop/hdfs computes
   checksums for it's files."
  [path checksum-type configuration]
  (let [bytes-per-checksum (.getInt configuration "io.bytes.per.checksum" 512)
        checksums-per-block (checksums-per-block configuration)
        md (MD5MD5CRCMessageDigest. bytes-per-checksum
                                    checksums-per-block
                                    (checksum-type-keyword->int checksum-type))]
    (with-open [in (FileInputStream. path)]
      (compute-checksum in md))))
