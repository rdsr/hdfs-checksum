(ns hdfs-checksum.core
  (:use [clojure.java.io :as io]
        [hdfs-checksum.util])
  (:import [hdfs_checksum MD5MD5CRCMessageDigest Util]
           [java.net URI]
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
  "Computes a standard checksum of a hdfs file.
   The file is accessed through the hadoop
   FileSystem api"
  [path algorithm ^Configuration conf]
  (let [fs (FileSystem/get conf)
        path (Path. path)
        md (MessageDigest/getInstance (name algorithm))]
    (with-open [in (.open fs path)]
      (compute-checksum in md))))

(defn- checksum-type
  [key]
  ({"CRC32C" org.apache.hadoop.util.DataChecksum$Type/CRC32C
     "CRC32" org.apache.hadoop.util.DataChecksum$Type/CRC32C}
   key
   org.apache.hadoop.util.DataChecksum$Type/CRC32C))

(defn hdfs-checksum
  "Computes the checksum of a local file in a way
   which matches how hadoop/hdfs computes
   checksums for it's files."
  [path ^Configuration conf]
  (let [checksum-key (.get conf "dfs.checksum.type" "CRC32")
        bytes-per-crc (.getInt conf "io.bytes.per.checksum" 512)
        crcs-per-block (crcs-per-block conf)
        md (MD5MD5CRCMessageDigest. bytes-per-crc
                                    crcs-per-block
                                    (checksum-type checksum-key))]
    (with-open [in (FileInputStream. path)]
      (compute-checksum in md))))

(defn block-checksums
  "Returns checksums per block for a hdfs file"
  [path ^Configuration conf]
  (let [path (-> path URI. .getPath)
        {:strs [bytes-per-crc crcs-per-block checksum-type checksums] :as c}
        (Util/blockChecksums path conf)]
    {:bytes-per-crc bytes-per-crc
     :crcs-per-block crcs-per-block
     :checksum-type checksum-type
     :checksums (map (fn [{:strs [block-id md5 boundaries]}]
                            {:block-id block-id :md5 md5 :boundaries (into [] boundaries)})
                          checksums)}))
