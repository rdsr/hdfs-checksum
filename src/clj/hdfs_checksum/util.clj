(ns hdfs-checksum.util
  (:import [java.io DataInputStream DataOutputStream
            ByteArrayInputStream ByteArrayOutputStream]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs FileChecksum]
           [org.apache.commons.codec.binary Hex]))

(defn crc-per-block
  [^Configuration configuration]
  (let [bytes-per-crc (.getInt configuration "io.bytes.per.checksum" 512)
        blocksize (or (.get configuration "dfs.blocksize")
                      (.get configuration "dfs.block.size"))]
    (/ (Integer/parseInt blocksize) bytes-per-crc)))

(defn checksum->str
  "Utility method to extract checksum out
   of FileChecksum class"
  [^FileChecksum checksum]
  (let [baos (ByteArrayOutputStream.)]
    (.write checksum (DataOutputStream. baos))
    (let [in (-> baos .toByteArray ByteArrayInputStream. DataInputStream.)
          bytes (byte-array 16)]
      (.readInt in)
      (.readLong in)
      (.readFully in bytes)
      (-> bytes
          Hex/encodeHex
          String.))))
