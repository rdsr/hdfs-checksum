(ns hdfs-checksum.util
  (:import [java.io DataInputStream DataOutputStream
            ByteArrayInputStream ByteArrayOutputStream]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs FileChecksum]
           [org.apache.commons.codec.binary Hex]))

(defn crcs-per-block
  [^Configuration conf]
  (let [bytes-per-crc (.getInt conf "io.bytes.per.checksum" 512)
        blocksize (or (.get conf "dfs.blocksize")  ;; hadoop 0.23
                      (.get conf "dfs.block.size"))] ;; hadoop 0.20.x
    (if (nil? blocksize)
      (throw (IllegalArgumentException. "Block size not set"))
             (/ (Integer/parseInt blocksize) bytes-per-crc))))

(defn checksum->str
  "Utility method to extract checksum out
   of FileChecksum class"
  [^FileChecksum checksum]
  (let [baos (ByteArrayOutputStream.)]
    (.write checksum (DataOutputStream. baos))
    (let [in (-> baos
                 .toByteArray
                 ByteArrayInputStream.
                 DataInputStream.)
          bytes (byte-array 16)]
      (.readInt in)  ; discard
      (.readLong in) ; discard
      (.readFully in bytes)
      (-> bytes
          Hex/encodeHex
          String.))))
