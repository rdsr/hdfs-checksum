(ns hdfs-checksum.command-line
  (:use [hdfs-checksum.core])
  (:import [java.net URI]
           [org.apache.hadoop.conf Configuration]))

(defn run [[path chcksm-knd algorithm]]
  (let [scheme (.getScheme (URI. path))
        conf (Configuration.)]
    (cond
     (and (= "local" chcksm-knd) (= "hdfs" scheme))
     (println (file-checksum path algorithm conf))
     (and (= "block" chcksm-knd) (= "hdfs" scheme))
     (println (block-checksums path conf))
     (and (= "hdfs" chcksm-knd)
          (or (nil? scheme) (= "file" scheme)))
     (println (hdfs-checksum path conf))
     :else (println "Couldn't determine which checksum routine to call for "
                    "Args: " path " " chcksm-knd " " algorithm ".Please check "
                    "documentation"))))

(run *command-line-args*)