;; so far we have mapreduce working continously, across multiple 'servers' (see 03_)
;; the problem is that our server_get implementation reads all data into memory
;; instead of just reading the latest data
;; let's rewrite continous_mapreduce and server_get to actually be continous

;; this one is an exercise for the reader :P


;; we want to our program to receive a stream of changes from a set of files, and only the changes
;; (ie. emulate tail -f)
;; there's 4 ways to do this:
;;   1. call tail -f from a script and parse it (catch: clojure doesn't yet have a good way to get streaming shell data)
;;   2. pipe tail -f into a script (catch: need to call tail -f externally)
;;   3. make our own tail, using java file watcher API, which detects filesystem write events (catch: only Java 7, and not as trivial as the following:)
;;   4. make our own tail, polling files (less efficient than 3, but simple to implement), and java's RandomAccessFile
;; let's go with 4
;; here's some starter code that will println any changes to "alpha.log", without reading the entire file

(import java.io.RandomAccessFile)

(defn raf-seq
  [#^RandomAccessFile raf]
  (if-let [line (.readLine raf)]
    (lazy-seq (cons line (raf-seq raf)))
    (do (Thread/sleep 1000)
        (recur raf))))

(defn tail-seq [input]
  (let [raf (RandomAccessFile. input "r")]
    (.seek raf (.length raf))
    (raf-seq raf)))

(doseq [line (tail-seq (str target_dir "alpha" ".log"))] (println line))


;; the rest is up to you!
;; you should be able to bring in code from 03_ and only mess around with continous_mapreduce and server_get
