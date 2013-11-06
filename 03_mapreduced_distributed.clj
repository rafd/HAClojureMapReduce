;; ok, now that we have a mapreduce function (see 02_) and sessions in files (see 01_)
;; let's bring the two together

;; the goal is to have each server receive a mapreduce instruction (filter, map and reduce functions)
;; and return its final response, then the answers from all servers are combined

;; define our servers and log directory
;; exercise: change target_dir to be relative to directory this file is in
;;           (PWD won't work w/ instarepl AFAIK because it's evalled in a tmp directory)

(def server_names ["alpha" "beta" "zeta" "gamma" "delta"])
(def target_dir "/Users/rafal/Code/mapreduce/logs/")


;; let's start off with our mapreduce function
;; the params are exactly the same as before
;; but this time, we ask each server for their mapreduce response (via 'server_mapreduce', a function we will create next)
;; then reduce the responses and run sumfn
;; note that we don't pass the sumfn to the servers because sumfn must be the very last step

(defn mapreduce
  "send mapreduce request to each server, reduce responses and summarize"
  [filterfn mapfn reducefn sumfn]
  (->> server_names
      (map (fn [x] (server_mapreduce x filterfn mapfn reducefn)))
      (reduce reducefn)
      (sumfn)
  )
)

;; now to create the server mapreduce fn
;; it's similar to our original mapreduce function (in 02_)
;; except let's assume our data is coming from a function called server_get
;; easy peasy. don't you love being able to assume that something exists which doesn't exist yet?

(defn server_mapreduce
  "run mapreduce on a specific server"
  [server_name filterfn mapfn reducefn]
  (->> (server_get server_name)
      (filter filterfn)
      (map mapfn)
      (flatten)
      (reduce reducefn)
   )
)

;; and here is our server_get
;; this would be where our implementation-specific code goes
;; (ie. if we used a database, instead of text files, only this function should have to change)
;; for now, let's just read the entire file in to memory, convert it into the data structure we expect (see 02_)
;; and return it

(defn server_get [server_name]

  (map
    (fn [line]
      (let [[timestamp name scores] (clojure.string/split line #" " 3)] ;; note the handy way of using let here
        {:timestamp (Integer/parseInt timestamp)
         :name name
         :scores (map #(Integer/parseInt %) (clojure.string/split scores #" "))}
      )
    )
    (line-seq (clojure.java.io/reader (str target_dir server_name ".log"))))
)




;; okeydoke, let's try some examples:
;; you should run the session generator before trying these
;; the code will fail if one of the servers is missing data
;; exercise for the reader: make it more robust


;; average # of sessions for all

#_(mapreduce
   (fn [x] true)
   (fn [x] (map #(hash-map :score % :count 1) (:scores x)))
   (fn [x y] (merge-with + x y))
   (fn [x] (float (/ (:score x) (:count x))))
 )

;; max score for sessions with timestamp > some value

#_(mapreduce
   #(> (:timestamp %) 1383522015) ;; put in a relevant timestamp
   #(hash-map :max (apply max (:scores %)))
   #(merge-with max %1 %2)
   #(:max %)
 )


;; how about temporally seperated queries?
;; as a proof of concept, let's create a function that:
;; runs mapreduce for timestamp > x and timestamp < x, then combines them together
;; so easy:


(defn time_mapreduce
  "run mapreduce with filter for timestamp > x and < x, then combine"
  [timestamp mapfn reducefn sumfn]
  (->> [(mapreduce #(> (:timestamp %) timestamp) mapfn reducefn identity) (mapreduce #(< (:timestamp %) timestamp) mapfn reducefn identity)]
       (reduce reducefn)
       (sumfn)
   )
)

#_(time_mapreduce
   1383522007
   (fn [x] (map #(hash-map :score % :count 1) (:scores x)))
   (fn [x y] (merge-with + x y))
   (fn [x] (float (/ (:score x) (:count x))))
 )


;; now how about getting a query updating continously?
;; we'll make it a recursive function
;; like in time_mapreduce, we combine two queries (the previous result with the new result)
;; the new result is limited to only be for entries with timestamp > the previous timestamp

(defn continous_mapreduce
  "run mapreduce continously, but only querying new data on subsequent runs"
  [mapfn reducefn sumfn data timestamp_before]
  (def timestamp_now (quot (System/currentTimeMillis) 1000))

  (->> [(mapreduce (fn [x] (< timestamp_before (:timestamp x) timestamp_now)) mapfn reducefn identity) data]
       (reduce reducefn)
       (def result)
  )

  (println (sumfn result))

  (Thread/sleep 4000)
  (recur mapfn reducefn sumfn result timestamp_now)
)

;; this is best run from the commandline

(continous_mapreduce
  (fn [x] (map #(hash-map :score % :count 1) (:scores x)))
  (fn ([] {:score 0 :count 0}) ([x y] (merge-with + x y))) ;; notice I added a 0-variable case to reduce
  (fn [x] (float (/ (:score x) (:count x))))
  {:score 0 :count 0}
  0
)


;; hmmm... but there's a flaw: our server_get function pulls in all data every time
;; which sort of defeats the point of being able to run it temporally seperated
;; let's fix this in 04_
