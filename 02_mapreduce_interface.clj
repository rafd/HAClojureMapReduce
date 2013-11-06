;; let's design our mapreduce function first before we make it distributed

#_(mapreduce
    filterfn
    mapfn
    reducefn
    sumfn
)

;; that was easy :P

;; let's have some data to play around with (later, we will get this from the 'server')

(def data [
           { :name "Alice" :timestamp 0 :scores [1 2 3 4]}
           { :name "Bob" :timestamp 1 :scores [5 6]}
           { :name "Candy" :timestamp 2 :scores [1 2 3 5]}
          ])


;; here's our mapreduce function
;; it could be made a lot more robust (accepting empty fns, etc.)
;; I normally wouldn't suggest assuming that the data var exists in scope,
;; but since later, we will be pulling in from a 'database', it's ok

(defn mapreduce
  "take in data, then filter, map, reduce and summarize using the passed in functions"
  [filterfn mapfn reducefn sumfn]

  ;; to see how it works in steps, try commenting out the functions from bottom up
  ;; while running in instarepl mode in lighttable
  ;; ie. (sumfn) --> #_(sumfn)

  (->> data
      (filter filterfn)
      (map mapfn)
      (flatten) ;; flatten is here to support mapfn outputting multiple lists
      (reduce reducefn)
      (sumfn)
  )

  ;; the above is equivalent to the following
  ;; the -> and ->> macros are useful in avoiding getting lost in nested functions
  #_(sumfn (reduce reducefn (flatten (map mapfn (filter filterfn data)))))
)

;; here are some test cases

;; average # of sessions of all

(mapreduce
  (fn [x] true) ;; using fn instead of #(), because we're passing 1 param, and #() will expect us to use %
  #(hash-map :n (count (:scores %)) :count 1 )
  #(merge-with + %1 %2)
  #(/ (:n %) (:count %))
 )

;; max score of timestamp > 0

(mapreduce
  #(> (:timestamp %) 0)
  #(hash-map :max (apply max (:scores %)))
  #(merge-with max %1 %2)
  #(:max %)
)

;; average score of all

(mapreduce
  (fn [x] true)
  (fn [x] (map #(hash-map :score % :count 1) (:scores x)))
  (fn [x y] (merge-with + x y))
  (fn [x] (/ (:score x) (:count x)) )
)
