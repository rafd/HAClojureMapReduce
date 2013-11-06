;; generate sessions across different files
;; sample session line:
;;    21324567 alice 9 3 2 2 3 4
;;
;; run from shell: clj 01_session_generator.clj
;; see it running: tail -f logs/*.log
;;
;; There's a number of ways to make this code more idiomatic
;; For example, passing the vars to the functions, rather than expecting them to be in scope
;; I'll leave that as an exercise for the reader :P

(def target_dir "/Users/rafal/Code/mapreduce/logs/")
(def servers ["alpha" "beta" "zeta" "gamma" "delta"])
(def users ["alice" "bob" "chester" "dylan" "eunice" "foster" "graham" "hillary"])

(defn session
  "create a session string"
  []
  (clojure.string/join " " (flatten [ (quot (System/currentTimeMillis) 1000)
                                      (rand-nth users)
                                      (repeatedly (+ 1 (rand-int 20)) #(rand-int 100))
                                      "\n"
                                    ])))

(defn log
  "write session string to a random file"
  []
  (spit (str target_dir (rand-nth servers) ".log") (session) :append true))

;; exercise: rewrite tick to work with recur instead of future

(defn tick
  "run a function repeatedly"
  [ms f]
  (future
    (f)
    (Thread/sleep ms)
    (tick ms f)))

;; running this in the lighttable instarepl makes it "unstoppable"
;; actually... you can go to View > Connections and close the connection

(tick 1000 log)
