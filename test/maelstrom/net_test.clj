(ns maelstrom.net-test
  (:require [clojure [test :refer :all]]
            [maelstrom [net :as net]]
            [maelstrom.net.journal :as j]))

; These tests drive the network directly, without a running test, so there's
; no journal to write to.
(use-fixtures :each
  (fn [run-test]
    (with-redefs [j/log-send! (fn [_ _])
                  j/log-recv! (fn [_ _])]
      (run-test))))

(defn test-net
  "A network with zero latency."
  []
  (net/net {:mean 0, :dist :constant} false false))

(defn send-n!
  "Sends n messages from src to dest, with bodies {:i 0} ... {:i n-1}."
  [net src dest n]
  (dotimes [i n]
    (net/send! net {:src src, :dest dest, :body {:i i}})))

(defn queue-size
  [net node]
  (.size (net/queue-for net node)))

(defn recv-all!
  "Receives every pending message for node; returns the set of their bodies'
  :i values."
  [net node]
  (loop [is #{}]
    (if-let [m (net/recv! net node 10)]
      (recur (conj is (:i (:body m))))
      is)))

(deftest queue-bound-test
  (let [n (test-net)]
    (net/add-node! n "src")

    (testing "running nodes have unbounded queues"
      (net/add-node! n "running")
      (send-n! n "src" "running" (* 2 net/max-queue-size))
      (is (= (* 2 net/max-queue-size) (queue-size n "running"))))

    (testing "down nodes drop messages beyond max-queue-size"
      (net/add-node! n "down")
      (net/down! n "down")
      (send-n! n "src" "down" (* 3 net/max-queue-size))
      (is (= net/max-queue-size (queue-size n "down"))))

    (testing "coming back up lifts the bound"
      (net/up! n "down")
      (send-n! n "src" "down" 5)
      (is (= (+ net/max-queue-size 5) (queue-size n "down"))))))

(deftest pause-redelivery-test
  ; A paused node keeps its queue. Messages sent while it's paused--up to
  ; max-queue-size--must be received once it resumes.
  (let [n (test-net)]
    (net/add-node! n "src")
    (net/add-node! n "n1")
    (net/down! n "n1")                                  ; pause
    (send-n! n "src" "n1" (* 2 net/max-queue-size))
    (net/up! n "n1")                                    ; resume
    (is (= (set (range net/max-queue-size))            ; oldest messages kept
           (recv-all! n "n1")))))

(deftest kill-restart-redelivery-test
  ; Mirrors the restart sequence in maelstrom.db/start-node!: snapshot the
  ; dead node's backlog, install a fresh queue, deliver init, then requeue
  ; the backlog. The restarted node must see init first, then receive the
  ; messages peers sent while it was dead.
  (let [n (test-net)]
    (net/add-node! n "src")
    (net/add-node! n "n1")
    (net/down! n "n1")                                  ; kill
    (send-n! n "src" "n1" (* 2 net/max-queue-size))     ; peers keep sending
    (let [backlog (net/drain-queue! n "n1")]
      (is (= net/max-queue-size (count backlog)))
      (net/add-node! n "n1")                            ; restart: fresh queue
      (net/up! n "n1")
      (net/send! n {:src "src", :dest "n1", :body {:type "init"}})
      ; The node consumes init before anything else...
      (is (= "init" (:type (:body (net/recv! n "n1" 10)))))
      ; ... and once the backlog is requeued, receives the buffered messages.
      (net/requeue! n "n1" backlog)
      (is (= (set (range net/max-queue-size))
             (recv-all! n "n1"))))))
