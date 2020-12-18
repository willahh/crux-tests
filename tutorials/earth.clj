(ns earth
  "https://juxt.pro/blog/crux-tutorial-datalog"
  (:require [crux.api :as crux]))

(def node (crux/start-node {}))

(def manifest
  {:crux.db/id :manifest
   :pilot-name "Johanna"
   :id/rocket "SB002-sol"
   :id/employee "22910x2"
   :badges "SETUP"
   :cargo ["stereo" "gold fish" "slippers" "secret note"]})

(crux/submit-tx node [[:crux.tx/put manifest]])

(crux/entity-history (crux/db node) :manifest :asc)

(crux/submit-tx node
                [[:crux.tx/put
                  {:crux.db/id :commodity/Pu
                   :common-name "Plutonium"
                   :type :element/metal
                   :density 19.816
                   :radioactive true}]
                 [:crux.tx/put
                  {:crux.db/id :commodity/N
                   :common-name "Nitrogen"
                   :type :element/gas
                   :density 1.2506
                   :radioactive false}]
                 [:crux.tx/put
                  {:crux.db/id :commodity/CH4
                   :common-name "Methane"
                   :type :molecule/gas
                   :density 0.717
                   :radioactive false}]])

(crux/submit-tx node
                [[:crux.tx/put
                  {:crux.db/id :stock/Pu
                   :commod :commodity/Pu
                   :weight-ton 21}
                  #inst "2115-02-13T18"]
                 [:crux.tx/put
                  {:crux.db/id :stock/Pu
                   :commod :commodity/Pu
                   :weight-ton 23 }
                  #inst "2115-02-14T18"]

                 [:crux.tx/put
                  {:crux.db/id :stock/Pu
                   :commod :commodity/Pu
                   :weight-ton 22.2 }
                  #inst "2115-02-15T18"]

                 [:crux.tx/put
                  {:crux.db/id :stock/Pu
                   :commod :commodity/Pu
                   :weight-ton 24 }
                  #inst "2115-02-18T18"]

                 [:crux.tx/put
                  {:crux.db/id :stock/Pu
                   :commod :commodity/Pu
                   :weight-ton 24.9 }
                  #inst "2115-02-19T18"]])

(crux/submit-tx node
                [[:crux.tx/put
                  {:crux.db/id :stock/N
                   :commod :commodity/N
                   :weight-ton 3}
                  #inst "2115-02-13T18"  ;; start valid-time
                  #inst "2115-02-19T18"] ;; end valid-time]
                [:crux.tx/put
                 {:crux.db/id :stock/CH4
                  :commod :commodity/CH4
                  :weight-ton 92 }
                 #inst "2115-02-15T18"
                 #inst "2115-02-19T18"]])

(crux/entity (crux/db node #inst "2115-02-14") :stock/Pu)
(crux/entity (crux/db node #inst "2115-02-18") :stock/Pu)

(defn easy-ingest
  "Uses Crux put transaction to add a vector of documents to a specified
  node"
  [node docs]
  (crux/submit-tx node (mapv (fn [doc] [:crux.tx/put doc]) docs)))


(crux/submit-tx node [[:crux.tx/put
                       (assoc manifest :badges ["SETUP" "PUT"])]])

(crux/entity (crux/db node) :manifest)



;; Crux Mercury Assignment: Datalog Queries
(easy-ingest node
             [{:crux.db/id :commodity/Pu
               :common-name "Plutonium"
               :type :element/metal
               :density 19.816
               :radioactive true}

              {:crux.db/id :commodity/N
               :common-name "Nitrogen"
               :type :element/gas
               :density 1.2506
               :radioactive false}

              {:crux.db/id :commodity/CH4
               :common-name "Methane"
               :type :molecule/gas
               :density 0.717
               :radioactive false}

              {:crux.db/id :commodity/Au
               :common-name "Gold"
               :type :element/metal
               :density 19.300
               :radioactive false}

              {:crux.db/id :commodity/C
               :common-name "Carbon"
               :type :element/non-metal
               :density 2.267
               :radioactive false}

              {:crux.db/id :commodity/borax
               :common-name "Borax"
               :IUPAC-name "Sodium tetraborate decahydrate"
               :other-names ["Borax decahydrate" "sodium borate"
                             "sodium tetraborate" "disodium tetraborate"]
               :type :mineral/solid
               :appearance "white solid"
               :density 1.73
               :radioactive false}])

;; Example 1. Basic Query
(crux/q (crux/db node)
        '{:find [element]
          :where [[element :type :element/metal]]})

;; Example 2. Quoting
(=
  (crux/q (crux/db node)
          '{:find [element]
            :where [[element :type :element/metal]]})

  (crux/q (crux/db node)
          '{:find [element]
            :where [[element :type :element/metal]]})

  (crux/q (crux/db node)
          '{:find [element]
            :where [[element :type :element/metal]]}))

;; Example 3. Return the name of metal elements
(crux/q (crux/db node)
        '{:find [name]
          :where [[e :type :element/metal]
                  [e :common-name name]]})

;; Example 4. More information
(crux/q (crux/db node)
        {:find '[name]
         :where '[[e :type t]
                  [e :common-name name]]
         :args [{'t :element/metal}]})
;:args can be used to further filter the results. Lets break down what is going down here.
;First, we are assigning all :crux.db/id that have a :type to e:
;e ← #{[:commodity/Pu] [:commodity/borax] [:commodity/CH4] [:commodity/Au] [:commodity/C] [:commodity/N]}
;At the same time we are assigning all the :types to t:
;t ← #{[:element/gas] [:element/metal] [:element/non-metal] [:mineral/solid] [:molecule/gas]}
;Then we assign all the names within e that have a :common-name to name:
;name ← #{["Methane"] ["Carbon"] ["Gold"] ["Plutonium"] ["Nitrogen"] ["Borax"]}
;We have specified that we want to get the names out, but not before looking at :args
;In :args we have further filtered the results to only show us the names of that have :type :element/metal.
;We could have done that inside the :where clause, but using :args removes the need for hard-coding inside the query clauses.

(defn filter-type
  [type]
  (crux/q (crux/db node)
          {:find '[name]
           :where '[[e :type t]
                    [e :common-name name]]
           :args [{'t type}]}))

(defn filter-appearance
  [description]
  (crux/q (crux/db node)
          {:find '[name IUPAC]
           :where '[[e :common-name name]
                    [e :IUPAC-name IUPAC]
                    [e :appearance appearance]]
           :args [{'appearance description}]}))

(filter-type :element/metal)
;;=> #{["Gold"] ["Plutonium"]}

(filter-appearance "white solid")
;;=> #{["Borax" "Sodium tetraborate decahydrate"]}

(crux/submit-tx node [[:crux.tx/put (assoc manifest
                                      :badges ["SETUP" "PUT" "DATALOG-QUERIES"])]])



;; ---- Crux Neptune Assignment: Bitemporality

(crux/submit-tx
  node
  [[:crux.tx/put
    {:crux.db/id :consumer/RJ29sUU
     :consumer-id :RJ29sUU
     :first-name "Jay"
     :last-name "Rose"
     :cover? true
     :cover-type :Full}
    #inst "2114-12-03"]])


(crux/submit-tx
  node
  [[:crux.tx/put
    {:crux.db/id :consumer/RJ29sUU
     :consumer-id :RJ29sUU
     :first-name "Jay"
     :last-name "Rose"
     :cover? true
     :cover-type :Full}
    #inst "2113-12-03" ;; Valid time start
    #inst "2114-12-03"] ;; Valid time end

   [:crux.tx/put
    {:crux.db/id :consumer/RJ29sUU
     :consumer-id :RJ29sUU
     :first-name "Jay"
     :last-name "Rose"
     :cover? true
     :cover-type :Full}
    #inst "2112-12-03"
    #inst "2113-12-03"]

   [:crux.tx/put
    {:crux.db/id :consumer/RJ29sUU
     :consumer-id :RJ29sUU
     :first-name "Jay"
     :last-name "Rose"
     :cover? false}
    #inst "2112-06-03"
    #inst "2112-12-02"]

   [:crux.tx/put
    {:crux.db/id :consumer/RJ29sUU
     :consumer-id :RJ29sUU
     :first-name "Jay"
     :last-name "Rose"
     :cover? true
     :cover-type :Promotional}
    #inst "2111-06-03"
    #inst "2112-06-03"]])

(crux/q (crux/db node #inst "2114-01-01")
        '{:find [cover type]
          :where [[e :consumer-id :RJ29sUU]
                  [e :cover? cover]
                  [e :cover-type type]]})

(crux/q (crux/db node #inst "2111-07-03")
        '{:find [cover type]
          :where [[e :consumer-id :RJ29sUU]
                  [e :cover? cover]
                  [e :cover-type type]]})

(crux/q (crux/db node #inst "2112-07-03")
        '{:find [cover type]
          :where [[e :consumer-id :RJ29sUU]
                  [e :cover? cover]
                  [e :cover-type type]]})

(crux/submit-tx
  node
  [[:crux.tx/put (assoc manifest :badges ["SETUP" "PUT" "DATALOG-QUERIES" "BITEMP"])]])