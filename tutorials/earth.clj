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



;; Crux Saturn Assignment: Match Transactions
(easy-ingest node
             [{:crux.db/id :gold-harmony
               :company-name "Gold Harmony"
               :seller? true
               :buyer? false
               :units/Au 10211
               :credits 51}

              {:crux.db/id :tombaugh-resources
               :company-name "Tombaugh Resources Ltd."
               :seller? true
               :buyer? false
               :units/Pu 50
               :units/N 3
               :units/CH4 92
               :credits 51}

              {:crux.db/id :encompass-trade
               :company-name "Encompass Trade"
               :seller? true
               :buyer? true
               :units/Au 10
               :units/Pu 5
               :units/CH4 211
               :credits 1002}

              {:crux.db/id :blue-energy
               :seller? false
               :buyer? true
               :company-name "Blue Energy"
               :credits 1000}])

(defn stock-check
  [company-id item]
  {:result (crux/q (crux/db node)
                   {:find '[name funds stock]
                     :where ['[e :company-name name]
                             '[e :credits funds]
                             ['e item 'stock]]
                     :args [{'e company-id}]})
   :item item})

(defn format-stock-check
  [{:keys [result item] :as stock-check}]
  (for [[name funds commod] result]
    (str "Name: " name ", Funds:" funds ", " item " " commod)))

(crux/submit-tx
  node
  [[:crux.tx/match
    :blue-energy
    {:crux.db/id :blue-energy
     :seller? false
     :buyer? true
     :company-name "Blue Energy"
     :credits 1000}]
   [:crux.tx/put
    {:crux.db/id :blue-energy
     :seller? false
     :buyer? true
     :company-name "Blue Energy"
     :credits 900
     :units/CH4 10}]

   [:crux.tx/match
    :tombaugh-resources
    {:crux.db/id :tombaugh-resources
     :company-name "Tombaugh Resources Ltd."
     :seller? true
     :buyer? false
     :units/Pu 50
     :units/N 3
     :units/CH4 92
     :credits 51}]
   [:crux.tx/put
    {:crux.db/id :tombaugh-resources
     :company-name "Tombaugh Resources Ltd."
     :seller? true
     :buyer? false
     :units/Pu 50
     :units/N 3
     :units/CH4 82
     :credits 151}]])


(format-stock-check (stock-check :tombaugh-resources :units/CH4))
(format-stock-check (stock-check :blue-energy :units/CH4))


(crux/submit-tx
  node
  [[:crux.tx/match
    :gold-harmony
    {:crux.db/id :gold-harmony
     :company-name "Gold Harmony"
     :seller? true
     :buyer? false
     :units/Au 10211
     :credits 51}]
   [:crux.tx/put
    {:crux.db/id :gold-harmony
     :company-name "Gold Harmony"
     :seller? true
     :buyer? false
     :units/Au 211
     :credits 51}]
   [:crux.tx/match
    :encompass-trade
    {:crux.db/id :encompass-trade
     :company-name "Encompass Trade"
     :seller? true
     :buyer? true
     :units/Au 10
     :units/Pu 5
     :units/CH4 211
     :credits 100002}]
   [:crux.tx/put
    {:crux.db/id :encompass-trade
     :company-name "Encompass Trade"
     :seller? true
     :buyer? true
     :units/Au 10010
     :units/Pu 5
     :units/CH4 211
     :credits 1002}]])

(format-stock-check (stock-check :gold-harmony :units/Au))
(format-stock-check (stock-check :encompass-trade :units/Au))

(crux/submit-tx
  node [[:crux.tx/put
         (assoc manifest
           :badges ["SETUP" "PUT" "DATALOG-QUERIES" "BITEMP" "MATCH"])]])

(crux/q (crux/db node)
        {:find '[belongings]
         :where '[[e :cargo belongings]]
         :args [{'belongings "secret note"}]})

;; Crux Jupiter Assignment: Delete Transactions
(crux/submit-tx
  node [[:crux.tx/put {:crux.db/id :kaarlang/clients
                       :clients [:encompass-trade]}
         #inst "2110-01-01T09"
         #inst "2111-01-01T09"]

        [:crux.tx/put {:crux.db/id :kaarlang/clients
                       :clients [:encompass-trade :blue-energy]}
         #inst "2111-01-01T09"
         #inst "2113-01-01T09"]

        [:crux.tx/put {:crux.db/id :kaarlang/clients
                       :clients [:blue-energy]}
         #inst "2113-01-01T09"
         #inst "2114-01-01T09"]

        [:crux.tx/put {:crux.db/id :kaarlang/clients
                       :clients [:blue-energy :gold-harmony :tombaugh-resources]}
         #inst "2114-01-01T09"
         #inst "2115-01-01T09"]])

(crux/entity-history
  (crux/db node #inst "2116-01-01T09")
  :kaarlang/clients
  :desc
  {:with-docs? true})

(crux/submit-tx
  node [[:crux.tx/delete :kaarlang/clients #inst "2110-01-01" #inst "2116-01-01"]])

(crux/entity-history
  (crux/db node #inst "2116-01-01T09")
  :kaarlang/clients
  :desc
  {:with-docs? true})

;; Crux 'Oumuamua Assignment: Evict Transactions
(crux/submit-tx node
                [[:crux.tx/put
                  {:crux.db/id :person/kaarlang
                   :full-name "Kaarlang"
                   :origin-planet "Mars"
                   :identity-tag :KA01299242093
                   :DOB #inst "2040-11-23"}]

                 [:crux.tx/put
                  {:crux.db/id :person/ilex
                   :full-name "Ilex Jefferson"
                   :origin-planet "Venus"
                   :identity-tag :IJ01222212454
                   :DOB #inst "2061-02-17"}]

                 [:crux.tx/put
                  {:crux.db/id :person/thadd
                   :full-name "Thad Christover"
                   :origin-moon "Titan"
                   :identity-tag :IJ01222212454
                   :DOB #inst "2101-01-01"}]

                 [:crux.tx/put
                  {:crux.db/id :person/johanna
                   :full-name "Johanna"
                   :origin-planet "Earth"
                   :identity-tag :JA012992129120
                   :DOB #inst "2090-12-07"}]])

(defn full-query
  [node]
  (crux/q
    (crux/db node)
    '{:find [id]
      :where [[e :crux.db/id id]]
      :full-results? true}))

(full-query node)

(crux/submit-tx node [[:crux.tx/evict :person/kaarlang]])

(full-query node)

(crux/entity-history
  (crux/db node)
  :person/kaarlang
  :desc
  {:with-docs? true})