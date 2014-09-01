(ns org.dthume.data.indexed-set.pldb
  (:require [clojure.core.reducers :as r]
            [clojure.core.logic.pldb :as pldb]
            [org.dthume.data.indexed-set :as idx-set]
            [org.dthume.data.set :as set :refer :all])
  (:import
   (clojure.lang Counted IHashEq ILookup IPersistentCollection
                 IPersistentSet IObj ISeq Seqable Sequential)))

(defn- apply-rels
  [db f rels]
  (r/reduce #(apply f %1 %2) db rels))

(declare with-db with-rels)

(deftype PldbIndex [db as-rels mdata]
  Object
  (equals [_ x]
    (boolean
     (and (instance? PldbIndex x)
          (= db (.db ^PldbIndex x)))))
  (hashCode [_] (.hashCode db))
  
  IHashEq
  (hasheq [this]
    (hash db))
  
  IObj
  (meta [_] mdata)
  (withMeta [_ mdata]
    (PldbIndex. db as-rels mdata))

  IPersistentCollection
  (cons [this value]
    (with-rels this pldb/db-fact value))
  (empty [this]
    (with-db this pldb/empty-db))
  (equiv [this x] (.equals this x))
  
  Counted
  (count [_] 0)
  
  IPersistentSet
  (disjoin [this k]
    (with-rels this pldb/db-retraction k))
  (get [this k] nil)
  
  idx-set/Index
  (get-data [this] db)

  SetAlgebra
  (set-union [this rhs]
    (->> rhs
         (mapcat as-rels)
         (apply-rels db pldb/db-fact)
         (with-db this)))
  (set-intersection [_ rhs]
    (throw (UnsupportedOperationException.)))
  (set-difference [this rhs]
    (->> rhs
         (mapcat as-rels)
         (apply-rels db pldb/db-retraction)
         (with-db this))))

(defn- with-db
  [^PldbIndex this db]
  (PldbIndex. db (.as-rels this) (.mdata this)))

(defn- with-rels
  [^PldbIndex this f v]
  (->> v
       ((.as-rels this))
       (apply-rels (.db this) f)
       (with-db this)))

(defn pldb-index
  ([as-rels]
     (pldb-index as-rels pldb/empty-db))
  ([as-rels base]
     (PldbIndex. base as-rels {})))

