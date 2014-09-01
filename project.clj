(defproject org.dthume/data.set-db "0.1.0-SNAPSHOT"
  :description "Indexed set type for clojure"
  :url "http://github.com/dthume/data.indexed-set"
  :license "Eclipse Public License 1.0"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [clj-tuple "0.1.6"]
                 [org.dthume/data.set "0.1.0"]]

  :plugins [[lein-midje "3.0.0"]]

  :profiles
  {:dev
   {:dependencies [[midje "1.6.3"]
                   [org.clojure/core.logic "0.8.8"]]}}

  :aliases
  {"ci-build"
   ^{:doc "Perform the Continuous Integration build"}
   ["do" ["clean"] ["check"] ["midje"]]

   "dev-check"
   ^{:doc "Check a build before commits"}
   ["do" ["clean"] ["check"] ["midje"] ["clean"]]

   "dev-repl"
   ^{:doc "Start a clean development NREPL session"}
   ["do" ["clean"] ["repl"]]

   "dev-test"
   ^{:doc "Run development unit tests"}
   ["do" ["clean"] ["midje"]]})

