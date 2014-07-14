(defproject org.dthume/data.indexed-set "0.0.1-SNAPSHOT"
  :description "Indexed set type for clojure"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [clj-tuple "0.1.5"]
                 [org.dthume/data.set "0.1.0-SNAPSHOT"]]

  :plugins [[lein-midje "3.0.0"]]

  :profiles {:dev {:dependencies [[midje "1.6.3"]]}}

  :aliases
  {"ci-build"
   ^{:doc "Perform the Continuous Integration build and deploy"}
   ["do" ["clean"] ["check"] ["midje"] ["clean"] ["deploy"]]

   "dev-check"
   ^{:doc "Perform the Continuous Integration build and deploy"}
   ["do" ["clean"] ["check"] ["midje"] ["clean"]]

   "dev-repl"
   ^{:doc "Start a clean development NREPL session"}
   ["do" ["clean"] ["repl"]]

   "dev-test"
   ^{:doc "Run development unit tests"}
   ["do" ["clean"] ["midje"]]})

