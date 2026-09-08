(ns metabase.driver.maxcompute-test
  "Unit tests for the MaxCompute driver.

  These tests do not require a live MaxCompute connection — they exercise the
  pure-Clojure pieces (namespace loading, identifier escaping, temporal literal
  formatting, temporal-type inference, and MBQL→HoneySQL compilation)."
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [honey.sql :as sql]
   [java-time.api :as t]
   [metabase.driver :as driver]
   [metabase.driver.maxcompute :as maxcompute]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.driver.sql.util.unprepare :as unprepare]
   [metabase.query-processor.compile :as qp.compile]
   [metabase.lib.core :as lib]
   [metabase.lib.test-util :as lib.tu]
   [metabase.lib.test-metadata :as lib.test-md]
   [metabase.query-processor.store :as qp.store]
   [metabase.query-processor.util.add-alias-info :as add]
   [metabase.util.date-2 :as u.date])
  (:import
   (java.time LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime ZonedDateTime Instant)))

(deftest ^:parallel namespace-loads-test
  (testing "The maxcompute namespace loads without throwing (validates ns declaration, requires, register!)"
    (is (some? (find-ns 'metabase.driver.maxcompute)))))

;; 2026-09-08 production incident (bi.siliconflow.cn v0.63.16.6 + driver v0.0.6):
;; table browse / simple data select rendered SQL with EMPTY select expressions
;; (…SELECT AS `_key_`…) and a one-blob FROM `project.db.table`. Guard the full
;; pipeline (field refs -> add-alias-info -> top-level clauses -> rendered SQL)
;; — unit-level HoneySQL shape tests missed this entirely.
(defn- compile-simple-table-query
  "Compiles an MBQL query (table browse shape: plain fields, no aggregation) to native
   SQL with a mock metadata provider whose Database details carry :project, mirroring
   the production incident (queryHash e0bfeb…). Returns the rendered SQL string."
  [db-details]
  (qp.store/with-metadata-provider
    (lib.tu/mock-metadata-provider
      {:database {:lib/type :metadata/database, :id 1 :name "test-mc" :engine :maxcompute
                  :details db-details}
       :tables   [{:lib/type :metadata/table, :id 100 :name "inference_detail_inputs" :schema "df_cs"}]
       :fields   [{:lib/type :metadata/column, :id 1001 :name "_key_"      :table-id 100 :base-type :type/Text}
                  {:lib/type :metadata/column, :id 1002 :name "_timestamp_" :table-id 100
                   :base-type :type/BigInteger :effective-type :type/DateTime
                   :coercion-strategy :Coercion/UNIXMilliseconds->DateTime}]})
    (driver/with-driver :maxcompute
      (:query
       (qp.compile/compile
         {:database 1
          :type     :query
          :query    {:source-table 100
                   :fields      [[:field 1001 nil] [:field 1002 nil]]}})))))

(deftest integration-table-browse-sql-test
  (testing "table-browse MBQL query compiles to well-formed SQL: every field renders an expression, FROM is two-part"
    (let [sql (compile-simple-table-query {:project "df_cs_673150" :endpoint "http://x" :ak "a" :sk "b"})]
      (testing "no empty SELECT expressions (the incident's `SELECT AS `_key_``)"
        (is (str/includes? sql "_key_"))
        (is (not (re-matches #"(?s).*SELECT +(AS|,).*" sql)) "select list must not be empty"))
      (testing "FROM identifier must not be a single-blob `project.db.table`"
        (is (not (str/includes? sql "`df_cs_673150.df_cs_673150.inference_detail_inputs`")))))))

(deftest ^:parallel driver-registered-test
  (testing ":maxcompute is registered with :sql-jdbc parent (derives from :sql as well)"
    ;; `isa?` against driver/hierarchy is the structural check; `driver/initialized?` is a runtime
    ;; state check that requires `driver/initialize!` to have run, which doesn't happen in unit tests.
    ;; 2026-09-08 incident fix: the driver must NOT derive from :sql-mbql5 —
    ;; its clause-order forwarding shims corrupt legacy 3-tuple :field clauses
    ;; on v0.63.x (identifiers render with empty components; the bi.siliconflow.cn
    ;; production breakage). See register comment in maxcompute.clj.
    (is (isa? driver/hierarchy :maxcompute :sql-jdbc))
    (is (isa? driver/hierarchy :maxcompute :sql))
    (is (not (isa? driver/hierarchy :maxcompute :sql-mbql5))
        ":maxcompute must not derive from :sql-mbql5 (legacy-field reorder bug)")))

(deftest ^:parallel escape-alias-test
  (testing "escape-alias converts aliases to valid MaxCompute identifiers"
    (are [input expected] (= expected (driver/escape-alias :maxcompute input))
      "simple"                  "simple"
      "with space"              "with_space"
      "café"                    "cafe"
      "9starts-with-digit"      "_9starts_with_digit"
      "emoji🚀name"             "emoji_name"
      "  padded  "              "padded")))

(deftest ^:parallel inline-value-string-test
  (testing "inline-value for String returns a quoted SQL string literal with escaped single quotes"
    (is (= "'Cam\\'s String'"
           (sql.qp/inline-value :maxcompute "Cam's String")))))

(deftest ^:parallel inline-value-temporal-test
  (testing "inline-value formats temporal literals with MaxCompute-specific syntax"
    (let [local-date     (LocalDate/of 2024 1 15)
          local-time     (LocalTime/of 12 30 45 0)
          local-datetime (LocalDateTime/of 2024 1 15 12 30 45 0)
          instant        (Instant/parse "2024-01-15T12:30:45Z")
          offset-time    (OffsetTime/of 12 30 45 0 (t/zone-offset 8))]
      (is (= (format "date\"%s\"" (u.date/format-sql local-date))
             (sql.qp/inline-value :maxcompute local-date)))
      (is (= (format "datetime\"%s\"" (u.date/format-sql local-time))
             (sql.qp/inline-value :maxcompute local-time)))
      (is (= (format "timestamp\"%s\"" (u.date/format-sql local-datetime))
             (sql.qp/inline-value :maxcompute local-datetime)))
      (is (= (format "timestamp\"%s\"" (u.date/format-sql instant))
             (sql.qp/inline-value :maxcompute instant)))
      ;; OffsetTime is converted to LocalTime in UTC
      (is (str/starts-with? (sql.qp/inline-value :maxcompute offset-time) "datetime\"")))))

(deftest ^:parallel unprepare-value-string-test
  (testing "unprepare-value for String returns a quoted SQL string literal with escaped single quotes"
    ;; unprepare-value returns the complete SQL literal (including surrounding single quotes),
    ;; matching the inline-value behavior for String.
    (is (= "'Cam\\'s String'"
           (unprepare/unprepare-value :maxcompute "Cam's String")))))

(deftest ^:parallel unprepare-value-temporal-test
  (testing "unprepare-value formats temporal literals with MaxCompute-specific syntax (matches inline-value)"
    (let [local-date     (LocalDate/of 2024 1 15)
          local-datetime (LocalDateTime/of 2024 1 15 12 30 45 0)]
      (is (= (format "date\"%s\"" (u.date/format-sql local-date))
             (unprepare/unprepare-value :maxcompute local-date)))
      (is (= (format "timestamp\"%s\"" (u.date/format-sql local-datetime))
             (unprepare/unprepare-value :maxcompute local-datetime))))))

(deftest ^:parallel inline-and-unprepare-consistency-test
  (testing "inline-value and unprepare-value produce identical syntax for temporal types"
    (let [local-date     (LocalDate/of 2024 6 1)
          local-time     (LocalTime/of 8 0 0 0)
          local-datetime (LocalDateTime/of 2024 6 1 8 0 0 0)
          instant        (Instant/parse "2024-06-01T08:00:00Z")]
      (doseq [value [local-date local-time local-datetime instant]]
        (is (= (sql.qp/inline-value :maxcompute value)
               (unprepare/unprepare-value :maxcompute value))
            (str "Mismatch for " (.getClass value)))))))

(deftest ^:parallel temporal-type-inference-test
  (testing "temporal-type correctly infers types from Java temporal values"
    ;; These call the private multimethod via var; use the public entry points if available.
    ;; We test indirectly through the metadata attached by with-temporal-type.
    (let [local-date (LocalDate/of 2024 1 1)]
      ;; LocalDate should be associated with :date
      (is (= :date (#'maxcompute/temporal-type local-date))))
    (let [local-datetime (LocalDateTime/of 2024 1 1 0 0 0)]
      (is (= :timestamp_ntz (#'maxcompute/temporal-type local-datetime))))))

(deftest ^:parallel honeysql-field-smoke-test
  (testing "->honeysql for :field produces a HoneySQL identifier without throwing"
    ;; :field impl looks up table metadata via qp.store; wrap with a mock provider so the store is initialized.
    ;; Use a real field id from the test metadata and supply ::add/source-table so the parent method can resolve
    ;; the field's table.
    ;; Note: :field clause order differs between Metabase versions:
    ;;   - master (PR #77529+):  [:field opts id-or-name]
    ;;   - v0.63.x and earlier:  [:field id-or-name opts]
    ;; Detect which order to use based on sql-mbql5 availability (removed in master).
    (let [field-id   (lib.test-md/id :venues :name)
          field-opts {:base-type         :type/Text
                      ::add/source-table (lib.test-md/id :venues)
                      ::add/source-alias "NAME"
                      :lib/uuid          "test-uuid"}
          mbql5?     (try
                       (require 'metabase.driver.sql-mbql5)
                       true
                       (catch Throwable _ false))
          field-clause (if mbql5?
                         ;; v0.63.x: [:field id-or-name opts]
                         [:field field-id field-opts]
                         ;; master: [:field opts id-or-name]
                         [:field field-opts field-id])]
      (qp.store/with-metadata-provider lib.test-md/metadata-provider
        (is (some? (sql.qp/->honeysql :maxcompute field-clause)))))))

(deftest ^:parallel honeysql-now-smoke-test
  (testing "->honeysql for :now produces a HoneySQL form without throwing"
    (is (some? (sql.qp/->honeysql :maxcompute [:now {:lib/uuid "test-uuid"}])))))

(deftest ^:parallel current-datetime-honeysql-form-test
  (testing "current-datetime-honeysql-form returns a form HoneySQL can format"
    (let [form (sql.qp/current-datetime-honeysql-form :maxcompute)]
      (is (some? form))
      (is (some? (sql/format-expr form {:nested true}))))))

;;; 2026-09-07 unix-timestamp->honeysql fix (ODPS-0130121 / ODPS-0130071 on real engine):
;;; the old impl emitted nonexistent TIMESTAMP_MILLIS/SECONDS/MICROS built-ins. New impl must
;;; produce engine-verified SQL shapes (see maxcompute.clj fix note). Rendering here is with
;;; the default :ansi dialect; the driver's quote-style (:mysql) only affects identifiers.

(defn- format-unix-ts [unit expr]
  (sql/format-expr (sql.qp/unix-timestamp->honeysql :maxcompute unit expr)
                   {:nested true :quoting :mysql}))
(deftest ^:parallel unix-timestamp->honeysql-seconds-test
  (testing ":seconds compiles to FROM_UNIXTIME with a BIGINT-cast arg inside CAST(... AS TIMESTAMP)"
    (is (= [ "CAST(FROM_UNIXTIME(CAST(t.x AS bigint)) AS timestamp)" ]
           (format-unix-ts :seconds (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))))))

(deftest ^:parallel unix-timestamp->honeysql-milliseconds-test
  (testing ":milliseconds compiles to the split-and-recombine shape with 3-digit fraction"
    (is (= [ "CAST(CONCAT(TO_CHAR(FROM_UNIXTIME(CAST(t.x / 1000 AS bigint)), 'yyyy-mm-dd hh:mi:ss'), '.', LPAD(CAST(t.x % 1000 AS STRING), 3, '0')) AS timestamp)" ]
           (format-unix-ts :milliseconds (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))))))

(deftest ^:parallel unix-timestamp->honeysql-subsecond-preserves-fraction-test
  (testing "ms and us render distinct shapes (3 vs 6 padding digits, 1000 vs 1000000 divisor)"
    (is (= [ "CAST(CONCAT(TO_CHAR(FROM_UNIXTIME(CAST(t.x / 1000 AS bigint)), 'yyyy-mm-dd hh:mi:ss'), '.', LPAD(CAST(t.x % 1000 AS STRING), 3, '0')) AS timestamp)" ]
           (format-unix-ts :milliseconds (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))))
    (is (= [ "CAST(CONCAT(TO_CHAR(FROM_UNIXTIME(CAST(t.x / 1000000 AS bigint)), 'yyyy-mm-dd hh:mi:ss'), '.', LPAD(CAST(t.x % 1000000 AS STRING), 6, '0')) AS timestamp)" ]
           (format-unix-ts :microseconds (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))))))

(deftest ^:parallel unix-timestamp->honeysql-no-regression-test
  (testing "the former broken forms are gone"
    (let [ms-str (first (format-unix-ts :milliseconds (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}])))
          s-str  (first (format-unix-ts :seconds      (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}])))]
      (testing "never emits the DOUBLE-producing FROM_UNIXTIME(x / 1000.0) style (ODPS-0130121)"
        (is (not (re-find #"FROM_UNIXTIME\(t\.x / 1000" s-str)))
        (is (not (re-find #"1000\.0" ms-str))))
      (testing "never emits nonexistent BigQuery built-ins (ODPS-0130071)"
        (is (not (str/includes? (str/upper-case ms-str) "TIMESTAMP_MILLIS")))
        (is (not (str/includes? (str/upper-case s-str) "TIMESTAMP_SECONDS")))))))

;;; 2026-09-08 engine-audit regression tests (audit rounds A–AE, live engine):
;;; every shape below was engine-verified before being asserted here.
(deftest ^:parallel trunc-renders-short-tokens-test
  (testing "DATETRUNC short token 'mi'; never 'minute'"
    (let [form (sql.qp/date :maxcompute :minute (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))
          s (first (sql/format-expr form {:nested true}))]
      (is (str/includes? s "DATETRUNC("))
      (is (str/includes? s "'mi'"))
      (is (not (str/includes? s "'minute'"))))))

(deftest ^:parallel day-of-year-composite-test
  (testing ":day-of-year via DATEDIFF composite; never EXTRACT(dayofyear)"
    (let [form (sql.qp/date :maxcompute :day-of-year (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))
          s (first (sql/format-expr form {:nested true}))]
      (is (str/includes? (str/upper-case s) "DATEDIFF("))
      (is (str/includes? s "'dd'"))
      (is (not (str/includes? (str/upper-case s) "DAYOFYEAR"))))))

(deftest ^:parallel quarter-composite-test
  (testing ":quarter month-shift composite"
    (let [form (sql.qp/date :maxcompute :quarter (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))
          q (first (sql/format-expr form {:nested true}))
          qy-form (sql.qp/date :maxcompute :quarter-of-year (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))
          qy (first (sql/format-expr qy-form {:nested true}))]
      (is (str/includes? (str/upper-case q) "DATEADD("))
      (is (str/includes? q "'mm'"))
      (is (str/includes? (str/upper-case qy) "CAST("))
      (is (not (re-find #"DATETRUNC\([^,]+, 'quarter'" (str/lower-case q)))))))

(deftest ^:parallel day-of-week-weekday-composite-test
  (testing ":day-of-week uses WEEKDAY and % operator; never EXTRACT(dayofweek) or MOD()"
    (let [form (sql.qp/date :maxcompute :day-of-week (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))
          s (first (sql/format-expr form {:nested true}))]
      (is (str/includes? s "WEEKDAY("))
      (is (str/includes? s "%"))
      (is (not (str/includes? (str/upper-case s) "MOD(")))
      (is (not (str/includes? (str/upper-case s) "DAYOFWEEK"))))))

(deftest ^:parallel week-of-year-iso-weekofyear-test
  (testing ":week-of-year-iso via WEEKOFYEAR; never isoweek"
    (let [form (sql.qp/date :maxcompute :week-of-year-iso (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))
          s (first (sql/format-expr form {:nested true}))]
      (is (str/includes? s "WEEKOFYEAR("))
      (is (not (str/includes? (str/upper-case s) "ISOWEEK"))))))

(deftest ^:parallel datetime-diff-native-datediff-test
  (testing "datetime-diff renders native DATEDIFF; no ghost TIMESTAMP_DIFF/DATETIME_DIFF"
    (let [diff (fn [u] (first (sql/format-expr
                               (sql.qp/datetime-diff :maxcompute u
                                                     (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}])
                                                     (sql.qp/->honeysql :maxcompute [:field "y" {::add/source-table "t" ::add/source-alias "y"}]))
                               {:nested true})))
          s-s (diff :second)
          s-h (diff :hour)
          s-m (diff :month)
          s-w (diff :week)]
      (is (str/includes? (str/upper-case s-s) "DATEDIFF("))
      (is (str/includes? s-s "'ss'"))
      (is (str/includes? s-h "'hh'"))
      (is (str/includes? s-m "'mm'"))
      (is (str/includes? (str/upper-case s-m) "CASE"))
      (is (str/includes? (str/upper-case s-w) "/ 7"))
      (is (not (str/includes? (str/upper-case s-s) "TIMESTAMP_DIFF")))
      (is (not (str/includes? (str/upper-case s-m) "DATETIME_DIFF"))))))

(deftest ^:parallel cast-temporal-string-generic-test
  (testing "generic String->Temporal coercion uses SUBSTR(1,19) cast (engine Q4/P-series probes)"
    (let [field-expr [:field "x" {::add/source-table "t" ::add/source-alias "x"}]
          compiled (sql.qp/cast-temporal-string
                     :maxcompute :Coercion/String->Temporal
                     (sql.qp/->honeysql :maxcompute field-expr))
          rendered (first (sql/format-expr compiled {:nested true}))]
      (is (str/includes? (str/upper-case rendered) "SUBSTR("))
      (is (str/includes? (str/upper-case rendered) "AS DATETIME")))))

(deftest ^:parallel misc-fixed-shapes-test
  (testing "float->DOUBLE, log base-first, YYYYMMDDHHMMSS->TO_DATE, ISO strip, GETDATE"
    (let [f-str (first (sql/format-expr (sql.qp/->float :maxcompute (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}])) {:nested true}))
          l-str (first (sql/format-expr (sql.qp/->honeysql :maxcompute [:log [:field "x" {::add/source-table "t" ::add/source-alias "x"}]]) {:nested true}))
          y-str (first (sql/format-expr (sql.qp/cast-temporal-string :maxcompute :Coercion/YYYYMMDDHHMMSSString->Temporal (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}])) {:nested true}))
          i-str (first (sql/format-expr (sql.qp/cast-temporal-string :maxcompute :Coercion/ISO8601->DateTime (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}])) {:nested true}))
          g-str (first (sql/format-expr (sql.qp/current-datetime-honeysql-form :maxcompute) {:nested true}))]
      (is (str/includes? (str/upper-case f-str) "AS DOUBLE"))
      (is (not (str/includes? (str/upper-case f-str) "FLOAT64")))
      (is (str/includes? l-str "LOG(10"))
      (is (str/includes? (str/upper-case y-str) "TO_DATE("))
      (is (str/includes? y-str "yyyymmddhhmiss"))
      (is (not (str/includes? (str/upper-case y-str) "PARSE_DATETIME")))
      (is (str/includes? (str/upper-case i-str) "REPLACE("))
      (is (str/includes? i-str "'T'"))
      (is (str/includes? i-str "'Z'"))
      (is (str/includes? (str/upper-case g-str) "GETDATE("))
      (is (not (str/includes? (str/upper-case g-str) "CURRENT_TIMESTAMP"))))))
