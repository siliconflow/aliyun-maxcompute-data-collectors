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
   [metabase.lib.test-metadata :as lib.test-md]
   [metabase.query-processor.store :as qp.store]
   [metabase.query-processor.util.add-alias-info :as add]
   [metabase.util.date-2 :as u.date])
  (:import
   (java.time LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime ZonedDateTime Instant)))

(deftest ^:parallel namespace-loads-test
  (testing "The maxcompute namespace loads without throwing (validates ns declaration, requires, register!)"
    (is (some? (find-ns 'metabase.driver.maxcompute)))))

(deftest ^:parallel driver-registered-test
  (testing ":maxcompute is registered with :sql-jdbc parent (derives from :sql as well)"
    ;; `isa?` against driver/hierarchy is the structural check; `driver/initialized?` is a runtime
    ;; state check that requires `driver/initialize!` to have run, which doesn't happen in unit tests.
    ;; The driver auto-detects whether `metabase.driver.sql-mbql5` exists (Metabase <= v0.63.x has it;
    ;; master removed it in PR #77529). When present, the driver registers with :sql-mbql5 as an
    ;; additional parent; when absent, :sql-jdbc alone suffices.
    (is (isa? driver/hierarchy :maxcompute :sql-jdbc))
    (is (isa? driver/hierarchy :maxcompute :sql))
    (let [mbql5-present? (try
                           (require 'metabase.driver.sql-mbql5)
                           true
                           (catch Throwable _ false))]
      (is (= mbql5-present?
             (isa? driver/hierarchy :maxcompute :sql-mbql5))
          ":sql-mbql5 derivation should match namespace availability"))))

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
