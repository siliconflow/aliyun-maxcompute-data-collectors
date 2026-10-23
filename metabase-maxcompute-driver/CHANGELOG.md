# Changelog

All notable changes to the MaxCompute Metabase Driver are recorded here.

## [Unreleased]

## [0.2.3] - 2026-10-23

- Format the SQL that Metabase displays and logs with the MySQL dialect. Core
  formats native SQL through `metabase.driver.sql.util/format-sql`, which picks
  a dialect from `metabase.driver.sql.util/dialects`; there is no `:maxcompute`
  entry, so it fell back to `StandardSql`, a dialect that does not understand
  backtick identifiers and rendered `` `tbl`.`col` `` as `` ` tbl `.` col ` ``.
  The text a user copies out of the query preview was therefore rejected by the
  engine with `ODPS-0130071: column ... cannot be resolved`. Execution is
  unaffected; `prettify-native-form` is display/log only.

## [0.2.2] - 2026-10-22

- Inline instants as `timestamp"<UTC wall clock>Z"` instead of
  `datetime"...Z"`. A `datetime` literal never accepts a zone suffix, so
  absolute-datetime filters failed with `ODPS-0130161: invalid DATETIME format`.
- Anchor time-of-day literals to the epoch date (`datetime"1970-01-01 hh:mm:ss"`);
  a bare `datetime"hh:mm:ss"` is also rejected by the engine.
- Drop the `ESCAPE '\'` clause from `LIKE` filters. The engine has a built-in
  backslash escape and does not accept the SQL-standard `ESCAPE` clause
  (`ODPS-0130161: invalid token`), so every `:contains` / `:starts-with` /
  `:ends-with` filter failed. Implemented through
  `transform-literal-like-pattern-honeysql`.
- Strip MBQL clause metadata maps in `:convert-timezone`, `:regex-match-first`,
  `:datetime-diff` and `:log`, matching the percentile fix in 0.2.1.

## [0.2.1] - 2026-10-22

- Fix `:percentile` and `:median` when the aggregation was renamed in the GUI:
  MBQL then carries a clause metadata map (`[:percentile {..} expr 0.9]`) which
  was passed to `->honeysql`, failing with `Unexpected ->honeysql call on a map`.
- Fix identifier qualification inside source queries and joins: MaxCompute only
  resolves a column qualifier when the full `<project>.<schema>.<table>` is
  quoted as one identifier, so three-or-more-component field identifiers are now
  qualified with the project id. Queries that became an aggregate over a source
  query previously failed with `ODPS-0130071: column ... cannot be resolved`.

## [0.2.0] - 2026-10-22

- Re-base the fork on upstream 0.1.1 and drop the legacy
  `unprepare/unprepare-value` implementations, which upstream replaced with
  `sql.qp/inline-value`.
- Correct engine-rejected SQL shapes verified against a live project:
  `TO_DATE(x, 'yyyymmddhhmiss')` instead of `PARSE_DATETIME`, two-argument
  `DATETRUNC(x, 'hh')`, `EXTRACT` without a timezone argument, composite
  `quarter` / `day-of-week` / `week` truncation, `DATEDIFF`-based
  `datetime-diff`, `GETDATE()` for "now", `DOUBLE` for `->float`.
- Fill capability gaps that the engine was verified to support:
  `:percentile-aggregations` (`PERCENTILE(x, p)` / `MEDIAN(x)`),
  `:standard-deviation-aggregations`, `:advanced-math-expressions`, `:regex`,
  `:binning`, `:full-join`, `:convert-timezone`.
- Escape inline string literals with a single backslash (`'Cam\'s String'`);
  the doubled-backslash form produced by upstream breaks the literal.
- Escape aliases collision-safely: a lossy fold of a non-ASCII alias now gets a
  CRC32 suffix so distinct joins cannot fold onto the same identifier.


- Correct the 0.1.1 compatibility status to verification-pending. Keep the
  artifact-specific ARRAY observations, but require full schema sync, native
  SELECT 1 and MBQL evidence before declaring the target window supported.

- Declare the supported scope as the current Metabase stable minor, in
  `compatibility.yaml` and `README.md`. Metabase minors that are no longer
  current are outside the window and fixes are not backported to them; the
  per-version observations for 0.1.1 are recorded in
  `docs/e2e/v0.1.1-metabase-supported-window.md`.
- Move the build baseline and the CI pin inside the supported window. The
  published 0.1.1 JAR is still compiled against 0.51.14, and the runtime
  fallback that resolves pre-0.56 Metabase driver APIs becomes dead code once
  the baseline moves.

## [0.1.1] - 2026-09-22

- Upgrade the bundled ODPS JDBC from 3.10.11 to 3.10.14.
- Append `legacyArrayGetObject=false` to the JDBC URL so ARRAY columns are
  returned as `java.sql.Array` from `ResultSet.getObject()`, matching the
  declared `Types.ARRAY` mapping Metabase relies on; the previous
  `ClassCastException` on ARRAY columns (and the `TO_JSON` workaround) is no
  longer needed on this artifact inside the supported Metabase window, which is
  `>=0.63.0, <0.64.0`. Outside that window the unwrapped ARRAY read is not
  guaranteed: on 0.51.x the value reaches the UI as an unrendered object
  reference. Requires ODPS JDBC >= 3.10.14 bundled.

## [0.1.0] - 2026-07-29

- Add compatibility with the Metabase 0.63 driver APIs.
- Pin the verified Metabase baseline to 0.63.1.12.
- Upgrade ODPS JDBC from 3.6.0 to 3.10.11 and bundle it in the driver JAR.
- Use the Metabase driver API for secret values and settings access.
- Migrate query processor extension points for float types, inline values, and
  field-filter dispatch.
- Remove the external JDBC class dependency declaration because the JDBC driver
  is packaged inside the plugin.
- Add a versioned compatibility matrix, reproducible build, checksum, SBOM,
  real MaxCompute E2E gate, and a reproducible release pipeline.

