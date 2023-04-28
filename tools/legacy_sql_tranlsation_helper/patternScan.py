import re
import readSQLToString

#Replace your legacy_sql or file_path here. Only use one and put the other as an empty string.
legacy_sql = ""
file_path = "sample_sql/sample1.sql"

if file_path != "" and legacy_sql == "":
    legacy_sql = readSQLToString.read_file_path(file_path)

def legacy_sql_scan(legacy_sql):
    x = re.search(r"FROM\n*(.*)\[(.*)\],\s*\n*\s*\[", legacy_sql)
    if x:
        print("{} detected".format("COMMA JOIN"))

    x = re.search(r"FLATTEN", legacy_sql)
    if x:
        print("{} detected".format("FLATTEN"))

    x = re.search(r"@", legacy_sql)
    if x:
        print("{} detected".format("Time Decorator"))

    x = re.search(r"\[(.*?)\]", legacy_sql)
    if x:
        print("{} detected".format("Legacy Table Name"))

    x = re.search(r"(\b\w+)\s*%\s*(\w+\b)", legacy_sql)
    if x:
        print("{} detected".format("x % y --> MOD(x,y)"))

    x = re.search(r"LEFT\(", legacy_sql)
    if x:
        print("{} detected".format("LEFT(s, len) --> SUBSTR(s, 0, len)"))

    x = re.search(r"RIGHT\(", legacy_sql)
    if x:
        print("{} detected".format("RIGHT(s, len) --> SUBSTR(s, -len)"))

    x = re.search(r"CONTAINS '(.+?)'", legacy_sql)
    if x:
        print("{} detected".format("CONTAINS 'string' --> LIKE '%string%'"))

    x = re.search(r"DATE\(([\"\'])(\d{4})(\d{2})(\d{2})([\"\'])\)", legacy_sql)
    if x:
        print("{} detected".format("DATE(xxxxxxxx) --> DATE(xxxx-xx-xx)"))

    x = re.search(r"DATE_ADD\(TIMESTAMP\('(\d{4})(\d{2})(\d{2})'\),\s*(\d+),\s*'HOUR'\)", legacy_sql)
    if x:
        print("{} detected".format("DATE_ADD(TIMESTAMP('20230211'), 8, 'HOUR') --> DATE_ADD(TIMESTAMP('2023-02-11'), INTERVAL 8 HOUR)"))

    x = re.search(r"TIMESTAMP\(UTC_USEC_TO_HOUR\(TIMESTAMP_TO_USEC\((.*?)\)\)\)", legacy_sql)
    if x:
        print("{} detected".format("TIMESTAMP(UTC_USEC_TO_HOUR(TIMESTAMP_TO_USEC(event_time))) --> TIMESTAMP_TRUNC(event_time, HOUR, \"UTC\" )"))

    x = re.search(r"UTC_USEC_TO_HOUR\((\d+)\)", legacy_sql)
    if x:
        print("{} detected".format("UTC_USEC_TO_HOUR(123456789) --> UNIX_MICROS(TIMESTAMP_TRUNC(TIMESTAMP_MICROS(123456789), HOUR)"))

    x = re.search(r"INTEGER\((.*?)\)", legacy_sql)
    if x:
        print("{} detected".format("INTEGER(x) --> SAFE_CAST(x as INT64)"))

    x = re.search(r"DATEDIFF\((.*?),\s*(.*?)\)", legacy_sql)
    if x:
        print("{} detected".format("DATEDIFF(t1, t2) --> TIMESTAMP_DIFF(t1, t2, DAY)"))

    x = re.search(r"STRFTIME_UTC_USEC\((.*?),\s*(.*?)\)", legacy_sql)
    if x:
        print("{} detected".format("STRFTIME_UTC_USEC(t, fmt)	--> FORMAT_TIMESTAMP(fmt, t)"))

    x = re.search(r"UTC_USEC_TO_DAY\((.*?)\)", legacy_sql)
    if x:
        print("{} detected".format("UTC_USEC_TO_DAY(t) --> TIMESTAMP_TRUNC(t, DAY)"))

    x = re.search(r"IS_NULL\((.*?)\)", legacy_sql)
    if x:
        print("{} detected".format("IS_NULL(x) --> x IS NULL"))

    x = re.search(r"REGEXP_MATCH", legacy_sql)
    if x:
        print("{} detected".format("REGEXP_MATCH --> REGEXP_CONTAINS"))

    x = re.search(r"USEC_TO_TIMESTAMP", legacy_sql)
    if x:
        print("{} detected".format("USEC_TO_TIMESTAMP --> TIMESTAMP_MICROS"))

    x = re.search(r"TIMESTAMP_TO_USEC", legacy_sql)
    if x:
        print("{} detected".format("TIMESTAMP_TO_USEC --> UNIX_MICROS"))

    x = re.search(r"SEC_TO_TIMESTAMP", legacy_sql)
    if x:
        print("{} detected".format("SEC_TO_TIMESTAMP --> TIMESTAMP_SECONDS"))

    x = re.search(r"TIMESTAMP_TO_MSEC", legacy_sql)
    if x:
        print("{} detected".format("TIMESTAMP_TO_MSEC --> UNIX_MILLIS"))

    x = re.search(r"INSTR", legacy_sql)
    if x:
        print("{} detected".format("INSTR --> STRPOS"))

    x = re.search(r"GROUP_CONCAT_UNQUOTED", legacy_sql)
    if x:
        print("{} detected".format("GROUP_CONCAT_UNQUOTED --> STRING_AGG"))

    x = re.search(r"GROUP_CONCAT", legacy_sql)
    if x:
        print("{} detected".format("GROUP_CONCAT --> STRING_AGG and a UDF"))

    x = re.search(r"NOW", legacy_sql)
    if x:
        print("{} detected".format("NOW --> CURRENT_TIMESTAMP"))

    x = re.search(r"UNIQUE", legacy_sql)
    if x:
        print("{} detected".format("UNIQUE --> DISTINCT"))

    x = re.search(r"TABLE_DATE_RANGE", legacy_sql)
    if x:
        print("{} detected".format("TABLE_DATE_RANGE --> _TABLE_SUFFIX BETWEEN date1 and date2"))

    x = re.search(r"hash", legacy_sql)
    if x:
        print("{} detected".format("TABLE_DATE_RANGE --> _TABLE_SUFFIX BETWEEN date1 and date2"))

    x = re.search(r"STRING\(", legacy_sql)
    if x:
        print("{} detected".format("STRING(bool_column) --> CAST(CAST(bool_column AS INT64) AS STRING)"))
legacy_sql_scan(legacy_sql)

