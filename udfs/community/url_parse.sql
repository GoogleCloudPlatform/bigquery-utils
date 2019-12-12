--parse_url(string urlString, string partToExtract [, string keyToExtract])
--Returns the specified part from the URL. Valid values for partToExtract include PROTOCOL, HOST, PATH, QUERY, and REF.
--For example, parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'HOST') returns 'facebook.com'.
CREATE OR REPLACE FUNCTION fn.check_protocol(url STRING)
AS (
  CASE
    WHEN REGEXP_CONTAINS(url, '^[a-zA-Z]+://') THEN url
    ELSE CONCAT('http://', url)
  END
);

CREATE OR REPLACE FUNCTION fn.parse_url(url STRING, part STRING)
AS (
  CASE
    -- Return HOST part of the URL.
    WHEN part = 'HOST' THEN SPLIT(check_protocol(url), '/')[OFFSET(2)]
    WHEN part = 'PATH' THEN REGEXP_EXTRACT(url, r'^[a-zA-Z]+://[a-zA-Z0-9.-]+/([a-zA-Z0-9.-/]+)') 
    WHEN part = 'QUERY' THEN 
      IF(REGEXP_CONTAINS(url, r'\?'), SPLIT(check_protocol(url), '?')[OFFSET(1)], NULL)
    WHEN part = 'REF' THEN
      IF(REGEXP_CONTAINS(url, '#'), SPLIT(check_protocol(url), '#')[OFFSET(1)], NULL)
    WHEN part = 'PROTOCOL' THEN RTRIM(REGEXP_EXTRACT(url, '^[a-zA-Z]+://'), '://')
    ELSE ''
  END
);
