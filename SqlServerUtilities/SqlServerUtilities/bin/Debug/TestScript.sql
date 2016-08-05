USE gcDev
GO

DROP TABLE ##temp;
GO

SELECT *
INTO ##temp
FROM gcDev.dbo.EtlRuntimeLog;

TRUNCATE TABLE gcDev.dbo.EtlRuntimeLog;

INSERT INTO gcDev.dbo.EtlRuntimeLog
SELECT *
FROM ##temp;