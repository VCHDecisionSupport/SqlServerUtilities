

;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Copy HCC fact tables from production to NewTest' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ClientFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ClientFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Copy HCC fact tables from production to NewTest' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.IncomeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.IncomeFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Copy HCC fact tables from production to NewTest' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ServiceFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ServiceFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Copy HCC fact tables from production to NewTest' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ServiceVisitFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ServiceVisitFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'New Package' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dim.ICareGroupUnitMapping',1) 
	AND obj.ObjectSchemaName = PARSENAME('dim.ICareGroupUnitMapping',2) 
	AND db.DatabaseName = 'ICAREMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRChildADR' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract3mFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract3mFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRChildADR' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AbstractAcuteFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AbstractAcuteFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRChildADR' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AbstractDayCareFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AbstractDayCareFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRChildADR' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AbstractRehabFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AbstractRehabFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRChildDx' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AbstractDxFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AbstractDxFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRChildPx' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AbstractPxFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AbstractPxFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRChildSCU' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AbstractScuFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AbstractScuFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRConsolidatedPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract_3M',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract_3M',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRConsolidatedPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkPHC_CareProvider',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkPHC_CareProvider',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRConsolidatedPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkPHC_Visit',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkPHC_Visit',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRConsolidatedPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkPHC_Visit',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkPHC_Visit',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRConsolidatedPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract_Dx',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract_Dx',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRConsolidatedPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkPHC_Dx',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkPHC_Dx',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRConsolidatedPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract_Px',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract_Px',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRConsolidatedPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkPHC_Px',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkPHC_Px',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRConsolidatedPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract_Scu',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract_Scu',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRConsolidatedPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkPHC_SCU',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkPHC_SCU',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRLoad2Prod' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ADR.AbstractFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ADR.AbstractFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRLoad2Prod' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ADR.DxFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ADR.DxFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRLoad2Prod' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ADR.DoctorFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ADR.DoctorFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRLoad2Prod' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ADR.PxFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ADR.PxFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRLoad2Prod' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ADR.ScuFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ADR.ScuFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRLoadDx' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract2DxFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract2DxFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRLoadPx' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract2PxFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract2PxFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRLoadSCU' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract2ProviderFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract2ProviderFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRLoadSCU' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract2ScuFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract2ScuFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRLoadToProd' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ADR.AbstractAcuteFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ADR.AbstractAcuteFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRLoadToProd' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ADR.AbstractDayCareFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ADR.AbstractDayCareFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRLoadToProd' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ADR.AbstractDxFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ADR.AbstractDxFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRLoadToProd' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ADR.AbstractPxFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ADR.AbstractPxFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRLoadToProd' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ADR.AbstractRehabFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ADR.AbstractRehabFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRLoadToProd' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ADR.AbstractScuFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ADR.AbstractScuFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract2_3M',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract2_3M',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkPHC2_Px',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkPHC2_Px',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkPHC2_Px',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkPHC2_Px',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkPHC2_Px',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkPHC2_Px',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract2_Px',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract2_Px',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkVGH_3M',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkVGH_3M',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkPHC2_3M',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkPHC2_3M',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkPHC2_3M',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkPHC2_3M',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkPHC2_3M',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkPHC2_3M',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkPHC2_Dx',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkPHC2_Dx',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkPHC2_Dx',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkPHC2_Dx',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract2_Dx',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract2_Dx',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkPHC2_Scu',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkPHC2_Scu',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract2_Provider',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract2_Provider',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract2_Scu',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract2_Scu',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainVH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract2_3M',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract2_3M',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainVH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkVGH2_Px',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkVGH2_Px',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainVH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkVGH2_Px',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkVGH2_Px',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainVH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkVGH2_Px',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkVGH2_Px',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainVH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract2_Px',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract2_Px',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainVH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkVGH2_3M',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkVGH2_3M',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainVH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkVGH_3M',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkVGH_3M',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainVH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkVGH2_3M',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkVGH2_3M',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainVH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkVGH2_3M',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkVGH2_3M',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainVH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkVGH2_Dx',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkVGH2_Dx',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainVH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkVGH2_Dx',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkVGH2_Dx',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainVH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract2_Dx',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract2_Dx',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainVH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WorkVGH2_Scu',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WorkVGH2_Scu',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainVH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract2_Provider',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract2_Provider',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRMainVH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract2_Scu',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract2_Scu',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRTransform' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Abstract2Fact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Abstract2Fact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADRTransform_ReloadPatient' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ADR.VCH_Reload_PatientInfo',1) 
	AND obj.ObjectSchemaName = PARSENAME('ADR.VCH_Reload_PatientInfo',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AdmissionStageLG',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AdmissionStageLG',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AdmissionStageLG',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AdmissionStageLG',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AdmissionStageLG',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AdmissionStageLG',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AdmissionStageLG',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AdmissionStageLG',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AdmissionStageLG',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AdmissionStageLG',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.admissionfact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.admissionfact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PRAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AdmissionPRStage',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AdmissionPRStage',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PRAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.admissionfact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.admissionfact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.SMHStageAdmission',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.SMHStageAdmission',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.AdmissionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.AdmissionFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AdmissionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AdmissionFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AdmissionStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AdmissionStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AdmissionStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AdmissionStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AdmissionStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AdmissionStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AdmissionStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AdmissionStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AdmissionStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AdmissionStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AdmissionStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AdmissionStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AdmissionStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AdmissionStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.AdmissionStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.AdmissionStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHAdmissions' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.admissionfact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.admissionfact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADTCMart Populate' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ADTC.AdmissionDischargeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ADTC.AdmissionDischargeFact',2) 
	AND db.DatabaseName = 'ADTCMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADTCMart Populate' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ADTC.CensusFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ADTC.CensusFact',2) 
	AND db.DatabaseName = 'ADTCMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADTCMart Populate' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ADTC.TransferFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ADTC.TransferFact',2) 
	AND db.DatabaseName = 'ADTCMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADTCMart Populate' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ADTC.CombineActivityFlat',1) 
	AND obj.ObjectSchemaName = PARSENAME('ADTC.CombineActivityFlat',2) 
	AND db.DatabaseName = 'ADTCMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADTCMart Populate' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCMart_Census',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCMart_Census',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADTCMart Populate' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCMart_CensusNew',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCMart_CensusNew',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADTCMart Populate' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCMart_Census',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCMart_Census',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LOS_Population' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ICareParisALCCube',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ICareParisALCCube',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LOS_Population' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.EligibleALCDays',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.EligibleALCDays',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LOS_Population' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.SM_Census',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.SM_Census',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LOS_Population' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.SM_Discharge',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.SM_Discharge',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LOS_Population' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ICare.ICareParisALCCube',1) 
	AND obj.ObjectSchemaName = PARSENAME('ICare.ICareParisALCCube',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LOS_Population' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('LOS.LOSFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('LOS.LOSFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LOS_Population' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.EligibleALCDays',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.EligibleALCDays',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LOS_Population' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LOS',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LOS',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHCensus' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CensusStageLG',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CensusStageLG',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHCensus' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CensusStageLG',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CensusStageLG',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHCensus' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CensusStageLG',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CensusStageLG',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHCensus' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.CensusFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.CensusFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PRCensus' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CensusPRStage',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CensusPRStage',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PRCensus' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CensusPRStage',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CensusPRStage',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PRCensus' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.CensusFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.CensusFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHCensus' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CensusStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CensusStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHCensus' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CensusStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CensusStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHCensus' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CensusStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CensusStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHCensus' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CensusStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CensusStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHCensus' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CensusStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CensusStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHCensus' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CensusStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CensusStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHCensus' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CensusStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CensusStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHCensus' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CensusStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CensusStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHCensus' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.CensusFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.CensusFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VH-CensusTminus1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.tmpStage4CensusVHTminus1',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.tmpStage4CensusVHTminus1',2) 
	AND db.DatabaseName = 'ADTC' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VH-CensusTminus1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.tmpStage3CensusVHTminus1',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.tmpStage3CensusVHTminus1',2) 
	AND db.DatabaseName = 'ADTC' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VH-CensusTminus1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Censusdatacurrentvhtminus1',1) 
	AND obj.ObjectSchemaName = PARSENAME('Censusdatacurrentvhtminus1',2) 
	AND db.DatabaseName = 'ADTC' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VH-CensusTminus1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.tmpStage6CensusVHTminus1',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.tmpStage6CensusVHTminus1',2) 
	AND db.DatabaseName = 'ADTC' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VH-CensusTminus1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ETL_ErrorCensusVHTminus1',1) 
	AND obj.ObjectSchemaName = PARSENAME('ETL_ErrorCensusVHTminus1',2) 
	AND db.DatabaseName = 'ADTC' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VH-CensusTminus1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.tmpStage2CensusVHTminus1',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.tmpStage2CensusVHTminus1',2) 
	AND db.DatabaseName = 'ADTC' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VH-CensusTminus1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.tmpStage5CensusVHTminus1',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.tmpStage5CensusVHTminus1',2) 
	AND db.DatabaseName = 'ADTC' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VH-CensusTminus1 1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.tmpStage4CensusVHTminus1',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.tmpStage4CensusVHTminus1',2) 
	AND db.DatabaseName = 'ADTC' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VH-CensusTminus1 1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.tmpStage3CensusVHTminus1',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.tmpStage3CensusVHTminus1',2) 
	AND db.DatabaseName = 'ADTC' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VH-CensusTminus1 1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Censusdatacurrentvhtminus1',1) 
	AND obj.ObjectSchemaName = PARSENAME('Censusdatacurrentvhtminus1',2) 
	AND db.DatabaseName = 'ADTC' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VH-CensusTminus1 1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.tmpStage6CensusVHTminus1',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.tmpStage6CensusVHTminus1',2) 
	AND db.DatabaseName = 'ADTC' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VH-CensusTminus1 1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ETL_ErrorCensusVHTminus1',1) 
	AND obj.ObjectSchemaName = PARSENAME('ETL_ErrorCensusVHTminus1',2) 
	AND db.DatabaseName = 'ADTC' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VH-CensusTminus1 1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.tmpStage2CensusVHTminus1',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.tmpStage2CensusVHTminus1',2) 
	AND db.DatabaseName = 'ADTC' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VH-CensusTminus1 1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.tmpStage5CensusVHTminus1',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.tmpStage5CensusVHTminus1',2) 
	AND db.DatabaseName = 'ADTC' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DischargeStageLG',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DischargeStageLG',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DischargeStageLG',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DischargeStageLG',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DischargeStageLG',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DischargeStageLG',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DischargeStageLG',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DischargeStageLG',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DischargeStageLG',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DischargeStageLG',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.DischargeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.DischargeFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PRDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DischargePRStage',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DischargePRStage',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PRDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DischargePRStage',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DischargePRStage',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PRDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.DischargeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.DischargeFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DischargeStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DischargeStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DischargeStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DischargeStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DischargeStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DischargeStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DischargeStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DischargeStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DischargeStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DischargeStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DischargeStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DischargeStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DischargeStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DischargeStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DischargeStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DischargeStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHDischarges' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.DischargeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.DischargeFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_ApptBase',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_ApptBase',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_EncntrBase',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_EncntrBase',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_Location',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_Location',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_LocHist',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_LocHist',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_PatientAddress',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_PatientAddress',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_PatientBase',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_PatientBase',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_PersonPatient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_PersonPatient',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_Prsnl',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_Prsnl',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_PrsnlReltn',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_PrsnlReltn',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_PersonCode',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_PersonCode',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_PersonCode',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_PersonCode',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.EncntrBase',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.EncntrBase',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.LocHist',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.LocHist',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.PrsnlReltn',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.PrsnlReltn',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.ApptBase',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.ApptBase',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.Location',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.Location',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.PatientAddress',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.PatientAddress',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.PatientBase',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.PatientBase',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.PersonCode',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.PersonCode',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.PersonPatient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.PersonPatient',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerSDA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.Prsnl',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.Prsnl',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMLoadEncounter' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CernerPatientAddress',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CernerPatientAddress',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMLoadEncounter' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CernerPatientBase',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CernerPatientBase',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMLoadEncounter' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CernerPersonPatient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CernerPersonPatient',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMLoadEncounter' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CernerEncntrBase',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CernerEncntrBase',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMLoadEncounter' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CernerLocHist',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CernerLocHist',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMLoadEncounter' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CernerPrsnl',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CernerPrsnl',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMLoadEncounter' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CernerPrsnlReltn',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CernerPrsnlReltn',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMLoadEncounter' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.ActivityFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.ActivityFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMLoadEncounter' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.ActivityFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.ActivityFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMLoadEncounter' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.ActivityFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.ActivityFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMLoadEncounter' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.AdmissionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.AdmissionFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMLoadEncounter' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.DischargeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.DischargeFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMLoadEncounter' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ED.CurrentVisitFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ED.CurrentVisitFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMLoadEncounter' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.TransferFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.TransferFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHTransfer' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TransferStageLG',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TransferStageLG',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHTransfer' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TransferStageLG',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TransferStageLG',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHTransfer' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TransferStageLG',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TransferStageLG',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGHTransfer' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.TransferFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.TransferFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PRTransfer' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TransferPRStage',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TransferPRStage',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PRTransfer' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.TransferFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.TransferFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHTransfer' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TransferStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TransferStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHTransfer' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TransferStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TransferStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHTransfer' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TransferStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TransferStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHTransfer' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TransferStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TransferStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHTransfer' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TransferStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TransferStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHTransfer' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TransferStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TransferStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHTransfer' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TransferStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TransferStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHTransfer' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TransferStageVH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TransferStageVH',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'VHTransfer' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.TransferFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.TransferFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_ApptBase',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_ApptBase',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_EncntrBase',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_EncntrBase',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_Location',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_Location',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_LocHist',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_LocHist',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_PatientAddress',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_PatientAddress',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_PatientBase',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_PatientBase',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_PersonCode',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_PersonCode',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_PersonPatient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_PersonPatient',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_Prsnl',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_Prsnl',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Cerner_PrsnlReltn',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Cerner_PrsnlReltn',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.ApptBase',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.ApptBase',2) 
	AND db.DatabaseName = 'DSSI' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.EncntrBase',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.EncntrBase',2) 
	AND db.DatabaseName = 'DSSI' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.Location',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.Location',2) 
	AND db.DatabaseName = 'DSSI' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.LocHist',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.LocHist',2) 
	AND db.DatabaseName = 'DSSI' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.PatientAddress',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.PatientAddress',2) 
	AND db.DatabaseName = 'DSSI' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.PatientBase',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.PatientBase',2) 
	AND db.DatabaseName = 'DSSI' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.PersonCode',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.PersonCode',2) 
	AND db.DatabaseName = 'DSSI' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.PersonPatient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.PersonPatient',2) 
	AND db.DatabaseName = 'DSSI' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.Prsnl',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.Prsnl',2) 
	AND db.DatabaseName = 'DSSI' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SMCernerLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Cerner.PrsnlReltn',1) 
	AND obj.ObjectSchemaName = PARSENAME('Cerner.PrsnlReltn',2) 
	AND db.DatabaseName = 'DSSI' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSExtract',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSExtract',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSControlRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSControlRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('CCRS.ExtractFile',1) 
	AND obj.ObjectSchemaName = PARSENAME('CCRS.ExtractFile',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSLoadtoDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSAdmission',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSAdmission',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSLoadtoDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSDischarge',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSDischarge',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSLoadtoDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSRaiRCAssessment',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSRaiRCAssessment',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSLoadtoDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSRaiRCMedication',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSRaiRCMedication',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSLoadtoDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSRaiRCAssessment',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSRaiRCAssessment',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSLoadtoDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSUpdate',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSUpdate',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSLoadtoDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSAdmission',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSAdmission',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSLoadtoDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSRaiRCAssessment',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSRaiRCAssessment',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSLoadtoDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSDischarge',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSDischarge',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSLoadtoDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSRaiRCMedication',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSRaiRCMedication',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSLoadtoDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSUpdate',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSUpdate',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSLoadtoDSDW_prod' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSAdmission',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSAdmission',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSLoadtoDSDW_prod' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSDischarge',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSDischarge',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSLoadtoDSDW_prod' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSRaiRCAssessment',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSRaiRCAssessment',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSLoadtoDSDW_prod' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSRaiRCMedication',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSRaiRCMedication',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSLoadtoDSDW_prod' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSRaiRCAssessment',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSRaiRCAssessment',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSLoadtoDSDW_prod' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSUpdate',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSUpdate',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSReloadfromSourceDataArchive' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSExtract',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSExtract',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSXml' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('CCRS.ExtractFile',1) 
	AND obj.ObjectSchemaName = PARSENAME('CCRS.ExtractFile',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityAddress',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityAddress',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityALClientNeeds',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityALClientNeeds',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityAlert',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityAlert',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityAssessmentAbuse',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityAssessmentAbuse',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityAssessmentHeader',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityAssessmentHeader',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCarePlanPattern',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCarePlanPattern',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCarePlanProfile',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCarePlanProfile',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCaseNoteAbuse',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCaseNoteAbuse',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCaseNoteClinicalService',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCaseNoteClinicalService',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCaseNoteContact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCaseNoteContact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCaseNoteGroupAttendance',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCaseNoteGroupAttendance',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCaseNoteHeader',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCaseNoteHeader',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityClient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityClient',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCurrentLocation',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCurrentLocation',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityClientGroup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityClientGroup',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityClientRegister',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityClientRegister',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityClientContribution',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityClientContribution',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCaseNoteContactHCC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCaseNoteContactHCC',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCriminalJustice',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCriminalJustice',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityDeath',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityDeath',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityDiagnosis',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityDiagnosis',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityDischarge',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityDischarge',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityEmployment',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityEmployment',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityFundingSource',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityFundingSource',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityHoNOSAdult',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityHoNOSAdult',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityHoNOSChild',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityHoNOSChild',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityHoNOSLearnDisable',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityHoNOSLearnDisable',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityHouseHoldComposition',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityHouseHoldComposition',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityIdentifier',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityIdentifier',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityIntervention',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityIntervention',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityInvoiceClient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityInvoiceClient',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityInvoiceClientVisit',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityInvoiceClientVisit',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityInvoiceHeader',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityInvoiceHeader',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityInvolvedProfession',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityInvolvedProfession',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityLegalStatus',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityLegalStatus',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityLTC6FinancialAffair',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityLTC6FinancialAffair',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityLTC6FinancialCalc',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityLTC6FinancialCalc',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityLTC6TempReduction',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityLTC6TempReduction',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityMaritalStatus',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityMaritalStatus',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityMDSHCAABB',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityMDSHCAABB',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityParenting',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityParenting',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityPayCosting',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityPayCosting',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityPersonName',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityPersonName',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityPregnancy',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityPregnancy',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityPriorityAccessScreener',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityPriorityAccessScreener',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('"Staging"."CommunityReferral"',1) 
	AND obj.ObjectSchemaName = PARSENAME('"Staging"."CommunityReferral"',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityReferralAbuse',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityReferralAbuse',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityReferralContact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityReferralContact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityReferralInPatient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityReferralInPatient',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityReferralRelationship',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityReferralRelationship',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunitySchoolEducation',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunitySchoolEducation',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunitySponsoredImmigrant',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunitySponsoredImmigrant',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunitySubstanceUseMHA',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunitySubstanceUseMHA',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityTCUAdmission',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityTCUAdmission',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityTCUDischarge',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityTCUDischarge',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityTSTHospital',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityTSTHospital',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityVeteranStatus',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityVeteranStatus',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityWaitlistClientOffer',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityWaitlistClientOffer',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityWaitlistDefinition',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityWaitlistDefinition',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityWaitlistEntry',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityWaitlistEntry',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityWaitlistProviderOffer',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityWaitlistProviderOffer',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityWaitlistSuspension',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityWaitlistSuspension',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityWeightGrowth',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityWeightGrowth',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.PersonFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.PersonFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.AssessmentContactFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.AssessmentContactFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.CarePlanHeaderFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.CarePlanHeaderFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.CarePlanNeedFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.CarePlanNeedFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ClientGPFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ClientGPFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.HoNOSAdultFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.HoNOSAdultFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.HoNOSChildFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.HoNOSChildFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.HoNOSLearningDisabilityFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.HoNOSLearningDisabilityFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.HSServiceSummaryFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.HSServiceSummaryFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ImmAdverseEventFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ImmAdverseEventFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ImmunizationAlertFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ImmunizationAlertFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ImmunizationHistoryFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ImmunizationHistoryFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.MedRecFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.MedRecFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.RiskScreenerFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.RiskScreenerFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.SchoolHistoryFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.SchoolHistoryFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.AddressFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.AddressFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.AlertFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.AlertFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.AssessmentHeaderFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.AssessmentHeaderFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.AbuseFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.AbuseFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.CarePlanPatternFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.CarePlanPatternFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Map.CarePlanProfile',1) 
	AND obj.ObjectSchemaName = PARSENAME('Map.CarePlanProfile',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.CaseNoteContactFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.CaseNoteContactFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.AbuseFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.AbuseFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.CaseNoteClinicalServiceFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.CaseNoteClinicalServiceFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.CaseNoteContactFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.CaseNoteContactFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.GroupAttendanceFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.GroupAttendanceFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.CaseNoteHeaderFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.CaseNoteHeaderFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ClientGroupFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ClientGroupFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ClientRegisterFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ClientRegisterFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.FinancialAssessmentFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.FinancialAssessmentFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.CriminalJusticeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.CriminalJusticeFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.CurrentLocationFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.CurrentLocationFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.DeathFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.DeathFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.DiagnosisFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.DiagnosisFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.DiagnosisGAFFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.DiagnosisGAFFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.DischargeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.DischargeFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.EmploymentFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.EmploymentFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.FundingSourceFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.FundingSourceFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.HouseHoldCompositionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.HouseHoldCompositionFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.IdentifierFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.IdentifierFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.InterventionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.InterventionFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.InvoiceClientFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.InvoiceClientFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.InvoiceClientVisitFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.InvoiceClientVisitFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.InvoiceHeaderFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.InvoiceHeaderFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.InvolvedProfessionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.InvolvedProfessionFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.LegalStatusFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.LegalStatusFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.MaritalStatusFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.MaritalStatusFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.MDSHCCAboriginalFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.MDSHCCAboriginalFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ParentingFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ParentingFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.PatientFocusFundingFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.PatientFocusFundingFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Map.PayCosting',1) 
	AND obj.ObjectSchemaName = PARSENAME('Map.PayCosting',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.PersonNameFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.PersonNameFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.PregnancyFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.PregnancyFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ReferralFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ReferralFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.AbuseFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.AbuseFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ReferralInPatientFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ReferralInPatientFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Map.ReferralRelationship',1) 
	AND obj.ObjectSchemaName = PARSENAME('Map.ReferralRelationship',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.SchoolEducationFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.SchoolEducationFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ScreeningResultFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ScreeningResultFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.SubstanceUseFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.SubstanceUseFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.WaitlistClientOfferFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.WaitlistClientOfferFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.WaitlistDefinitionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.WaitlistDefinitionFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.WaitlistEntryFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.WaitlistEntryFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.WaitlistProviderOfferFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.WaitlistProviderOfferFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.WaitlistSuspensionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.WaitlistSuspensionFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.WeightGrowthFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.WeightGrowthFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.YouthClinicActivityFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.YouthClinicActivityFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadDSDWChild1' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.SLPActivityFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.SLPActivityFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityAddress',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityAddress',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityAlert',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityAlert',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityAssessmentContact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityAssessmentContact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityAssessmentHeader',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityAssessmentHeader',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCarePlanHeader',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCarePlanHeader',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCarePlanNeed',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCarePlanNeed',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCarePlanProfile',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCarePlanProfile',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityClient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityClient',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityClientGP',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityClientGP',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityClientGroup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityClientGroup',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityClientRegister',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityClientRegister',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCriminalJustice',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCriminalJustice',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCurrentLocation',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCurrentLocation',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityDeath',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityDeath',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityEmployment',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityEmployment',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityFundingSource',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityFundingSource',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityHoNOSAdult',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityHoNOSAdult',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityHoNOSChild',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityHoNOSChild',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityHoNOSLearnDisable',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityHoNOSLearnDisable',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityHouseHoldComposition',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityHouseHoldComposition',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityHSServiceSummary',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityHSServiceSummary',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityIdentifier',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityIdentifier',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityImmAdverseEvent',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityImmAdverseEvent',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityImmunizationAlert',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityImmunizationAlert',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityImmunizationHistory',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityImmunizationHistory',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityInvolvedProfession',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityInvolvedProfession',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityLegalStatus',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityLegalStatus',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityMaritalStatus',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityMaritalStatus',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityMedRec',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityMedRec',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityParenting',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityParenting',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityPatientFocusFunding',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityPatientFocusFunding',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityPayCosting',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityPayCosting',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityPersonName',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityPersonName',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityPregnancy',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityPregnancy',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityReferralInPatient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityReferralInPatient',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityReferralRelationship',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityReferralRelationship',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityRiskScreener',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityRiskScreener',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunitySchoolEducation',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunitySchoolEducation',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunitySchoolHistory',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunitySchoolHistory',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityScreeningResult',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityScreeningResult',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunitySLPActivity',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunitySLPActivity',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityWaitlistClientOffer',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityWaitlistClientOffer',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityWaitlistDefinition',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityWaitlistDefinition',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityWaitlistEntry',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityWaitlistEntry',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityWaitlistProviderOffer',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityWaitlistProviderOffer',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityWaitlistSuspension',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityWaitlistSuspension',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityWeightGrowth',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityWeightGrowth',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityYouthClinicActivity',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityYouthClinicActivity',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityLoadKDCDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityMaritalStatus',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityMaritalStatus',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityAddress',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityAddress',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityAlert',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityAlert',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityAssessmentAbuse',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityAssessmentAbuse',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityAssessmentHeader',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityAssessmentHeader',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCaseNoteAbuse',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCaseNoteAbuse',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCaseNoteClinicalService',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCaseNoteClinicalService',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCaseNoteContact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCaseNoteContact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCaseNoteGroupAttendance',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCaseNoteGroupAttendance',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCaseNoteHeader',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCaseNoteHeader',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityClient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityClient',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCurrentLocation',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCurrentLocation',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityClientGroup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityClientGroup',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityClientRegister',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityClientRegister',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCaseNoteContactHCC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCaseNoteContactHCC',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityCriminalJustice',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityCriminalJustice',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityDeath',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityDeath',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityDiagnosis',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityDiagnosis',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityDischarge',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityDischarge',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityEmployment',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityEmployment',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityHoNOSAdult',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityHoNOSAdult',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityHoNOSChild',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityHoNOSChild',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityHoNOSLearnDisable',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityHoNOSLearnDisable',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityHouseHoldComposition',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityHouseHoldComposition',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityIdentifier',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityIdentifier',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityIntervention',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityIntervention',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityLegalStatus',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityLegalStatus',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityMaritalStatus',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityMaritalStatus',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityMDSHCAABB',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityMDSHCAABB',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityParenting',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityParenting',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityPregnancy',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityPregnancy',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('"Staging"."CommunityReferral"',1) 
	AND obj.ObjectSchemaName = PARSENAME('"Staging"."CommunityReferral"',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityReferralAbuse',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityReferralAbuse',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunitySchoolEducation',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunitySchoolEducation',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunitySubstanceUseMHA',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunitySubstanceUseMHA',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityWaitlistClientOffer',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityWaitlistClientOffer',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityWaitlistDefinition',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityWaitlistDefinition',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityWaitlistEntry',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityWaitlistEntry',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityWaitlistProviderOffer',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityWaitlistProviderOffer',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityReloadDSDW' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CommunityWeightGrowth',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CommunityWeightGrowth',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRParent' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LoadParisHCCMRRActual',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LoadParisHCCMRRActual',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRParent' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LoadParisHCCMRRClientRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LoadParisHCCMRRClientRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRParent' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LoadParisHCCMRRClient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LoadParisHCCMRRClient',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRParent' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LoadParisHCCMRRIncomeRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LoadParisHCCMRRIncomeRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRParent' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LoadParisHCCMRRIncome',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LoadParisHCCMRRIncome',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRParent' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LoadParisHCCMRRServiceRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LoadParisHCCMRRServiceRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRParent' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LoadParisHCCMRRService',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LoadParisHCCMRRService',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRParent' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ServiceVisitFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ServiceVisitFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRParent' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ClientFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ClientFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRParent' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.IncomeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.IncomeFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRParent' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ServiceFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ServiceFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRReloadClient' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LoadParisHCCMRRActual',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LoadParisHCCMRRActual',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRReloadClient' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LoadParisHCCMRRClientRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LoadParisHCCMRRClientRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRReloadClient' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LoadParisHCCMRRClient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LoadParisHCCMRRClient',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRReloadClient' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LoadParisHCCMRRIncomeRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LoadParisHCCMRRIncomeRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRReloadClient' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LoadParisHCCMRRIncome',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LoadParisHCCMRRIncome',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRReloadClient' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LoadParisHCCMRRServiceRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LoadParisHCCMRRServiceRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRReloadClient' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LoadParisHCCMRRService',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LoadParisHCCMRRService',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRReloadClient' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ServiceVisitFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ServiceVisitFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRReloadClient' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ClientFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ClientFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRReloadClient' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.IncomeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.IncomeFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityHCCMRRReloadClient' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Community.ServiceFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Community.ServiceFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HCCMRRProcessSubmissionReport' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCCMRRLoadSubmissionXMLReport',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCCMRRLoadSubmissionXMLReport',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HCCMRRProcessSubmissionReport' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCCMRRLoadSubmissionXMLSummaryReport',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCCMRRLoadSubmissionXMLSummaryReport',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HCCMRRProcessSubmissionReport' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCCMRRLoadSubmissionXMLSummaryReport',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCCMRRLoadSubmissionXMLSummaryReport',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HCCMRRProcessSubmissionReport' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCCMRRLoadSubmissionXMLSummaryReport',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCCMRRLoadSubmissionXMLSummaryReport',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HCCMRRProcessSubmissionReport' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCCMRRLoadSubmissionXMLSummaryReport',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCCMRRLoadSubmissionXMLSummaryReport',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HCCMRRProcessSubmissionReport' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCCMRRLoadSubmissionXMLSummaryReport',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCCMRRLoadSubmissionXMLSummaryReport',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HCCMRRProcessSubmissionReport' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCCMRRLoadSubmissionXMLSummaryReport',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCCMRRLoadSubmissionXMLSummaryReport',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HCCMRRSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCCMRR.ExtractHCCClient',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCCMRR.ExtractHCCClient',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HCCMRRSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCCMRR.ExtractHCCIncome',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCCMRR.ExtractHCCIncome',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HCCMRRSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCCMRR.ExtractHCCService',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCCMRR.ExtractHCCService',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateHCCMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.ClientFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.ClientFact',2) 
	AND db.DatabaseName = 'HCCMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateHCCMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.ServiceFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.ServiceFact',2) 
	AND db.DatabaseName = 'HCCMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateHCCMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.ServiceVisitFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.ServiceVisitFact',2) 
	AND db.DatabaseName = 'HCCMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HCRSGenerateCIRecord' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputFile',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputFile',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HCRSGenerateCIRecord' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractOrganizationContactInfoFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractOrganizationContactInfoFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HCRSGenerateCIRecord' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractFile',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractFile',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSRecordDelete',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSRecordDelete',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSRecordDeleteReloadSDA',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSRecordDeleteReloadSDA',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSRecordDeleteReloadSDA',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSRecordDeleteReloadSDA',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSRecordDeleteReloadSDA',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSRecordDeleteReloadSDA',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSRecordDeleteReloadSDA',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSRecordDeleteReloadSDA',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSRecordDeleteReloadSDA',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSRecordDeleteReloadSDA',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSRecordDeleteReloadSDA',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSRecordDeleteReloadSDA',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSRecordDeleteReloadSDA',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSRecordDeleteReloadSDA',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSRecordDeleteReloadSDA',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSRecordDeleteReloadSDA',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractAdmissionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractAdmissionFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractDischargeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractDischargeFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractERVisitFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractERVisitFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractRaiHCMedicationFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractRaiHCMedicationFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractRaiHCAssessmentFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractRaiHCAssessmentFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractServiceDetailFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractServiceDetailFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractServiceEndFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractServiceEndFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractServiceStartFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractServiceStartFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractClientProfileUpdateFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractClientProfileUpdateFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild1GenerateHCRFileDeleteRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputFile',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputFile',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractAdmissionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractAdmissionFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractDischargeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractDischargeFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractERVisitFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractERVisitFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputFile',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputFile',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractRaiHCMedicationFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractRaiHCMedicationFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractRaiHCAssessmentFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractRaiHCAssessmentFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractServiceDetailFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractServiceDetailFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractServiceEndFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractServiceEndFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractServiceStartFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractServiceStartFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractClientProfileUpdateFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractClientProfileUpdateFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild2GenerateHCRFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputText',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputText',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild3GenerateHCRFileNonClientRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSProcessingOutputFile',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSProcessingOutputFile',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild3GenerateHCRFileNonClientRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractOrganizationContactInfoFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractOrganizationContactInfoFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild3GenerateHCRFileNonClientRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractOrganizationProfileFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractOrganizationProfileFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadHCRSDataChild3GenerateHCRFileNonClientRecords' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.ExtractFile',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.ExtractFile',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.CIHIError',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.CIHIError',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('"Staging"."HCRSCIHIErrorAction"',1) 
	AND obj.ObjectSchemaName = PARSENAME('"Staging"."HCRSCIHIErrorAction"',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HCRSCIHISubmissionError',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HCRSCIHISubmissionError',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('CIHI.SubmissionErrorFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('CIHI.SubmissionErrorFact',2) 
	AND db.DatabaseName = 'HCRSMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.PendingAdmissionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.PendingAdmissionFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.PendingClientProfileUpdateFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.PendingClientProfileUpdateFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.PendingDischargeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.PendingDischargeFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.PendingRaiHCAssessmentFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.PendingRaiHCAssessmentFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.PendingRaiHCMedicationFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.PendingRaiHCMedicationFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.PendingServiceDetailFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.PendingServiceDetailFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.PendingServiceEndFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.PendingServiceEndFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HCRS.PendingServiceStartFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HCRS.PendingServiceStartFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('CIHI.AdmissionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('CIHI.AdmissionFact',2) 
	AND db.DatabaseName = 'HCRSMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('CIHI.OrganizationContactInfoFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('CIHI.OrganizationContactInfoFact',2) 
	AND db.DatabaseName = 'HCRSMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('CIHI.DischargeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('CIHI.DischargeFact',2) 
	AND db.DatabaseName = 'HCRSMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('CIHI.ERVisitFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('CIHI.ERVisitFact',2) 
	AND db.DatabaseName = 'HCRSMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('CIHI.RaiHCMedicationFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('CIHI.RaiHCMedicationFact',2) 
	AND db.DatabaseName = 'HCRSMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('CIHI.OrganizationProfileFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('CIHI.OrganizationProfileFact',2) 
	AND db.DatabaseName = 'HCRSMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('CIHI.RaiHCAssessmentFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('CIHI.RaiHCAssessmentFact',2) 
	AND db.DatabaseName = 'HCRSMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('CIHI.ServiceDetailFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('CIHI.ServiceDetailFact',2) 
	AND db.DatabaseName = 'HCRSMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('CIHI.ServiceEndFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('CIHI.ServiceEndFact',2) 
	AND db.DatabaseName = 'HCRSMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('CIHI.ServiceStartFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('CIHI.ServiceStartFact',2) 
	AND db.DatabaseName = 'HCRSMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ReconcileHcrsDetailedSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('CIHI.ClientProfileUpdateFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('CIHI.ClientProfileUpdateFact',2) 
	AND db.DatabaseName = 'HCRSMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.AddressFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.AddressFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.AssessmentContactFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.AssessmentContactFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.AssessmentHeaderFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.AssessmentHeaderFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.CaseNoteContactFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.CaseNoteContactFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.CaseNoteHeaderFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.CaseNoteHeaderFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.ClientGPFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.ClientGPFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.CubeSource',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.CubeSource',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.CurrentLocationFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.CurrentLocationFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.DischargeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.DischargeFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.HCRSAssessmentFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.HCRSAssessmentFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.HCRSRAIHighRiskSummary',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.HCRSRAIHighRiskSummary',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.HomeSupportActivityFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.HomeSupportActivityFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.HoNOSFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.HoNOSFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.ImmAdverseEventFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.ImmAdverseEventFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.ImmunizationAlertFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.ImmunizationAlertFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.ImmunizationHistoryFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.ImmunizationHistoryFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.InterventionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.InterventionFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.InvolvedProfessionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.InvolvedProfessionFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.Measure',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.Measure',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.PersonFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.PersonFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.PersonNameFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.PersonNameFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.ReferralFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.ReferralFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.SchoolHistoryFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.SchoolHistoryFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.ScreeningResultFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.ScreeningResultFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.SLPActivityFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.SLPActivityFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.SubstanceUseFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.SubstanceUseFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.sysdiagrams',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.sysdiagrams',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.WaitlistClientOfferFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.WaitlistClientOfferFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.WaitlistDefinitionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.WaitlistDefinitionFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.WaitlistEntryFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.WaitlistEntryFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.WaitlistProviderOfferFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.WaitlistProviderOfferFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.WaitTimeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.WaitTimeFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.WeightGrowthFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.WeightGrowthFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.YouthClinicActivityFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.YouthClinicActivityFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.AddressType',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.AddressType',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.Age',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.Age',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.AgeMonth',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.AgeMonth',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.Antigen',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.Antigen',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.AssessmentReason',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.AssessmentReason',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.AssessmentType',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.AssessmentType',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.CaseNoteReason',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.CaseNoteReason',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.CaseNoteType',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.CaseNoteType',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ClientCategory',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ClientCategory',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ClientCategorySubGroup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ClientCategorySubGroup',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ClientDeRegisterReason',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ClientDeRegisterReason',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ClientGroup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ClientGroup',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ClientRegisterCategory',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ClientRegisterCategory',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ClientRegisterType',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ClientRegisterType',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.CommunityLHA',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.CommunityLHA',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.CommunityLkup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.CommunityLkup',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.CommunityLocation',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.CommunityLocation',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.CommunityOrganization',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.CommunityOrganization',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.CommunityProgram',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.CommunityProgram',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.CommunityRegion',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.CommunityRegion',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.CommunityServiceLocation',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.CommunityServiceLocation',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ContactSetting',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ContactSetting',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ContactType',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ContactType',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.CriminalJusticeInvolvement',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.CriminalJusticeInvolvement',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.CriminalJusticeType',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.CriminalJusticeType',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.Date',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.Date',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.DeathLocation',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.DeathLocation',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.DeathNotify',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.DeathNotify',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.DischargeDisposition',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.DischargeDisposition',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.DischargeDispositionLookupCOM',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.DischargeDispositionLookupCOM',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.DischargeOutcome',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.DischargeOutcome',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.Dx',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.Dx',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.DxCodingSignificance',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.DxCodingSignificance',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.DxState',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.DxState',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.EducationLevel',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.EducationLevel',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.EducationLevelLookup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.EducationLevelLookup',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.Ethnicity',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.Ethnicity',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.Facility',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.Facility',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.GAFScore',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.GAFScore',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.GAFScoreGroup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.GAFScoreGroup',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.Gender',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.Gender',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.HealthAuthority',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.HealthAuthority',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.HomeSupportCluster',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.HomeSupportCluster',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.HoNOS',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.HoNOS',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.HouseholdComposition',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.HouseholdComposition',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.HouseType',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.HouseType',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.HSDA',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.HSDA',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ImmAlert',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ImmAlert',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ImmCategory',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ImmCategory',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.Intervention',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.Intervention',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.InterventionType',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.InterventionType',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.LegalStatus',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.LegalStatus',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.LevelOfCare',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.LevelOfCare',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.LHA',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.LHA',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.LocalReportingOffice',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.LocalReportingOffice',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.LocationCategory',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.LocationCategory',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.LocationType',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.LocationType',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.MaritalStatus',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.MaritalStatus',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.MethodOfSubstanceIntake',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.MethodOfSubstanceIntake',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.PostalCode',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.PostalCode',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.Provider',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.Provider',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.Province',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.Province',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.RaiAnswerValue',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.RaiAnswerValue',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ReasonEndingService',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ReasonEndingService',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ReasonEndingServiceLookup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ReasonEndingServiceLookup',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ReferralPriority',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ReferralPriority',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ReferralReason',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ReferralReason',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ReferralSource',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ReferralSource',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ReferralSourceLookup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ReferralSourceLookup',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ReferralType',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ReferralType',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.School',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.School',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ScreeningEvent',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ScreeningEvent',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ScreeningEventResult',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ScreeningEventResult',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ServiceProviderCategoryLookup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ServiceProviderCategoryLookup',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ServiceType',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ServiceType',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.SharingNeedles',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.SharingNeedles',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.StageOfChange',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.StageOfChange',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.SubstancePattern',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.SubstancePattern',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.SubstanceUse',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.SubstanceUse',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.SubstanceUseLookup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.SubstanceUseLookup',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.SuicideAttempt',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.SuicideAttempt',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.Time',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.Time',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ViolenceAbuse',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ViolenceAbuse',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.WaitlistClientOfferStatus',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.WaitlistClientOfferStatus',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.WaitlistOfferOutcome',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.WaitlistOfferOutcome',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.WaitlistPriority',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.WaitlistPriority',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.WaitlistProviderOfferStatus',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.WaitlistProviderOfferStatus',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.WaitListReason',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.WaitListReason',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.WaitlistReasonNotOffered',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.WaitlistReasonNotOffered',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.WaitlistReasonOfferRemoved',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.WaitlistReasonOfferRemoved',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.WaitlistReasonRejected',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.WaitlistReasonRejected',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.WaitlistStatus',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.WaitlistStatus',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.WaitlistType',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.WaitlistType',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CommunityMart SPDBDECSUP04-STDBDECSUP01' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.WaitTimeMeasure',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.WaitTimeMeasure',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.HoNOSFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.HoNOSFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.HoNOSFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.HoNOSFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.HoNOSFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.HoNOSFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.AddressFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.AddressFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.AssessmentHeaderFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.AssessmentHeaderFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.CaseNoteContactFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.CaseNoteContactFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.CaseNoteHeaderFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.CaseNoteHeaderFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.InterventionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.InterventionFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.InvolvedProfessionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.InvolvedProfessionFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.PersonFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.PersonFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.ReferralFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.ReferralFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.SubstanceUseFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.SubstanceUseFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.WaitlistClientOfferFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.WaitlistClientOfferFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.WaitlistDefinitionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.WaitlistDefinitionFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.WaitlistEntryFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.WaitlistEntryFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.WaitlistProviderOfferFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.WaitlistProviderOfferFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.DiagnosisFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.DiagnosisFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateCommunityMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.DiagnosisGAFFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.DiagnosisGAFFact',2) 
	AND db.DatabaseName = 'CommunityMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CPWC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('CPWC.RawDataFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('CPWC.RawDataFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CPWC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('CPWC.MOHProvider',1) 
	AND obj.ObjectSchemaName = PARSENAME('CPWC.MOHProvider',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CPWC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CPWCMartGenerateByHAFiscalPeriod',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CPWCMartGenerateByHAFiscalPeriod',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CPWC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CPWCMOHSourceData',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CPWCMOHSourceData',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'GenerateCPWCTablesinFinanceMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CPWCMartGenerateByHAFiscalPeriod',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CPWCMartGenerateByHAFiscalPeriod',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PerfCountersUpload' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('snapshots.performance_counter_values',1) 
	AND obj.ObjectSchemaName = PARSENAME('snapshots.performance_counter_values',2) 
	AND db.DatabaseName = 'MDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PerfCountersUpload' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('snapshots.performance_counter_instances',1) 
	AND obj.ObjectSchemaName = PARSENAME('snapshots.performance_counter_instances',2) 
	AND db.DatabaseName = 'MDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'QueryActivityUpload' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('snapshots.active_sessions_and_requests',1) 
	AND obj.ObjectSchemaName = PARSENAME('snapshots.active_sessions_and_requests',2) 
	AND db.DatabaseName = 'MDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'QueryActivityUpload' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('snapshots.query_stats',1) 
	AND obj.ObjectSchemaName = PARSENAME('snapshots.query_stats',2) 
	AND db.DatabaseName = 'MDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'QueryActivityUpload' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('snapshots.notable_query_plan',1) 
	AND obj.ObjectSchemaName = PARSENAME('snapshots.notable_query_plan',2) 
	AND db.DatabaseName = 'MDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'QueryActivityUpload' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('snapshots.notable_query_text',1) 
	AND obj.ObjectSchemaName = PARSENAME('snapshots.notable_query_text',2) 
	AND db.DatabaseName = 'MDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'SqlTraceUpload' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('snapshots.trace_data',1) 
	AND obj.ObjectSchemaName = PARSENAME('snapshots.trace_data',2) 
	AND db.DatabaseName = 'myMDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ProfileData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('DataProfile.ColumnStatistics',1) 
	AND obj.ObjectSchemaName = PARSENAME('DataProfile.ColumnStatistics',2) 
	AND db.DatabaseName = 'DQMF' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ProfileData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('DataProfile.ColumnNullRatios',1) 
	AND obj.ObjectSchemaName = PARSENAME('DataProfile.ColumnNullRatios',2) 
	AND db.DatabaseName = 'DQMF' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ProfileData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('DataProfile.ValueDistribution',1) 
	AND obj.ObjectSchemaName = PARSENAME('DataProfile.ValueDistribution',2) 
	AND db.DatabaseName = 'DQMF' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSForXMLFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSExtract',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSExtract',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSForXMLFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CCRSControlRecord',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CCRSControlRecord',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'CCRSForXMLFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('CCRS.ExtractFile',1) 
	AND obj.ObjectSchemaName = PARSENAME('CCRS.ExtractFile',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_AAP_CARESTREAM',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_AAP_CARESTREAM',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_AAP_CARESTREAM2',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_AAP_CARESTREAM2',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_AAP_POS',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_AAP_POS',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_PREFERRED_SURGEON',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_PREFERRED_SURGEON',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_PROCCODE',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_PROCCODE',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_REFERRAL_JOINTS',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_REFERRAL_JOINTS',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_REFERRING_PROVIDER',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_REFERRING_PROVIDER',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_SOA_POS',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_SOA_POS',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_SPI_CONSULTSURGEON',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_SPI_CONSULTSURGEON',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_SPI_FACILITY',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_SPI_FACILITY',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_SPI_OASISREFERDATE',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_SPI_OASISREFERDATE',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_SPI_PATSTATUS',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_SPI_PATSTATUS',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_SPI_POSTPONED_REASON',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_SPI_POSTPONED_REASON',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_SPI_PREHABREASON',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_SPI_PREHABREASON',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_SPI_PREOPREASON',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_SPI_PREOPREASON',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_SPI_PROCCODE',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_SPI_PROCCODE',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_SPI_PROCSURGEON',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_SPI_PROCSURGEON',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_SPI_REASON_NOT_REQ',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_SPI_REASON_NOT_REQ',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_SPI_REASON_REMOVED',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_SPI_REASON_REMOVED',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_SPI_WAITLISTDATE',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_SPI_WAITLISTDATE',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_SPI_WILLATTENDPREHAB',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_SPI_WILLATTENDPREHAB',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_CDO_SPI_WILLATTENDPREOP',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_CDO_SPI_WILLATTENDPREOP',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_PROVIDER',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_PROVIDER',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_SHORTCODE',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_SHORTCODE',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.D_USERCREATEDBY',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.D_USERCREATEDBY',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.F_AAP',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.F_AAP',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.F_APPTS',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.F_APPTS',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.F_IOF',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.F_IOF',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.F_REFIN',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.F_REFIN',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.F_REFOUT',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.F_REFOUT',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.F_REFOUT2',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.F_REFOUT2',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.F_SCAN_DOCS',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.F_SCAN_DOCS',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.F_SPI',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.F_SPI',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.F_TAF_SOA',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.F_TAF_SOA',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.F_TASK_USER',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.F_TASK_USER',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ImportOASISData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.F_TASKS',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.F_TASKS',2) 
	AND db.DatabaseName = 'OASISMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundProfile',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundProfile',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundLimbAx',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundLimbAx',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxDrain',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxDrain',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundLoadingBatch',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundLoadingBatch',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxIncision',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxIncision',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxOstomy',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxOstomy',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxProduct',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxProduct',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAx',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAx',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundCareType',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundCareType',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Wound',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Wound',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxDressCare',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxDressCare',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxEachWound',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxEachWound',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxNurseCarePlan',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxNurseCarePlan',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundListData',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundListData',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundPatient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundPatient',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundPatientProfile',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundPatientProfile',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundProduct',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundProduct',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundProfileGoal',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundProfileGoal',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxEachWoundEdge',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxEachWoundEdge',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundProfileEtiology',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundProfileEtiology',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundProfileOstomyType',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundProfileOstomyType',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxEachWoundExudate',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxEachWoundExudate',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxEachWoundUnderminingFactor',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxEachWoundUnderminingFactor',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxEachWoundSurroundSkin',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxEachWoundSurroundSkin',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxDrainSurroundSkin',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxDrainSurroundSkin',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxEachWoundSinusTractFactor',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxEachWoundSinusTractFactor',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundPatientProfileComorbidity',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundPatientProfileComorbidity',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxDetailAntibiotic',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxDetailAntibiotic',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxDetailAntibiotic',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxDetailAntibiotic',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxDetailAntibiotic',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxDetailAntibiotic',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxDetailAntibiotic',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxDetailAntibiotic',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxDetailCSResult',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxDetailCSResult',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxDetailCSResult',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxDetailCSResult',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxDetailCSResult',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxDetailCSResult',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxDetailCSResult',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxDetailCSResult',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxEachWoundBedPercentage',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxEachWoundBedPercentage',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxIncisionExudate',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxIncisionExudate',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxIncisionSurroundSkin',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxIncisionSurroundSkin',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundAxOstomySurroundSkin',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundAxOstomySurroundSkin',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.WoundPatientProfileInterferingFactor',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.WoundPatientProfileInterferingFactor',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailAntibioticFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailAntibioticFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailAntibioticFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailAntibioticFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailAntibioticFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailAntibioticFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailAntibioticFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailAntibioticFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailCSResultFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailCSResultFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailCSResultFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailCSResultFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailCSResultFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailCSResultFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailCSResultFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailCSResultFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailExudateFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailExudateFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailExudateFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailExudateFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailProductFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailProductFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailProductFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailProductFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailProductFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailProductFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailProductFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailProductFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailSinusTractFactorFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailSinusTractFactorFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailSurroundSkinFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailSurroundSkinFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailSurroundSkinFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailSurroundSkinFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailSurroundSkinFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailSurroundSkinFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailSurroundSkinFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailSurroundSkinFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailUnderminingFactorFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailUnderminingFactorFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailWoundBedPercentageFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailWoundBedPercentageFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDetailWoundEdgeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDetailWoundEdgeFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxDressCareFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxDressCareFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.AxNurseCarePlanFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.AxNurseCarePlanFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.CareTypeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.CareTypeFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.LimbAxFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.LimbAxFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.PatientFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.PatientFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.PatientProfileComorbidityFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.PatientProfileComorbidityFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.PatientProfileFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.PatientProfileFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.PatientProfileInterferingFactorFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.PatientProfileInterferingFactorFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.ProfileEtiologyFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.ProfileEtiologyFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.ProfileFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.ProfileFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.ProfileGoalFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.ProfileGoalFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.ProfileOstomyTypeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.ProfileOstomyTypeFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadWound' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Wound.WoundFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Wound.WoundFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Import_exel' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.Data Warehouse',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.Data Warehouse',2) 
	AND db.DatabaseName = 'DIMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadDIData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('DI.ExamBillingCodeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('DI.ExamBillingCodeFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadDIData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('DI.ExamRequestFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('DI.ExamRequestFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadDIData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DIExamBillingCode',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DIExamBillingCode',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadDIData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DIExamBillingCode',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DIExamBillingCode',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadDIData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DIExamRequest',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DIExamRequest',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadDIData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('DI.ExamBillingCodeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('DI.ExamBillingCodeFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadDIData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('DI.ExamRequestFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('DI.ExamRequestFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadDIData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DIExamBillingCode',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DIExamBillingCode',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadDIData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DIExamRequestGE',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DIExamRequestGE',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoDailyActualOccupancyAndStaffingLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.ActualHourlyOccupancy',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.ActualHourlyOccupancy',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoDailyActualOccupancyAndStaffingLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.ActualHourlyStaffing',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.ActualHourlyStaffing',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoDailyActualOccupancyAndStaffingPHC_VH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.ActualHourlyOccupancy',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.ActualHourlyOccupancy',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoDailyActualOccupancyAndStaffingPHC_VH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.ActualHourlyStaffing',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.ActualHourlyStaffing',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoDailyActualOccupancyAndStaffingRHS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.ActualHourlyOccupancy',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.ActualHourlyOccupancy',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoDailyActualOccupancyAndStaffingRHS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.ActualHourlyStaffing',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.ActualHourlyStaffing',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoDailyStaffing' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ESPBatchStaffing',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ESPBatchStaffing',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoDailyStaffing' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.LoadError',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.LoadError',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoDailyStaffingPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ESPBatchStaffing',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ESPBatchStaffing',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORAllocationPlanLGH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORAllocationPlanLGH',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreScheduleShiftsLGH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreScheduleShiftsLGH',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreShiftsLGH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreShiftsLGH',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanMSJ' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreScheduleShiftsPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreScheduleShiftsPHC',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanMSJ' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreShiftsPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreShiftsPHC',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanMSJ' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORAllocationPlanPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORAllocationPlanPHC',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanRHS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreScheduleShiftsRHS',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreScheduleShiftsRHS',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanRHS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreShiftsRHS',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreShiftsRHS',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanRHS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORAllocationPlanRHS',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORAllocationPlanRHS',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanSPH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreScheduleShiftsPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreScheduleShiftsPHC',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanSPH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreShiftsPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreShiftsPHC',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanSPH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORAllocationPlanPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORAllocationPlanPHC',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanUBC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORAllocationPlanUBC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORAllocationPlanUBC',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanUBC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreScheduleShiftsUBC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreScheduleShiftsUBC',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanUBC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreShiftsUBC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreShiftsUBC',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanVGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORAllocationPlanVGH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORAllocationPlanVGH',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanVGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreScheduleShiftsVGH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreScheduleShiftsVGH',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORAllocPlanVGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreShiftsVGH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreShiftsVGH',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompletedCasesLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CapPlanPatientLGH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CapPlanPatientLGH',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompletedCasesLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreHCPBackup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreHCPBackup',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompletedCasesLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CapPlanPatient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CapPlanPatient',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompletedCasesLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HealthcareProfessionalLGH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HealthcareProfessionalLGH',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompletedCasesLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreEncounterLGH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreEncounterLGH',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompletedCasesRHS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CapPlanPatientRHS',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CapPlanPatientRHS',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompletedCasesRHS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreHCPBackup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreHCPBackup',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompletedCasesRHS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CapPlanPatient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CapPlanPatient',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompletedCasesRHS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HealthcareProfessionalRHS',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HealthcareProfessionalRHS',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompletedCasesRHS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreEncounterRHS',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreEncounterRHS',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompletedCasesVGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CapPlanPatientVGH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CapPlanPatientVGH',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompletedCasesVGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreHCPBackup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreHCPBackup',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompletedCasesVGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CapPlanPatient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CapPlanPatient',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompletedCasesVGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HealthcareProfessionalVGH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HealthcareProfessionalVGH',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompletedCasesVGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreEncounterVGH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreEncounterVGH',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompWLPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CapPlanPatientPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CapPlanPatientPHC',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompWLPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreHCPBackup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreHCPBackup',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompWLPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORWaitListPHCWeekly',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORWaitListPHCWeekly',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompWLPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HealthcareProfessionalPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HealthcareProfessionalPHC',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompWLPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HealthcareProfessionalPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HealthcareProfessionalPHC',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompWLPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CapPlanPatient',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CapPlanPatient',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompWLPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HealthcareProfessionalPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HealthcareProfessionalPHC',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORCompWLPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreEncounterPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreEncounterPHC',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORScheduledCasesLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreHCPBackup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreHCPBackup',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORScheduledCasesRHS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreHCPBackup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreHCPBackup',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORScheduledCasesVGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.LoadError',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.LoadError',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORScheduledCasesVGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORScheduledCasesVGH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORScheduledCasesVGH',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORScheduledCasesVGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreHCPBackup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreHCPBackup',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORWaitlistCasesLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORWaitListLGH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORWaitListLGH',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORWaitlistCasesLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreHCPBackup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreHCPBackup',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORWaitlistCasesLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CapPlanPatientLGH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CapPlanPatientLGH',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORWaitlistCasesRHS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORWaitListRHS',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORWaitListRHS',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORWaitlistCasesRHS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreHCPBackup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreHCPBackup',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORWaitlistCasesRHS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CapPlanPatientRHS',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CapPlanPatientRHS',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORWaitlistCasesRHS' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORWaitListVCHWeekly',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORWaitListVCHWeekly',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORWaitlistCasesVGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORWaitListVGH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORWaitListVGH',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORWaitlistCasesVGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.TheatreHCPBackup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.TheatreHCPBackup',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoORWaitlistCasesVGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CapPlanPatientVGH',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CapPlanPatientVGH',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoPHCWeeklyCensusLoad' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PHCCensusWeekly',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PHCCensusWeekly',2) 
	AND db.DatabaseName = 'EMENDO' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmendoStaffEvaluation' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.StaffingMetricsByPlanningGroupReport',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.StaffingMetricsByPlanningGroupReport',2) 
	AND db.DatabaseName = 'Emendo' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmergencyPCISLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PCISDemo',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PCISDemo',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmergencyPCISLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PCISDemo',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PCISDemo',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmergencyPCISLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PCISVisit',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PCISVisit',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmergencyPCISLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PCISVisit',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PCISVisit',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmergencyPCISLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PCISVisit',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PCISVisit',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmergencyPCISLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PCISLoc',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PCISLoc',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmergencyPCISLoadVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.EmergencyPCIS',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.EmergencyPCIS',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmergencyPCISParent' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ED.CurrentVisitArea',1) 
	AND obj.ObjectSchemaName = PARSENAME('ED.CurrentVisitArea',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmergencyPCISParent' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ED.CurrentVisitFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ED.CurrentVisitFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmergencyPCISParent' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.EmergencyPCIS_Area',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.EmergencyPCIS_Area',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmergencyPCISTransformArea' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.VisitArea',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.VisitArea',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'EmergencyPCISTransformVisit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.EmergencyPCIS_Intermediate2',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.EmergencyPCIS_Intermediate2',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateED_Visit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.EDVisitArea',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.EDVisitArea',2) 
	AND db.DatabaseName = 'EDMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateED_Visit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.ED_Visit',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.ED_Visit',2) 
	AND db.DatabaseName = 'EDMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateED_Visit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.EDMart_EDVisitArea',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.EDMart_EDVisitArea',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateED_Visit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ED_Visit_IDs',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ED_Visit_IDs',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateED_Visit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ED_Visit',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ED_Visit',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateED_Visit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ED_Visit_Include_ETLAuditID',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ED_Visit_Include_ETLAuditID',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateED_Visit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ED_Visit_Include_PatientID',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ED_Visit_Include_PatientID',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGLoadTB' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LGTB',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LGTB',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGLoadTB' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LGTB',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LGTB',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGLoadTB' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LGTB',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LGTB',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGLoadTB' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LG_TB_dups',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LG_TB_dups',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGLoadTB' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LG_TB_load',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LG_TB_load',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGLoadTBArea' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LGTBA',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LGTBA',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGLoadTBArea' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LG_TBA_load',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LG_TBA_load',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGMain' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LGStar',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LGStar',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGMain' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LGStar',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LGStar',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGMain' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LGStar',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LGStar',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGMain' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LG_Star_Load',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LG_Star_Load',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGMain' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ED.CurrentVisitArea',1) 
	AND obj.ObjectSchemaName = PARSENAME('ED.CurrentVisitArea',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGMain' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ED.CurrentVisitFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ED.CurrentVisitFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGMerge' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LG_Merged',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LG_Merged',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGMerge' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LG_Merged',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LG_Merged',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGMergeTBA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LG_TBA_Merged',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LG_TBA_Merged',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGMergeTBA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LG_TBA_Merged',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LG_TBA_Merged',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGTransformAll' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LG_VisitFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LG_VisitFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LGTransformTBAAll' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LG_VisitArea',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LG_VisitArea',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCCancelledVisits',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCCancelledVisits',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCFamilyMD',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCFamilyMD',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCAdmission',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCAdmission',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCDischarge',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCDischarge',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCEDDischargeDiagnosis',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCEDDischargeDiagnosis',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCFlatColumn_EDOrderRequest',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCFlatColumn_EDOrderRequest',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCEDPatientCTASForDownTime',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCEDPatientCTASForDownTime',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCEDTimeSeen',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCEDTimeSeen',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCFlatColumn_EDTriageExtract',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCFlatColumn_EDTriageExtract',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCPatientID',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCPatientID',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCMergedMRN',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCMergedMRN',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCPatientAlert',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCPatientAlert',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCInsurancePlan_PatientLevel',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCInsurancePlan_PatientLevel',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCInsurancePlan_VisitLevel',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCInsurancePlan_VisitLevel',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCPatientActivityAddress',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCPatientActivityAddress',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCPatientActivityAdmittingProvider',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCPatientActivityAdmittingProvider',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCPatientActivityAttendingProvider',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCPatientActivityAttendingProvider',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCPatientActivityGender',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCPatientActivityGender',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCPatientActivityService',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCPatientActivityService',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCPatientActivityVisitType',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCPatientActivityVisitType',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCLoadData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCE_PHCPatientActivityLocation',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCE_PHCPatientActivityLocation',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformADTC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.AdmissionFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.AdmissionFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformADTC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Secure.CurrentPatientExtendedAttribute',1) 
	AND obj.ObjectSchemaName = PARSENAME('Secure.CurrentPatientExtendedAttribute',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformADTC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Secure.CurrentPatientExtendedAttribute',1) 
	AND obj.ObjectSchemaName = PARSENAME('Secure.CurrentPatientExtendedAttribute',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformADTC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Secure.CurrentPatientExtendedAttribute',1) 
	AND obj.ObjectSchemaName = PARSENAME('Secure.CurrentPatientExtendedAttribute',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformADTC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Secure.CurrentPatientExtendedAttribute',1) 
	AND obj.ObjectSchemaName = PARSENAME('Secure.CurrentPatientExtendedAttribute',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformADTC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Secure.CurrentPatientExtendedAttribute',1) 
	AND obj.ObjectSchemaName = PARSENAME('Secure.CurrentPatientExtendedAttribute',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformADTC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Secure.CurrentPatientExtendedAttribute',1) 
	AND obj.ObjectSchemaName = PARSENAME('Secure.CurrentPatientExtendedAttribute',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformADTC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.DischargeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.DischargeFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformADTC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.TransferFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.TransferFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformADTC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Secure.PatientAlertFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Secure.PatientAlertFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformADTC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.ActivityFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.ActivityFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformADTC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.ActivityFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.ActivityFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformADTC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Adtc.ActivityFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Adtc.ActivityFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformED' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ED.CurrentLocationTransferFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ED.CurrentLocationTransferFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformED' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Secure.CurrentPatientExtendedAttribute',1) 
	AND obj.ObjectSchemaName = PARSENAME('Secure.CurrentPatientExtendedAttribute',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformED' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Secure.CurrentPatientExtendedAttribute',1) 
	AND obj.ObjectSchemaName = PARSENAME('Secure.CurrentPatientExtendedAttribute',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformED' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Secure.CurrentPatientExtendedAttribute',1) 
	AND obj.ObjectSchemaName = PARSENAME('Secure.CurrentPatientExtendedAttribute',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformED' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ED.CurrentVisitFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ED.CurrentVisitFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCTransformED' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ED.CurrentVisitArea',1) 
	AND obj.ObjectSchemaName = PARSENAME('ED.CurrentVisitArea',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ED Visit PRGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PRGHLoad',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PRGHLoad',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ED Visit PRGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.EDVisitPRGHLoad',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.EDVisitPRGHLoad',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ED Visit PRGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ED.CurrentVisitFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ED.CurrentVisitFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ED Visit PRGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.EDVisitPRGHFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.EDVisitPRGHFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'STS_ED_Main' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.STSStar',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.STSStar',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'STS_ED_Main' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.STSStarLoad',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.STSStarLoad',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'STS_ED_Main' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ED.CurrentVisitFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ED.CurrentVisitFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'STS_ED_Main' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.STS_VisitFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.STS_VisitFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NACRSAmbulatoryFile2013' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('NACRS.ExtractArchiveError',1) 
	AND obj.ObjectSchemaName = PARSENAME('NACRS.ExtractArchiveError',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NACRSAmbulatoryFile2013' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('NACRS.ExtractFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('NACRS.ExtractFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NACRSAmbulatoryFile2013' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.NACRSExtractFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.NACRSExtractFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NACRSAmbulatoryFile2013' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.NACRSACExtractFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.NACRSACExtractFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NACRSAmbulatoryFile2013' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.NACRSACExtractFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.NACRSACExtractFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NACRSAmbulatoryFile2013' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.NACRSACExtractFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.NACRSACExtractFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NACRSAmbulatoryFile2013' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.NACRSACExtractFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.NACRSACExtractFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NACRSFacilityFile' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.NACRSFIFExtract',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.NACRSFIFExtract',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NACRSLoadED' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('NACRS.PendingFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('NACRS.PendingFact',2) 
	AND db.DatabaseName = '' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NACRSLoadED' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.NACRSPendingFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.NACRSPendingFact',2) 
	AND db.DatabaseName = '' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NACRSProcessErrors' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.NACRSBlankFileName',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.NACRSBlankFileName',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NACRSProcessErrors' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.NACRSDQError',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.NACRSDQError',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NACRSProcessErrors' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('NACRS.ExtractDQError',1) 
	AND obj.ObjectSchemaName = PARSENAME('NACRS.ExtractDQError',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NACRSProcessErrors' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.NACRSError',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.NACRSError',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NACRSProcessErrors' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('NACRS.ExtractError',1) 
	AND obj.ObjectSchemaName = PARSENAME('NACRS.ExtractError',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ExportCCDim' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.FinCCHierarchy',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.FinCCHierarchy',2) 
	AND db.DatabaseName = 'FinanceMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ExportCCDim' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ProgramCC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ProgramCC',2) 
	AND db.DatabaseName = 'FinanceMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_Dim_EarningCode' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.EarningCode',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.EarningCode',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_Dim_Employee' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Employee',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Employee',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_Dim_JobCode' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.JobCode',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.JobCode',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_Finance_BiWeekly_FiscalPeriod_LabourHrCstFact_Regional' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LabourPHCRegional',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LabourPHCRegional',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_Finance_LabourBudgetFact_PHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LabourBudgetFactPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LabourBudgetFactPHC',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_Historical_NonLabourFact_PHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PHC_Non_Labour_Fact_History',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PHC_Non_Labour_Fact_History',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_Historical_NonLabourFact_PHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.NonLabourFact_PHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.NonLabourFact_PHC',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_LabourFact_PHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LabourPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LabourPHC',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_NonLabourFact_PHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PHC_NonLabour',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PHC_NonLabour',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_NonLabourFact_PHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.NonLabourFact_PHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.NonLabourFact_PHC',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_Staging_GLSummary_PHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.GLSummaryPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.GLSummaryPHC',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_Staging_GLSummary_PHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.GLSummaryPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.GLSummaryPHC',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_Tree_CSVFiles' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PSChsPrimaryNodePHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PSChsPrimaryNodePHC',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_Tree_CSVFiles' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PSChsPrimaryTreePHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PSChsPrimaryTreePHC',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_Tree_CSVFiles' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PSActiveAccountPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PSActiveAccountPHC',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_Tree_CSVFiles' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PSActiveCostCentrePHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PSActiveCostCentrePHC',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_Tree_CSVFiles' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PSDefinitionPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PSDefinitionPHC',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_Tree_CSVFiles' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PSTreeLeafPHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PSTreeLeafPHC',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_Tree_CSVFiles' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PSTreePHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PSTreePHC',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'Load_Tree_CSVFiles' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PSTreeNodePHC',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PSTreeNodePHC',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.DateDim',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.DateDim',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.FiscalPeriodDim',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.FiscalPeriodDim',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.LabourDistributionDim',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.LabourDistributionDim',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.PayPeriodDim',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.PayPeriodDim',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.LabourBudgetFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.LabourBudgetFact',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.GLSummaryFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.GLSummaryFact',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.LabourFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.LabourFact',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.BudgetLDRSummary',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.BudgetLDRSummary',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('KPI.ED_Visits',1) 
	AND obj.ObjectSchemaName = PARSENAME('KPI.ED_Visits',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('KPI.Productivity',1) 
	AND obj.ObjectSchemaName = PARSENAME('KPI.Productivity',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.CurrentPeriod',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.CurrentPeriod',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit_LabourFact' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.DateDim',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.DateDim',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit_LabourFact' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.FiscalPeriodDim',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.FiscalPeriodDim',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit_LabourFact' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.LabourDistributionDim',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.LabourDistributionDim',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit_LabourFact' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.PayPeriodDim',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.PayPeriodDim',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit_LabourFact' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.LabourBudgetFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.LabourBudgetFact',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit_LabourFact' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.LabourFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.LabourFact',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit_LabourFact' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.GLSummaryFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.GLSummaryFact',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit_LabourFact' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.BudgetLDRSummary',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.BudgetLDRSummary',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit_LabourFact' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('KPI.ED_Visits',1) 
	AND obj.ObjectSchemaName = PARSENAME('KPI.ED_Visits',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit_LabourFact' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('KPI.Productivity',1) 
	AND obj.ObjectSchemaName = PARSENAME('KPI.Productivity',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PublishAudit_LabourFact' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.CurrentPeriod',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.CurrentPeriod',2) 
	AND db.DatabaseName = 'FinanceReportsAudit' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ADTCData' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ADTCSummaryData',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ADTCSummaryData',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'DeptRollUp' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DeptRollUpValidation',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DeptRollUpValidation',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'DeptRollUp' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DeptRollupNew',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DeptRollupNew',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'FinanceLDRImport' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LDR',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LDR',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'FinanceLDRImport' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LDRValidation',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LDRValidation',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'FinanceLDRImport' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.LDR',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.LDR',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'GLStatisticHierarchy' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.GLStatisticHierarchy',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.GLStatisticHierarchy',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadBiWeeklyLabourHrCstFact' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.BiWeeklyValidation',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.BiWeeklyValidation',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadBiWeeklyLabourHrCstFact' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PhPyHrsProductiveHours',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PhPyHrsProductiveHours',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadBiWeeklyLabourHrCstFact' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.BiWeeklyLabourHrCstFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.BiWeeklyLabourHrCstFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadBiWeeklyLabourHrCstFact' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.BiWeeklyLabourHrCstFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.BiWeeklyLabourHrCstFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadCIHIMISGrouping' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CIHICostCenterMISGrouping',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CIHICostCenterMISGrouping',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadCIHIMISGrouping' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CIHIMISGrouping',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CIHIMISGrouping',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadGLHierarchy' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.GLHierarchy',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.GLHierarchy',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadLDBudget' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LDBudget',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LDBudget',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadLDBudget' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.YearlyBudgetFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.YearlyBudgetFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadLDBudget' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.YearlyBudgetFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.YearlyBudgetFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NewFinanceLedger' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.GLAccountFact_Actual',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.GLAccountFact_Actual',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NewFinanceLedger' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.GLAccountFact_Budget',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.GLAccountFact_Budget',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NewFinanceLedger' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.GLAccountStatFact_Budget',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.GLAccountStatFact_Budget',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NewFinanceLedger' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.GLAccountStatFact_Actual',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.GLAccountStatFact_Actual',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NewFinanceLedger' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.Ledger',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.Ledger',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NewFinanceLedger' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LedgerValidation',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LedgerValidation',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NewFinanceLedger' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.Ledger',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.Ledger',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NewInsufficientNotice' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.InsufficientNoticeNew',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.InsufficientNoticeNew',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NewInsufficientNotice' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.InsufficientNoticeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.InsufficientNoticeFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'NewInsufficientNotice' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.InsufficientNoticeValidation',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.InsufficientNoticeValidation',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateFinanceMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.BiWeeklyLabourHrCstFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.BiWeeklyLabourHrCstFact',2) 
	AND db.DatabaseName = 'FinanceMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateFinanceMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.CasualHrsFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.CasualHrsFact',2) 
	AND db.DatabaseName = 'FinanceMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateFinanceMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.EntityProgramSubProgram',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.EntityProgramSubProgram',2) 
	AND db.DatabaseName = 'FinanceMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateFinanceMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.YearlyBudgetFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.YearlyBudgetFact',2) 
	AND db.DatabaseName = 'FinanceMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateFinanceMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.FTPTCFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.FTPTCFact',2) 
	AND db.DatabaseName = 'FinanceMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateFinanceMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.InsufficientNoticeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.InsufficientNoticeFact',2) 
	AND db.DatabaseName = 'FinanceMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateFinanceMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.LDProductiveHourByJobCategory',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.LDProductiveHourByJobCategory',2) 
	AND db.DatabaseName = 'FinanceMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateFinanceMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.FiscalPeriodLabourHrCstFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.FiscalPeriodLabourHrCstFact',2) 
	AND db.DatabaseName = 'FinanceMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateFinanceMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.FiscalPeriodLabourHrCstFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.FiscalPeriodLabourHrCstFact',2) 
	AND db.DatabaseName = 'FinanceMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateFinanceMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.GLAccountFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.GLAccountFact',2) 
	AND db.DatabaseName = 'FinanceMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateFinanceMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.GLAccountFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.GLAccountFact',2) 
	AND db.DatabaseName = 'FinanceMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateFinanceMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.GLAccountStatsFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.GLAccountStatsFact',2) 
	AND db.DatabaseName = 'FinanceMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateFinanceMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.GLAccountStatsFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.GLAccountStatsFact',2) 
	AND db.DatabaseName = 'FinanceMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PsoftUserList' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PsoftUserList',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PsoftUserList',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PsoftUserList' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Finance.GLAccountFact_Actual',1) 
	AND obj.ObjectSchemaName = PARSENAME('Finance.GLAccountFact_Actual',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PsoftUserList' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PsoftUserListValidation',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PsoftUserListValidation',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LabLoadResults' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LABFACT',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LABFACT',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LabLoadResults' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LABResults',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LABResults',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LabLoadResults' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('LAB.LABFACT',1) 
	AND obj.ObjectSchemaName = PARSENAME('LAB.LABFACT',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LabLoadResults' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LABResults',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LABResults',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'MedComExtract' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.MedCom',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.MedCom',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'MedComExtract' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('MedCom.ExtractEncounter',1) 
	AND obj.ObjectSchemaName = PARSENAME('MedCom.ExtractEncounter',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'MedComExtract' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('MedCom.ExtractICBC_RCMP',1) 
	AND obj.ObjectSchemaName = PARSENAME('MedCom.ExtractICBC_RCMP',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'MedComExtract' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('MedCom.ExtractOOP',1) 
	AND obj.ObjectSchemaName = PARSENAME('MedCom.ExtractOOP',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'MedComExtract' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('MedCom.ExtractOtherThirdParty',1) 
	AND obj.ObjectSchemaName = PARSENAME('MedCom.ExtractOtherThirdParty',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'MedComExtract' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('MedCom.ExtractPQ',1) 
	AND obj.ObjectSchemaName = PARSENAME('MedCom.ExtractPQ',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'MedComExtract' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('MedCom.ExtractWCB',1) 
	AND obj.ObjectSchemaName = PARSENAME('MedCom.ExtractWCB',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'MHAMRRSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('MHAMRR.ExtractClientFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('MHAMRR.ExtractClientFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'MHAMRRSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('MHAMRR.ExtractDiagnosisFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('MHAMRR.ExtractDiagnosisFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'MHAMRRSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('MHAMRR.ExtractHoNoSFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('MHAMRR.ExtractHoNoSFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'MHAMRRSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('MHAMRR.ExtractServiceEpisodeFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('MHAMRR.ExtractServiceEpisodeFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'MHAMRRSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('MHAMRR.ExtractServiceEventFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('MHAMRR.ExtractServiceEventFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'MHAMRRSubmission' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('MHAMRR.ExtractSubstanceUseFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('MHAMRR.ExtractSubstanceUseFact',2) 
	AND db.DatabaseName = 'ExternalExtractProcessing' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ORCaseCosting' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.CaseCostingView',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.CaseCostingView',2) 
	AND db.DatabaseName = 'ORMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ORCaseCosting' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.CaseCostingView',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.CaseCostingView',2) 
	AND db.DatabaseName = 'ORMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ORCaseCosting' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ORData.CaseCostingFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ORData.CaseCostingFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ORCaseCosting' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CaseCosting',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CaseCosting',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ORCaseCosting' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CaseCosting',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CaseCosting',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ORCaseCosting' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.CaseCosting',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.CaseCosting',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ORCaseCosting' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORCaseCostingLoad',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORCaseCostingLoad',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ORCaseCosting' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORCaseCostingLoad',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORCaseCostingLoad',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ORCaseCosting' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Dim.ORResourceCatalog',1) 
	AND obj.ObjectSchemaName = PARSENAME('Dim.ORResourceCatalog',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ORCaseCosting' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORResourceCatalog',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORResourceCatalog',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ORCompletedCases' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ORData.CompletedFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ORData.CompletedFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ORCompletedCases' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORCompleted',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORCompleted',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ORCompletedCases' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORCompleted',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORCompleted',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ORCompletedCases' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORCompleted',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORCompleted',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ORCompletedCases' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORCompleted',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORCompleted',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ORCompletedCases' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORCompletedFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORCompletedFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCORCompleted' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PHCORCompleted',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PHCORCompleted',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCORCompleted' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PHCORCompleted',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PHCORCompleted',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCORCompleted' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PHCORCompleted',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PHCORCompleted',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCORCompleted' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PHCORCompleted',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PHCORCompleted',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCORCompleted' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PHCORCompletedLoad',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PHCORCompletedLoad',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCORCompleted' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('ORData.CompletedFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('ORData.CompletedFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCORCompleted' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PHCORCompletedFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PHCORCompletedFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PHCORCompleted' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PHCORPxRollup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PHCORPxRollup',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateORMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORCompletedStage',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORCompletedStage',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateORMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ORCompletedStage',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ORCompletedStage',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateORMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.CompletedFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.CompletedFact',2) 
	AND db.DatabaseName = 'ORMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateORMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.PxSpecialGroupCase',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.PxSpecialGroupCase',2) 
	AND db.DatabaseName = 'ORMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PopulateORMart' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.CaseNumber',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.CaseNumber',2) 
	AND db.DatabaseName = 'ORMart' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LGHAdmin1',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LGHAdmin1',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PharmacyAdmin1Load',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PharmacyAdmin1Load',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LGHAdmin2',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LGHAdmin2',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.pharmcyAdmin2LoadError_temp',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.pharmcyAdmin2LoadError_temp',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.LGHAdmin2',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.LGHAdmin2',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.pharmcyAdmin2LoadError_temp',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.pharmcyAdmin2LoadError_temp',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PharmacyAdmin2Load',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PharmacyAdmin2Load',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminLGH' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PharmacyAdminLoad',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PharmacyAdminLoad',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminLoadToProd' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Pharmacy.AdminFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Pharmacy.AdminFact',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PHCAdmin1',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PHCAdmin1',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PharmacyAdmin1Load',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PharmacyAdmin1Load',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PHCAdmin2',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PHCAdmin2',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PHCAdmin2',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PHCAdmin2',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PharmacyAdmin2Load',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PharmacyAdmin2Load',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PharmacyAdminLoad',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PharmacyAdminLoad',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PharmacyAdminLoad',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PharmacyAdminLoad',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminVA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.VAAdmin1',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.VAAdmin1',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminVA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PharmacyAdmin1Load',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PharmacyAdmin1Load',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminVA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.VAAdmin2',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.VAAdmin2',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminVA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.pharmcyAdmin2LoadError_temp',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.pharmcyAdmin2LoadError_temp',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminVA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.VAAdmin2',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.VAAdmin2',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminVA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.pharmcyAdmin2LoadError_temp',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.pharmcyAdmin2LoadError_temp',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminVA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PharmacyAdmin2Load',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PharmacyAdmin2Load',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyAdminVA' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PharmacyAdminLoad',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PharmacyAdminLoad',2) 
	AND db.DatabaseName = 'DSDW' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PharmacyOrderPHC' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.PHCOrder',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.PHCOrder',2) 
	AND db.DatabaseName = 'SourceDataArchive' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PPE_LoadIndicators' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ExternalAnes',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ExternalAnes',2) 
	AND db.DatabaseName = 'PPE' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PPE_LoadIndicators' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ExternalData',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ExternalData',2) 
	AND db.DatabaseName = 'PPE' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PPE_LoadIndicators' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ExternalOrth',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ExternalOrth',2) 
	AND db.DatabaseName = 'PPE' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PPE_LoadIndicators' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ExternalPed',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ExternalPed',2) 
	AND db.DatabaseName = 'PPE' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PPE_LoadIndicators' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.ExternalPsy',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.ExternalPsy',2) 
	AND db.DatabaseName = 'PPE' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PPE_LoadIndicators' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DimIndicator',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DimIndicator',2) 
	AND db.DatabaseName = 'PPE' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PPE_LoadIndicators' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DimPeerGroup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DimPeerGroup',2) 
	AND db.DatabaseName = 'PPE' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PPE_LoadIndicators' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DimPhysician',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DimPhysician',2) 
	AND db.DatabaseName = 'PPE' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'PPE_LoadIndicators' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.DimPhysicianPeerGroup',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.DimPhysicianPeerGroup',2) 
	AND db.DatabaseName = 'PPE' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HAILoadRawEx' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('HAI.HAIFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('HAI.HAIFact',2) 
	AND db.DatabaseName = 'RawEx' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HAILoadRawEx' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HAIFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HAIFact',2) 
	AND db.DatabaseName = 'RawEx' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HAILoadRawEx' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HAIFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HAIFact',2) 
	AND db.DatabaseName = 'RawEx' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HAILoadRawEx' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HAIFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HAIFact',2) 
	AND db.DatabaseName = 'RawEx' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'HAILoadRawEx' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('Staging.HAIFact',1) 
	AND obj.ObjectSchemaName = PARSENAME('Staging.HAIFact',2) 
	AND db.DatabaseName = 'RawEx' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadSurvey' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('SurveyResponses.S1_Staging',1) 
	AND obj.ObjectSchemaName = PARSENAME('SurveyResponses.S1_Staging',2) 
	AND db.DatabaseName = 'Valhue' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadSurvey_2' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('SurveyResponses.S2_Staging',1) 
	AND obj.ObjectSchemaName = PARSENAME('SurveyResponses.S2_Staging',2) 
	AND db.DatabaseName = 'Valhue' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadSurvey_2_FormatChecks' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('staging.fileFormatCheck',1) 
	AND obj.ObjectSchemaName = PARSENAME('staging.fileFormatCheck',2) 
	AND db.DatabaseName = 'Valhue' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadSurvey_3' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('SurveyResponses.s3_Staging',1) 
	AND obj.ObjectSchemaName = PARSENAME('SurveyResponses.s3_Staging',2) 
	AND db.DatabaseName = 'Valhue' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'LoadSurveys_FormatChecks' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('staging.fileFormatCheck',1) 
	AND obj.ObjectSchemaName = PARSENAME('staging.fileFormatCheck',2) 
	AND db.DatabaseName = 'Valhue' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ValhueExtractFormatCheck' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('staging.fileFormatCheck',1) 
	AND obj.ObjectSchemaName = PARSENAME('staging.fileFormatCheck',2) 
	AND db.DatabaseName = 'Valhue' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ValhueWaitlistLoad' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('staging.ORMISExtractStage1',1) 
	AND obj.ObjectSchemaName = PARSENAME('staging.ORMISExtractStage1',2) 
	AND db.DatabaseName = 'Valhue' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'ValhueWaitlistLoad' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('staging.ORMISExtractStage1',1) 
	AND obj.ObjectSchemaName = PARSENAME('staging.ORMISExtractStage1',2) 
	AND db.DatabaseName = 'Valhue' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'TestExcel' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.TestExcel',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.TestExcel',2) 
	AND db.DatabaseName = 'valhue' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);


;WITH pkg AS ( 
	SELECT PkgID 
	FROM DQMF.dbo.ETL_Package AS pkg 
	WHERE 1=1 
	AND pkg.PkgName = 'TestExcel_withOLEDB' 
) 
, obj AS ( 
	SELECT ObjectID 
	FROM DQMF.dbo.MD_Object AS obj 
   JOIN DQMF.dbo.MD_Database AS db 
	ON obj.DatabaseId = db.DatabaseId 
	WHERE 1=1 
	AND obj.ObjectPhysicalName = PARSENAME('dbo.TestExcel',1) 
	AND obj.ObjectSchemaName = PARSENAME('dbo.TestExcel',2) 
	AND db.DatabaseName = 'VALHUE' 
) 
, map AS ( 
	SELECT pkg.PkgID, obj.ObjectID 
	FROM pkg  
	CROSS JOIN obj 
) 
MERGE INTO DQMF.dbo.ETL_PackageObject AS dst 
USING map  
ON ( 
map.PkgID = dst.PackageID 
AND map.ObjectID = dst.ObjectID) 
WHEN MATCHED THEN  
UPDATE SET  
dst.PackageID = map.PkgID 
WHEN NOT MATCHED THEN 
INSERT VALUEs (map.PkgID, map.objectID);
