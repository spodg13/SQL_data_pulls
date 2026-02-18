queries = {
    "eDiscovery": """
        DECLARE @PatientID VARCHAR(18) = '{patient_id}'
	    ,@UserID VARCHAR(18) = '{user_id}'
        ,@StartTime DATETIME = '{start_date}'
        ,@Endtime DATETIME = '{end_date}'
        ,@sys_log VARCHAR (18) = '{user_login}';
        SET NOCOUNT ON;

        CREATE TABLE #tmpUSER (User_ID NVARCHAR(50) PRIMARY KEY, User_Name VARCHAR(100))
        INSERT INTO #tmpUSER (User_ID, User_Name) VALUES ('S202360', 'BLUEPRISM; PAC PD')
        INSERT INTO #tmpUSER (User_ID, User_Name) VALUES ('ICSERV', 'SERVICE; IC')
        INSERT INTO #tmpUSER (User_ID, User_Name) VALUES ('USERPSMF', 'USER; PROGRAMMATIC SMF')
        INSERT INTO #tmpUSER (User_ID, User_Name) VALUES ('HBBCKGRND', 'HB; BACKGROUND')
        INSERT INTO #tmpUSER (User_ID, User_Name) VALUES ('TASBATCH1', 'TAS, BACKGROUND USER')
        INSERT INTO #tmpUSER (User_ID, User_Name) VALUES ('30109164', 'INTERCONNECT, RDD-SERVICES')
        INSERT INTO #tmpUSER (User_ID, User_Name) VALUES ('1', 'EPIC, MADISON')


        SELECT a.ACCESS_INSTANT
            ,a.PROCESS_ID
            ,a.ACCESS_TIME
            ,a.USER_ID
            ,a.METRIC_ID
            ,a.CSN
            ,a.WORKSTATION_ID
            ,a.PAT_ID
        INTO #tmpAcc
        FROM {access_log} AS a
        WHERE a.ACCESS_TIME BETWEEN @StartTime
                AND @Endtime
            {where_clause}
            ;

        WITH tmpDTL
        AS (
            SELECT ACCESS_TIME
                ,v.ACCESS_INSTANT
                ,a.USER_ID
                ,a.METRIC_ID
                ,m.EVENT_ACTION_TYPE_C
                ,m.EVENT_ACT_SUBTYPE_C
                ,v.DATA_MNEMONIC_ID
                ,v.STRING_VALUE
                ,a.CSN
                ,a.WORKSTATION_ID
                ,a.PAT_ID
            FROM #tmpAcc a
            LEFT JOIN {acc_log_dtl} v
                ON a.ACCESS_INSTANT = v.ACCESS_INSTANT
                AND a.PROCESS_ID = v.PROCESS_ID
            LEFT JOIN clarity_rpt..ACCESS_LOG_METRIC m
                ON m.METRIC_ID = a.METRIC_ID
            WHERE a.ACCESS_TIME BETWEEN @StartTime
                    AND @Endtime
            )
            ,tmpMTDTL
        AS (
            SELECT ACCESS_TIME
                ,w.ACCESS_INSTANT
                ,a.USER_ID
                ,a.METRIC_ID
                ,m.EVENT_ACTION_TYPE_C
                ,m.EVENT_ACT_SUBTYPE_C
                ,w.DATA_MNEMONIC_ID
                ,w.STRING_VALUE
                ,a.CSN
                ,a.WORKSTATION_ID
                ,a.PAT_ID
            FROM #tmpAcc a
            LEFT JOIN {acc_log_MTDTL} W
                ON a.ACCESS_INSTANT = w.ACCESS_INSTANT
                AND a.PROCESS_ID = w.PROCESS_ID
            LEFT JOIN clarity_rpt..ACCESS_LOG_METRIC m
                ON m.METRIC_ID = a.METRIC_ID
            WHERE a.ACCESS_TIME BETWEEN @StartTime
                    AND @Endtime
                AND w.DATA_MNEMONIC_ID IS NOT NULL
            )
        SELECT *
        INTO #tmpFinal
        FROM tmpDTL

        UNION

        SELECT *
        FROM tmpMTDTL

        SELECT tu.ACCESS_TIME
            ,tu.User_ID
            ,Replace(e.name, ',', ';') AS Emp_Name
            ,tu.CSN
            ,v.ENC_TYPE_TITLE
            ,d.DEPARTMENT_NAME
            ,tu.METRIC_ID
            ,m.METRIC_NAME
            ,z.NAME AS 'Type'
            ,tu.DATA_MNEMONIC_ID
            ,tu.STRING_VALUE
            ,CASE 
                WHEN tu.DATA_MNEMONIC_ID = 'LRP'
                    THEN (
                            SELECT rd.REPORT_NAME
                            FROM clarity_rpt.dbo.REPORT_DETAILS rd
                            WHERE rd.LRP_ID = tu.STRING_VALUE
                            )
                WHEN tu.DATA_MNEMONIC_ID = 'DAT'
                    THEN (
                            SELECT d.CALENDAR_DT_STR
                            FROM clarity_rpt.dbo.DATE_DIMENSION d
                            WHERE Ceiling(tu.STRING_VALUE) = d.EPIC_DAT
                            )
                WHEN tu.DATA_MNEMONIC_ID = 'DXRDAT'
                    THEN (
                            SELECT d.CALENDAR_DT_STR
                            FROM clarity_rpt.dbo.DATE_DIMENSION d
                            WHERE Ceiling(tu.STRING_VALUE) = d.EPIC_DAT
                            )
                WHEN tu.DATA_MNEMONIC_ID LIKE 'HNO%'
                    THEN (
                            SELECT CONCAT (
                                    z.NAME
                                    ,' signed by '
                                    ,h.current_author_id
                                    ,' Date Of Service '
                                    ,h.CRT_INST_LOCAL_DTTM
                                    )
                            FROM Clarity_rpt..HNO_INFO h
                            INNER JOIN clarity_rpt..ZC_NOTE_TYPE_IP z
                                ON h.IP_NOTE_TYPE_C = z.TYPE_IP_C
                            WHERE h.NOTE_ID = tu.STRING_VALUE
                            )
                WHEN tu.DATA_MNEMONIC_ID LIKE 'PROVS%'
                    THEN (
                            SELECT s.PROV_NAME
                            FROM Clarity_rpt..Clarity_ser s
                            WHERE s.PROV_ID = tu.STRING_VALUE
                            )
                ELSE ''
                END AS Report_Info
            ,CONCAT (
                p.PAT_LAST_NAME
                ,'; '
                ,p.PAT_First_Name
                ) AS Patient_Name
            ,p.PAT_MRN_ID
            ,tu.PAT_ID
            ,tu.WORKSTATION_ID
        FROM #tmpFinal tu
        LEFT JOIN #tmpUSER AS PU 
            ON tu.USER_ID=PU.User_ID
        INNER JOIN clarity_rpt.dbo.ACCESS_LOG_METRIC AS m
            ON tu.METRIC_ID = m.metric_id
        LEFT JOIN clarity_rpt.dbo.PATIENT AS p
            ON tu.PAT_ID = p.PAT_ID
        LEFT JOIN clarity_rpt.dbo.CLARITY_EMP AS e
            ON tu.USER_ID = e.USER_ID AND e.EMP_RECORD_TYPE_C = 1
        LEFT JOIN clarity_rpt..ZC_EVENT_ACTION_TYPE z
            ON tu.EVENT_ACTION_TYPE_C = z.EVENT_ACTION_TYPE_C
        LEFT JOIN clarity_rpt..V_PAT_ENC v
            ON tu.CSN = v.PAT_ENC_CSN_ID
        LEFT JOIN clarity_rpt..CLARITY_DEP d
            ON v.effective_dept_id = d.DEPARTMENT_ID

        WHERE PU.USER_ID IS NULL and e.name is not null

        ORDER BY tu.ACCESS_TIME
            ,tu.METRIC_ID
            ,tu.DATA_MNEMONIC_ID
            
        DROP TABLE #tmpAcc

        DROP TABLE #tmpFinal
        DROP TABLE #tmpUSER
        """,
# ----------------------------
# CYBER PATIENTLESS
# ----------------------------
    "cyber_patientless": """
        DECLARE
	        @UserID    NVARCHAR(50)= '{user_id}', 
            @StartTime DATETIME = '{start_date}',
            @EndTime   DATETIME = '{end_date}',
            @PatientID NVARCHAR(50) = '{patient_id}',
            @UserLogin NVARCHAR(50) = '{user_login}';
            SET NOCOUNT ON;

        with CTE_WRKF as (
            SELECT  
                a.ACCESS_INSTANT
                ,a.PROCESS_ID
                ,a.ACCESS_TIME
                ,a.METRIC_ID
                ,a.USER_ID
                ,a.WORKSTATION_ID
                ,a.PAT_ID

            FROM {acc_WRKF} a
            WHERE a.ACCESS_TIME BETWEEN @StartTime AND @Endtime 
            {where_clause}   
            AND a.PAT_ID IS NULL
            )

            SELECT 
                cast(a.access_time as date) as 'Date'
                ,a.ACCESS_TIME
                ,a.USER_ID
                ,e.NAME
                ,e.SYSTEM_LOGIN
                ,d.DEPARTMENT_NAME
                ,a.METRIC_ID
                ,m.METRIC_NAME
                ,a.WORKSTATION_ID
            INTO #tmpWRKF

            FROM  CTE_WRKF a 
            LEFT JOIN clarity_rpt.dbo.ACCESS_LOG_METRIC m ON a.METRIC_ID = m.METRIC_ID
            LEFT JOIN clarity_rpt.dbo.CLARITY_EMP e ON a.USER_ID = e.USER_ID
            LEFT JOIN clarity_rpt..CLARITY_DEP AS d ON e.MR_LOGON_DEPT_ID = d.DEPARTMENT_ID
            ;

        WITH tmpAccess
        AS (
            SELECT 
                cast(d.access_time as date) as 'Date'
                ,d.ACCESS_INSTANT
                ,d.ACCESS_TIME
                ,d.USER_ID
                ,d.METRIC_ID
                ,d.WORKSTATION_ID
                ,d.PAT_ID

            FROM {access_log} AS d
            WHERE d.ACCESS_TIME BETWEEN @StartTime
                    AND @Endtime
                    AND d.USER_ID=@UserID
                    )

            SELECT 
                w.Date
                ,w.ACCESS_TIME
                ,w.USER_ID
                ,e.NAME
                ,e.SYSTEM_LOGIN
                ,d.DEPARTMENT_NAME
                ,w.METRIC_ID
                ,m.METRIC_NAME
                ,w.WORKSTATION_ID          
            INTO #tmpAccL

            FROM tmpAccess w
            LEFT JOIN clarity_rpt.dbo.ACCESS_LOG_METRIC m ON w.METRIC_ID = m.METRIC_ID
            LEFT JOIN clarity_rpt.dbo.CLARITY_EMP e ON w.USER_ID = e.USER_ID
            LEFT JOIN clarity_rpt..CLARITY_DEP AS d ON e.MR_LOGON_DEPT_ID = d.DEPARTMENT_ID

            ORDER BY ACCESS_TIME

            SELECT *
            FROM #tmpAccL l
            WHERE l.WORKSTATION_ID IS NOT NULL
            UNION
            SELECT *
            FROM #tmpWRKF f
            WHERE f.WORKSTATION_ID IS NOT NULL
            ORDER BY l.ACCESS_TIME

            DROP TABLE #tmpAccL
            DROP TABLE #tmpWRKF

            """,
# ----------------------------
#  ACCESS LOG py READY    
# ----------------------------            
    "access_log_PY": """
        DECLARE @PatientID VARCHAR(18) = '{patient_id}'
            ,@UserID VARCHAR(18) = '{user_id}'
            ,@StartTime DATETIME = '{start_date}'
            ,@Endtime DATETIME = '{end_date}'
            ,@sys_log VARCHAR(18) = '{user_login}';

        SET NOCOUNT ON;

        WITH tmpAccess
        AS (
            SELECT d.ACCESS_INSTANT
                ,d.PROCESS_ID
                ,d.ACCESS_TIME
                ,d.USER_ID
                ,d.METRIC_ID
                ,d.CSN
                ,d.WORKSTATION_ID
                ,d.PAT_ID
            FROM {access_log} AS d
            WHERE d.ACCESS_TIME BETWEEN @StartTime
                    AND @Endtime
                {where_clause}
            ),

        tmp_next
        AS (
            SELECT ACCESS_TIME
                ,v.ACCESS_INSTANT
                ,USER_ID
                ,a.METRIC_ID
                ,m.EVENT_ACTION_TYPE_C
                ,m.EVENT_ACT_SUBTYPE_C
                ,v.DATA_MNEMONIC_ID
                ,v.STRING_VALUE
                --,w.DATA_MNEMONIC_ID AS 'MTDLT_MNEMONIC'
                --,w.STRING_VALUE AS 'MTDLT_STRING'
                ,CSN
                ,WORKSTATION_ID
                ,PAT_ID
            FROM tmpAccess a
            LEFT JOIN {acc_log_dtl} v
                ON a.ACCESS_INSTANT = v.ACCESS_INSTANT
                AND a.PROCESS_ID = v.PROCESS_ID
            LEFT JOIN {acc_log_MTDTL} W
                ON A.ACCESS_INSTANT=W.ACCESS_INSTANT 
                AND A.PROCESS_ID=W.PROCESS_ID
            LEFT JOIN clarity_rpt..ACCESS_LOG_METRIC m
                ON m.METRIC_ID = a.METRIC_ID
                
            )
        SELECT Cast(tu.ACCESS_TIME as date) as 'DATE' 
            ,tu.ACCESS_TIME
            ,tu.USER_ID
            ,e.SYSTEM_LOGIN
            ,Replace(e.name, ',', ';') AS Emp_Name
            ,tu.CSN
            ,v.ENC_TYPE_TITLE as 'ENC_TYPE'
            ,v.ENC_TYPE_C
            ,Case
                When v.HOSP_ADMSN_TIME is null then v.appt_time
            else v.HOSP_ADMSN_TIME
            End as 'ENC_START'
            ,Case	
                When v.HOSP_DISCHRG_TIME is null then dateadd(MINUTE,v.APPT_LENGTH,v.APPT_TIME)
            Else v.HOSP_DISCHRG_TIME
            End as 'ENC_END'
            ,v.CONTACT_DATE as ENC_DATE
            ,d.DEPARTMENT_NAME
            ,tu.METRIC_ID
            ,m.METRIC_NAME
            ,z.NAME AS 'Type'
            ,tu.DATA_MNEMONIC_ID
            ,tu.STRING_VALUE
            ,CASE
                WHEN tu.DATA_MNEMONIC_ID = 'RECDOB'
                    THEN ( 
                            SELECT d.CALENDAR_DT_STR
                            FROM clarity_rpt.dbo.DATE_DIMENSION d
                            WHERE Ceiling(tu.STRING_VALUE) = d.EPIC_DAT
                            )
                WHEN tu.DATA_MNEMONIC_ID = 'LRP'
                    THEN (
                            SELECT rd.REPORT_NAME
                            FROM clarity_rpt.dbo.REPORT_DETAILS rd
                            WHERE rd.LRP_ID = tu.STRING_VALUE
                            )
                WHEN tu.DATA_MNEMONIC_ID = 'DAT'
                    THEN (
                            SELECT d.CALENDAR_DT_STR
                            FROM clarity_rpt.dbo.DATE_DIMENSION d
                            WHERE Ceiling(tu.STRING_VALUE) = d.EPIC_DAT
                            )
                WHEN tu.DATA_MNEMONIC_ID = 'DXRDAT'
                    THEN (
                            SELECT d.CALENDAR_DT_STR
                            FROM clarity_rpt.dbo.DATE_DIMENSION d
                            WHERE Ceiling(tu.STRING_VALUE) = d.EPIC_DAT
                            )
                ELSE '-'
                END AS Report_Info
            --,TU.MTDLT_MNEMONIC
            --,tu.MTDLT_STRING
            ,CONCAT (
                p.PAT_LAST_NAME
                ,'; '
                ,p.PAT_First_Name
                ) AS Patient_Name
            ,p.PAT_MRN_ID
            ,p.PAT_ID
            ,p.birth_date
            ,p.SEX_C
            ,tu.WORKSTATION_ID
        FROM tmp_next tu
        INNER JOIN clarity_rpt.dbo.ACCESS_LOG_METRIC AS m
            ON tu.METRIC_ID = m.metric_id
        LEFT JOIN clarity_rpt.dbo.PATIENT AS p
            ON tu.PAT_ID = p.PAT_ID
        LEFT JOIN clarity_rpt.dbo.CLARITY_EMP AS e
            ON tu.USER_ID = e.USER_ID
        LEFT JOIN clarity_rpt..ZC_EVENT_ACTION_TYPE z
            ON tu.EVENT_ACTION_TYPE_C = z.EVENT_ACTION_TYPE_C
        LEFT JOIN clarity_rpt..V_PAT_ENC v
            ON tu.CSN = v.PAT_ENC_CSN_ID
        LEFT JOIN clarity_rpt..CLARITY_DEP d
            ON v.effective_dept_id = d.DEPARTMENT_ID
        ORDER BY tu.ACCESS_TIME
            ,tu.METRIC_ID
            ,tu.DATA_MNEMONIC_ID
            """,
# ----------------------------
#  ACCESS LOG FAST
# ----------------------------
    "access_log_fast": """
        Declare @PatientID VARCHAR(18) = '{patient_id}',
            @UserID VARCHAR(18) = '{user_id}'
            ,@StartTime DATETIME = '{start_time}'
            ,@Endtime DATETIME = '{end_time}'
            ,@sys_log VARCHAR (18) = '{user_login}';

        Set NOCOUNT ON;

        WITH tmpAccess AS (
            SELECT
                d.ACCESS_INSTANT
                ,d.PROCESS_ID
                ,d.ACCESS_TIME
                ,d.USER_ID
                ,d.METRIC_ID
                ,d.CSN
                ,d.WORKSTATION_ID
                ,d.PAT_ID
                
            FROM {access_log} AS d
            WHERE
                d.ACCESS_TIME BETWEEN @StartTime
                AND @Endtime
                {where_clause}
        ),
    
        tmp_next AS (
            SELECT
                ACCESS_TIME
                ,v.ACCESS_INSTANT
                ,USER_ID
                ,a.METRIC_ID
                ,m.metric_name
                ,m.EVENT_ACTION_TYPE_C
                ,m.EVENT_ACT_SUBTYPE_C
                ,v.DATA_MNEMONIC_ID
                ,v.STRING_VALUE
                ,CSN
                ,WORKSTATION_ID
                ,PAT_ID
                
            FROM tmpAccess a
            LEFT JOIN {acc_log_dtl} v
                ON a.ACCESS_INSTANT = v.ACCESS_INSTANT
                AND a.PROCESS_ID = v.PROCESS_ID
            LEFT JOIN clarity_rpt..ACCESS_LOG_METRIC m
                ON m.METRIC_ID = a.METRIC_ID          
        )
            SELECT
                tu.ACCESS_TIME
                ,tu.User_ID
                ,REPLACE(e.name, ',', ';') AS Emp_Name
                ,e.SYSTEM_LOGIN
                ,tu.CSN
                ,v.ENC_TYPE_TITLE
                ,d.DEPARTMENT_NAME
                ,tu.METRIC_ID
                ,tu.METRIC_NAME
                ,z.NAME AS 'Type'
                ,tu.DATA_MNEMONIC_ID
                ,tu.STRING_VALUE
                ,CASE
                    WHEN tu.DATA_MNEMONIC_ID = 'LRP'
                        THEN (
                        SELECT
                            rd.REPORT_NAME
                        FROM clarity_rpt.dbo.REPORT_DETAILS rd
                        WHERE
                            rd.LRP_ID = tu.STRING_VALUE
                    )
                    WHEN tu.DATA_MNEMONIC_ID = 'DAT'
                        THEN (
                        SELECT
                            d.CALENDAR_DT_STR
                        FROM clarity_rpt.dbo.DATE_DIMENSION d
                        WHERE
                            Ceiling(tu.STRING_VALUE) = d.EPIC_DAT
                    )
                    WHEN tu.DATA_MNEMONIC_ID = 'DXRDAT'
                        THEN (
                        SELECT
                            d.CALENDAR_DT_STR
                        FROM clarity_rpt.dbo.DATE_DIMENSION d
                        WHERE
                            Ceiling(tu.STRING_VALUE) = d.EPIC_DAT
                    )
                    ELSE '-'
                END AS Report_Info
                ,concat(p.PAT_LAST_NAME, '; ', p.PAT_First_Name) AS Patient_Name
                ,p.PAT_MRN_ID
                ,p.PAT_ID
                ,tu.WORKSTATION_ID
            FROM tmp_next tu

            LEFT JOIN clarity_rpt.dbo.PATIENT AS p
                ON tu.PAT_ID = p.PAT_ID
            LEFT JOIN clarity_rpt.dbo.CLARITY_EMP AS e
                ON tu.USER_ID = e.USER_ID
                AND e.EMP_RECORD_TYPE_C = '1'
            LEFT JOIN clarity_rpt..ZC_EVENT_ACTION_TYPE z
                ON tu.EVENT_ACTION_TYPE_C = z.EVENT_ACTION_TYPE_C
            LEFT JOIN clarity_rpt..V_PAT_ENC v
                ON tu.CSN = v.PAT_ENC_CSN_ID
            LEFT JOIN clarity_rpt..CLARITY_DEP d
                ON v.effective_dept_id = d.DEPARTMENT_ID
                

            ORDER BY tu.ACCESS_TIME, tu.METRIC_ID, tu.DATA_MNEMONIC_ID
"""
}