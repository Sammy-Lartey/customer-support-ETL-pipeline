from libs import *

class Analytics:

    def __init__(self, engine, schema_name):
        self.engine = engine
        self.schema_name = schema_name


    def create_indexes(self):
        indexes = [
            ('customers', 'number'),
            ('customers', 'profileId'),
            ('customers', 'customerId'),
            ('complaints', 'profileId'), 
            ('complaints', 'customerId'),
            ('complaints', 'number'),
            ('complaints', 'region'),
        ]

        with self.engine.begin() as conn:
            for table, column in indexes:
                try:
                    conn.execute(text(f"""
                        CREATE INDEX IF NOT EXISTS idx_{table}_{column}
                        ON {self.schema_name}.{table} ("{column}");
                    """))
                    print(f"Created index on {table}.{column}")
                except Exception as e:
                    print(f"Error creating index on {table}.{column}: {e}")
    
    def create_views(self):
        schema = self.schema_name
        views = {
            'vw_customer_overview': f"""
                CREATE OR REPLACE VIEW {schema}.vw_customer_overview AS
                SELECT c."customerId", c."profileId", c."name", c."number",
                    c."number2", c.gender, c."dateOfBirth", c."accountType",
                    c.branch,
                    COUNT(co."customerId") AS "totalComplaints",
                    MIN(co."logDate") AS "firstComplaintDate",
                    MAX(co."logDate") AS "lastComplaintDate"
                FROM {schema}.customers c
                LEFT JOIN {schema}.complaints co
                    ON c."customerId" = co."customerId"
                GROUP BY c."customerId", c."profileId", c."name",
                        c."number", c."number2", c.gender,
                        c."dateOfBirth", c."accountType", c.branch;
            """,

            'vw_complaint_summary': f"""
                CREATE OR REPLACE VIEW {schema}.vw_complaint_summary AS
                SELECT co."customerId", c."name", c."number", co."profileId",
                    co."logDate", co."complaintSource", co."natureOfComplaint",
                    co."subject", co."detailsOfComplaint", co."status",
                    co."resolutionDate", co."turnaroundTime", co."location",
                    co."region", co."updates", co."comment", co."reasonForReversalRequest",
                    CASE
                        WHEN co."turnaroundTime" <= 24 THEN 'Within 1 day'
                        WHEN co."turnaroundTime" <= 72 THEN 'Within 3 days'
                        ELSE 'Over 3 days'
                    END as "turnaroundCategory"
                FROM {schema}.complaints co
                JOIN {schema}.customers c ON co."customerId" = c."customerId";
            """,

            'vw_regional_stats': f"""
                CREATE OR REPLACE VIEW {schema}.vw_regional_stats AS
                SELECT region,
                    COUNT(*) as "totalComplaints",
                    COUNT(DISTINCT "customerId") as "uniqueCustomers",
                    AVG("turnaroundTime") as "avgTurnaroundTime",
                    SUM(CASE WHEN status = 'Resolved' THEN 1 ELSE 0 END) as "resolvedCount",
                    ROUND(
                        (SUM(CASE WHEN status = 'Resolved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2
                    ) as "resolutionRate"
                FROM {schema}.complaints
                WHERE region IS NOT NULL AND region != 'Unknown'
                GROUP BY region
                ORDER BY "totalComplaints" DESC;
            """,

            'vw_complaint_status_report': f"""
                CREATE OR REPLACE VIEW {schema}.vw_complaint_status AS
                SELECT co."status",
                    COUNT(*) as "complaintCount",
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {schema}.complaints), 2) as "percentage",
                    AVG(co."turnaroundTime") as "avgTurnaroundTime",
                    MIN(co."logDate") as "oldestComplaint",
                    MAX(co."logDate") as "newestComplaint"
                FROM {schema}.complaints co
                GROUP BY co."status"
                ORDER BY "complaintCount" DESC;
            """,

            'vw_monthly_trends': f"""
                CREATE OR REPLACE VIEW {schema}.vw_monthly_trends AS
                SELECT TO_CHAR(DATE_TRUNC('month', co."logDate"), 'Month YYYY') as "month",
                    COUNT(*) as "totalComplaints",
                    COUNT(DISTINCT co."customerId") as "uniqueCustomers",
                    AVG(co."turnaroundTime") as "avgTurnaroundTime",
                    MODE() WITHIN GROUP (ORDER BY co."natureOfComplaint") as "topComplaintType",
                    MODE() WITHIN GROUP (ORDER BY co."region") as "topRegion"
                FROM {schema}.complaints co
                WHERE co."logDate" IS NOT NULL
                GROUP BY DATE_TRUNC('month', co."logDate")
                ORDER BY DATE_TRUNC('month', co."logDate") DESC;
            """
        }
        
        with self.engine.begin() as conn:
            for name, sql in views.items():
                try:
                    conn.execute(text(sql))
                    print(f"Created view: {name}")
                except Exception as e:
                    print(f"Error creating view {name}: {str(e)}")
       
        print("All views created successfully.")

    
    def create_materialized_views(self):
        schema = self.schema_name
        mats = {
            'mv_monthly_complaint_summary': f"""
                CREATE MATERIALIZED VIEW {schema}.mv_monthly_complaint_summary AS
                SELECT TO_CHAR(DATE_TRUNC('month', co."logDate"), 'Month YYYY') as "month",
                       COUNT(*) as "totalComplaints",
                       COUNT(DISTINCT co."customerId") as "uniqueCustomers",
                       AVG(co."turnaroundTime") as "avgTurnaroundTime"
                FROM {schema}.complaints co
                WHERE co."logDate" IS NOT NULL
                GROUP BY DATE_TRUNC('month', co."logDate")
                ORDER BY DATE_TRUNC('month', co."logDate") DESC;
            """
        }
        with self.engine.begin() as conn:
            for name, sql in mats.items():
                conn.execute(text(sql))
        print("Materialized views created successfully.")