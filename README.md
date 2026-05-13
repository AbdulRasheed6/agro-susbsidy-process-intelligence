# agro-susbsidy-process-intelligence
Analysis and ML-driven optimization of the EU Agricultural Subsidy Process using event log data. Includes predictive modeling, process mining, and data engineering pipelines for monitoring workflow efficiency, delays, and operational insights


# Workflows

1. Set up docker infrasturcuture environment
  a. docker compose up -d
  b. docker compose -f docker-compose.admin.yml
  c.  docker compose -f docker-compose.admin.yml run --rm admin-job python3 scripts/setup_minio.py
2. Create and set RBAC policy
   a. mc  alias set  local endpoint admin password
   b. mc admin policy create local ingestion-policy /work/configs/policies/ingestion-policy.json
Created policy `ingestion-policy` successfully.
   mc admin policy create local transformation-policy /work/configs/policies/transform-policy.json
Created policy `transformation-policy` successfully.
   mc admin policy create local analytics-policy /work/configs/policies/analytics-policy.json
Created policy `analytics-policy` successfully.
   
   c. mc admin user add local ingestion-user ingestion-password 
   d.  mc admin policy attach local ingestion-policy --user ingestion-user

3. Test spark intenration with MinIO and alsp if RBAC policy holds for different cateory of users
    a. docker compose up -d
    b. docker compose -f  docker-compose.test.yml up


