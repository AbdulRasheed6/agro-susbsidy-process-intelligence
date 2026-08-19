# agro-susbsidy-process-intelligence
Analysis and ML-driven optimization of the EU Agricultural Subsidy Process using event log data. Includes predictive modeling, process mining, and data engineering pipelines for monitoring workflow efficiency, delays, and operational insights


# Workflows

1. Set up docker infrasturcuture environment
  a. docker compose up -d
  b. docker compose -f docker-compose.admin.yml
  c.  docker compose -f docker-compose.admin.yml run --rm admin-job python3 scripts/setup_minio.py
  d. docker run --rm -it --network lakehouse-net -v ${PWD}:/work --entrypoint=/bin/sh minio/mc
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

4 Download , stage and ingest:
    a. docker compose -f docker-compose.ingestion.yml run --rm ingestion-job bash
    b.  python3  src/pipelines/ingestion/download_and_stage.py
    c.  python3  src/pipelines/ingestion/ingestion_pipeline.py
    

5. Prepare experiment environment
    a.  docker compose -f docker-compose.notebook.yml up -d
    b.  hostname -I (pick any network IP address)
    c.  docker logs notebook-job (get token from it)
    c.  Then login with this (<IP address>:8899)
    d.   Pick the token  from thes server url  and  add a password

6. Proceed to the next phase (bronze - silver)
   a. docker compose -f docker-compose.transform.yml run --rm transform-job bash
   b. python3 src/pipelines/transformation/bronze_to_silver_transformer.py
