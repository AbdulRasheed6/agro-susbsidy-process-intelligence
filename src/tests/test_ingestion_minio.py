from src.utils.spark_session import create_spark_session


def test_spark_minio():
    spark= create_spark_session("test-minio")


    # write  test data
    df= spark.range(5)

    print("\n--- Test 1: Write to Bronze ---")

    try:


        df.write.mode("overwrite").parquet("s3a://bronze/test_connection")

        print("SUCCESS: Spark wrote to bronze")

    except Exception as e:
        print(f" Failed : Write to bronze -> {e}")


    print("\n--- Test 2 : Read from bronze---")

    try:
        df_read= spark.read.parquet("s3a://bronze/test_connection")
        df_read.show()

        print(" Success: Read from bronze")

    except Exception as e:
        print(f" Failed: Read from bronze -> {e}")


    print("\n--- Test 3: Write to Gold (should fail for ingestion user) ---")

    
    try:
        df.write.mode("overwrite").parquet("s3a://gold/test_write")
        print(" Error should not have access to gold")

    except Exception as e:
        # Check if '403' or 'Access Denied' is in the error message
        if "403" in str(e) or "Access Denied" in str(e):
            print("Expected Failure: Access Denied (policy enforced)")
        else:
            print(f" Unexpected Error : {e}")

    spark.stop()

if __name__ == "__main__":
    print("Starting test ...")
    test_spark_minio()


