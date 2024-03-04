s3_hive_script_path="s3://flights-delay/scripts/security_delay/security_delay.hql"
s3_output_bucket_path="s3://flights-delay/hadoop-output/security_delay/hive_security_delay_time.csv"

# Download the Hive script from S3
aws s3 cp $s3_hive_script_path ./security_delay.hql

# Check if the download was successful
if [ ! -f ./security_delay.hql ]; then
    echo "Failed to download Hive script from S3."
    exit 1
fi

echo "Execution Time in Seconds" > hive_security_delay_time.csv

# Loop to execute the script 5 times
for i in {1..5}; do
    # Start timer
    start_time=$(date +%s)

    # Execute your Hive script
    hive -f ./security_delay.hql

    # End timer
    end_time=$(date +%s)

    # Calculate duration
    duration=$((end_time - start_time))

    # Append execution time info to CSV
    echo "$duration" >> hive_security_delay_time.csv

done

# Upload the execution time file to S3 after completing all iterations
aws s3 cp hive_security_delay_time.csv $s3_output_bucket_path

echo "All executions completed and execution times recorded."