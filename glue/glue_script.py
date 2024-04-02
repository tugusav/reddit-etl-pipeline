import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from awsglue import DynamicFrame


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1710584061086 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, 
    connection_type="s3", format="csv", 
    connection_options={"paths": ["s3://reddit-pipeline-sink-tugusavcloud/raw/reddit_20240316.csv"], "recurse": True}, 
    transformation_ctx="AmazonS3_node1710584061086")

df = AmazonS3_node1710584061086.toDF()

# Create new column engagement from Score + Num_comments
df_transformed = df.withColumn("engagement", df['score'] + df['num_comments'])

# Create new column predicted_upvote from upvote_ratio * Score
df_transformed = df_transformed.withColumn("predicted_upvote", df['upvote_ratio'] * df['score'])

# Convert DataFrame to DynamicFrame
S3bucket_node_combined = DynamicFrame.toDF(df_transformed, glueContext, 'S3bucket_node_combined')


# Script generated for node Amazon S3
AmazonS3_node1710584075528 = glueContext.write_dynamic_frame.from_options(
    frame=S3bucket_node_combined, connection_type="s3", 
    format="csv", 
    connection_options={"path": "s3://reddit-pipeline-sink-tugusavcloud/transformed/", "partitionKeys": []}, 
    transformation_ctx="AmazonS3_node1710584075528")

job.commit()