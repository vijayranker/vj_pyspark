from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import math

# Build a spark session
spark = SparkSession.builder.master("local").appName("ecap1").getOrCreate()

# Read data from HDFS
ecapdf = spark.read.csv("csv/bundle.csv",
                        sep="\t", header=True, inferSchema=True)
corr_df = spark.read.csv(
    "csv/sector-corr.csv", sep="\t",
    header=True, inferSchema=True)

combinations = ecapdf.alias("left").crossJoin(ecapdf.alias("right"))

full_data_frame = combinations.join(corr_df, [combinations['left.cat'] == corr_df.sector_id_i,
                                              combinations['right.cat'] == corr_df.sector_id_j])

pull_df = full_data_frame.withColumn("pul", when(
    (full_data_frame['left.bid'] == full_data_frame['right.bid']) & (full_data_frame['left.nt'] == 1),
    full_data_frame['left.ul'] * full_data_frame['right.ul'] * 1)
                                     .otherwise(
    full_data_frame['left.ul'] * full_data_frame['right.ul'] * full_data_frame['corr']))

grouped_pull = pull_df.groupby('left.bid')
pul_df = grouped_pull.agg({'pul': 'sum'})
pul_df.show(10)

final_df = ecapdf.join(pul_df, ecapdf.bid == pul_df.bid)

final_df.show(10)

self_final_df = final_df.alias("left").join(final_df.alias("right"))

sum_pul = final_df.agg(F.sum("sum(pul)")).collect()[0][0]

upul = math.sqrt(sum_pul)

print('sum pul: {}'.format(sum_pul))

print('upul: {}'.format(upul))

self_final_df = final_df.alias("left").join(final_df.alias("right"))

self_final_df_with_corr = self_final_df.join(corr_df, [self_final_df['left.cat'] == corr_df.sector_id_i,
                                                       self_final_df['right.cat'] == corr_df.sector_id_j])

self_final_df_with_corr.show(10)

# Since i don't have the value for rc_term setting it as 0
upul_df = self_final_df_with_corr.withColumn("rc", when(self_final_df_with_corr['left.nt'] == 1,
                                                        ((self_final_df_with_corr['left.ul'] * self_final_df_with_corr[
                                                            'left.ul'] * 1) + 0) / upul)
                                             .otherwise(((self_final_df_with_corr['left.ul'] * self_final_df_with_corr[
    'left.ul'] * self_final_df_with_corr['corr']) + 0) / upul))

upul_df.show(10)
