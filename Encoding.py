# Databricks notebook source
from pyspark.sql import functions as F

# use databricks arguments for parameters
def get_argument(parameter):
    dbutils.widgets.text(parameter, '')
    return dbutils.widgets.get(parameter)

# job params, constants for now
num_record_files = 1

# script parameters, parse from JSON input to the job task
csv_input_path = get_argument("csv_input_path")
train_output_path = get_argument("train_output_path")
test_output_path = get_argument("test_output_path")
print("csv_input_path: " + csv_input_path)
print("train_output_path: " + train_output_path)
print("test_output_path: " + test_output_path)

# read in the data set
activity_df = spark.read.option("inferSchema",False).option("header", "true").csv(csv_input_path)
activity_df.createOrReplaceTempView("activity")

labeled_df = spark.sql("""                  
    select case when duration_period/tenure > 0.3 then 1 else 0  end as is_retrained
        ,cast(num_sessions as float) as num_sessions                    
        ,cast(game_level_tries as float) as game_level_tries                    
        ,cast(in_game_powers as float) as in_game_powers                    
        ,cast(powers_successfully_applied as float) as powers_successfully_applied                    
        ,cast(near_win_attempts as float) as near_win_attempts                    
        ,cast(session_duration as float) as session_duration       
        ,country_code
        ,timezone 
        ,case when rand() < 0.2 then 1 else 0 end as is_holdout
    from (
        select * except (_c0, tenure), datediff(cast(last_activity_dt as date), cast(first_activity_dt as date)) as duration_period, cast(tenure as int) as tenure
        from activity
    )
    where tenure >= 14
      and tenure <= 90
""")

labeled_df.createOrReplaceTempView("labeled")

# scale the numeric values
scaled_df = spark.sql("""                                        
    select is_retrained, is_holdout
        ,COALESCE(num_sessions, avg(num_sessions) over ())/max(num_sessions) over () as num_sessions
        ,COALESCE(game_level_tries, avg(game_level_tries) over ())/max(game_level_tries) over () as game_level_tries
        ,COALESCE(in_game_powers, avg(in_game_powers) over ())/max(in_game_powers) over () as in_game_powers
        ,COALESCE(powers_successfully_applied, avg(powers_successfully_applied) over ())/max(powers_successfully_applied) over () as powers_successfully_applied
        ,COALESCE(near_win_attempts, avg(near_win_attempts) over ())/max(near_win_attempts) over () as near_win_attempts
        ,COALESCE(session_duration, avg(session_duration) over ())/max(session_duration) over () as session_duration
        ,country_code
        ,timezone 
    from labeled
""")

scaled_df.createOrReplaceTempView("scaled")

# apply vocabularies
encoded_df = spark.sql("""                                        
    with country_vocab as (
        select country_code, row_number() over (order by counts desc) - 1 as vocab_index
        from (                  
            select country_code, sum(1) as counts
            from scaled
            where is_holdout = 0
            group by 1
        )            
    ), timezone_vocab as (
        select timezone, row_number() over (order by counts desc) as vocab_index
        from (                  
            select timezone, sum(1) as counts
            from scaled
            where is_holdout = 0
            group by 1
        )    
        order by 2
        limit 9        
    )

    select s.* except (country_code, timezone)
        ,c.vocab_index as country_code
        ,COALESCE(t.vocab_index, 0) as timezone 
    from scaled s
    join country_vocab as c
      on s.country_code = c.country_code
    left outer join timezone_vocab as t
      on s.timezone = t.timezone
""")

# reshape the data
reshaped_df = encoded_df.withColumn("numeric", F.array(["num_sessions", "game_level_tries", "in_game_powers", "powers_successfully_applied", "near_win_attempts", "session_duration"]))
reshaped_df = reshaped_df.withColumn("categorical", F.array(["country_code", "timezone"]))
reshaped_df = reshaped_df.select("is_retrained", "numeric", "categorical")

# split into train and test
train_df = reshaped_df.filter("is_holdout = 0")
test_df = reshaped_df.filter("is_holdout = 1")

# save records
train_df.repartition(num_record_files).write.format("tfrecords").option("recordType", "Example").mode("overwrite").save(train_output_path)
test_df.repartition(num_record_files).write.format("tfrecords").option("recordType", "Example").mode("overwrite").save(test_output_path)

