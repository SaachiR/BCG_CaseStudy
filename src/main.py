from pyspark.sql import SparkSession
from jobs.carcrash import CarCrash
from settings import config_file_path, appName, logLevel, sparkConfig

if __name__ == '__main__':
    # Initialize sparks session
    spark = SparkSession \
                .builder \
                .config(
                        sparkConfig["sparkConfigDriverOptions"],
                        sparkConfig["sparkConfigAddOpens"]
                        )\
                .appName(appName) \
                .getOrCreate()

    spark.sparkContext.setLogLevel(logLevel)

    carCrashAnalysis = CarCrash(spark, config_file_path)

    # 1. Find the number of crashes (accidents) in which number of persons killed are male?
    print("Analysis Result 1: ", carCrashAnalysis.count_accidents_for_gender(gender="MALE"))

    # 2. How many two-wheelers are booked for crashes?
    print("Analysis Result 2: ", carCrashAnalysis.count_accidents_for_vehicle_style(vehicle_style="MOTORCYCLE"))

    # 3. Which state has the highest number of accidents in which females are involved?
    print("Analysis Result 3: ", carCrashAnalysis.get_state_with_highest_accident_for_gender(gender="FEMALE"))

    # 4. Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    print("Analysis Result 4: ", carCrashAnalysis.get_top_vehicles_contributing_to_injuries_and_death_within_rank(startRank=5,endRank=15))

    # 5. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    print("Analysis Result 5: ")
    print(carCrashAnalysis.get_ethnic_group_with_rank_foreach_veh_body_style(rank=1))

    # 6. Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
    print("Analysis Result 6: ", carCrashAnalysis.get_top_n_zipcodes_with_crashes_due_to_alcohol(n_count=5))

    # 7. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    print("Analysis Result 7: ", carCrashAnalysis.get_count_of_crashes_with_insurance_and_no_damage())

    # 8. Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers,
    # # used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
    print("Analysis Result 8: ", carCrashAnalysis.get_top_n_vehicle_brand_with_conditions(n_state=25, n_vehicle_colours=10, n_vehicle_brands=5))







    