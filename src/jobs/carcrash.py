from utilities import preprocess
from readers import csv_reader, yaml_reader
from pyspark.sql import Window
from pyspark.sql.functions import countDistinct, col, rank, row_number



class CarCrash:
    def __init__(self, spark, config_file_path) -> None:

        try:
            input_file_paths = yaml_reader.read_yaml_from_path(config_file_path).get("INPUT_FILENAME")
            self.df_charges = csv_reader.load_csv_to_df(spark, input_file_paths.get("Charges"))
            self.df_damages = csv_reader.load_csv_to_df(spark, input_file_paths.get("Damages"))
            self.df_endorse = csv_reader.load_csv_to_df(spark, input_file_paths.get("Endorse"))
            self.df_primary_person = csv_reader.load_csv_to_df(spark, input_file_paths.get("Primary_Person"))
            self.df_units = csv_reader.load_csv_to_df(spark, input_file_paths.get("Units"))
            self.df_restrict = csv_reader.load_csv_to_df(spark, input_file_paths.get("Restrict"))
        except Exception as e:
            print("Error :" + str(e))

    def count_accidents_for_gender(self, gender):
        """
        Find the number of crashes (accidents) in which number of persons killed are male

        Parameters:
        ----------
        gender : gender of the person

        Returns:
        dataframe count
        """
        df =self.df_primary_person.filter(self.df_primary_person.PRSN_GNDR_ID == gender)\
                                  .agg(countDistinct("CRASH_ID")) \
                                  .withColumnRenamed("count(CRASH_ID)","CRASH_ID_COUNT")
        
        return df.first().CRASH_ID_COUNT
        
        
    def count_accidents_for_vehicle_style(self, vehicle_style):
        """
        Count of accidents with a particular vehicle style involved in crashes

        Parameters:
        ----------
        vehicle_style : vehicle_style

        Returns:
        ---------------------
        dataframe count
        """

        df = preprocess.drop_df_duplicates(self.df_units).filter(col("VEH_BODY_STYL_ID").contains(vehicle_style)).count()

        return df


    def get_state_with_highest_accident_for_gender(self, gender):
        """
        Find state has the highest number of accidents for the given gender
        
        Parameters:
        ----------
        gender : gender of the person

        Returns:
        ---------------------
        State with highest accidents
        """
        df = self.df_primary_person.filter(self.df_primary_person.PRSN_GNDR_ID == gender) \
                                   .groupby("DRVR_LIC_STATE_ID") \
                                   .agg(countDistinct("CRASH_ID")) \
                                   .withColumnRenamed("count(CRASH_ID)","CRASH_ID_COUNT")\
                                   .orderBy(col("CRASH_ID_COUNT").desc())

        return df.first().DRVR_LIC_STATE_ID
    
    def get_top_vehicles_contributing_to_injuries_and_death_with_rank(self, startRank, endRank):
        """
        Find Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        
        Parameters:
        ----------
        startRank: from rank
        endRank: to rank

        Returns:
        ---------------------
        List of VEH_MAKE_IDs
        """
        df_units=preprocess.drop_df_duplicates(self.df_units)
        df = self.df_units.filter(df_units.DEATH_CNT >0) \
                                .groupby("VEH_MAKE_ID").sum("TOT_INJRY_CNT")\
                                .withColumnRenamed("sum(TOT_INJRY_CNT)","TOTAL_INJURY_CNT")
        ordering = Window.orderBy(col("TOTAL_INJURY_CNT").desc())
        
        df_rnk_injury_count=df.withColumn("RNK_INJRY_CNT",rank().over(ordering))
        df_VEH_MAKE_IDs_with_Death_Injury= df_rnk_injury_count.where((df_rnk_injury_count.RNK_INJRY_CNT>=startRank) & (df_rnk_injury_count.RNK_INJRY_CNT<=endRank))
        
        return [vehicle[0] for vehicle in df_VEH_MAKE_IDs_with_Death_Injury.select("VEH_MAKE_ID").collect()]

    def get_ethnic_group_with_rank_foreach_crash_veh_body_style(self, rank):
        """
        Show top ethnic user group of each unique body style, for all the body styles involved in crashes

        Parameters:
        ----------
        rank: rank of the ethnic group

        Returns:
        ---------------------
        None
        """

        df_units=preprocess.drop_df_duplicates(self.df_units)
        df_crash_veh_styl_detail=df_units.groupby(['CRASH_ID','UNIT_NBR','VEH_BODY_STYL_ID'])\
                                         .count()\
                                         .select(col("CRASH_ID"),col("UNIT_NBR"),col("VEH_BODY_STYL_ID"))
        
        window = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        df=df_crash_veh_styl_detail.join(self.df_primary_person, on=['CRASH_ID','UNIT_NBR'], how='inner')\
                    .filter(~df_crash_veh_styl_detail.VEH_BODY_STYL_ID.isin(["NA", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)", "UNKNOWN"]))\
                    .filter(~self.df_primary_person.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"]))\
                    .groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count() \
                    .withColumn("row", row_number().over(window)).filter(col("row") == rank).drop("row", "count")
        
        df.show(truncate=False)

    def get_top_n_zipcodes_crashes_due_to_alcohol(self, n_count):
        """
        Return the Top 5 Zip Codes with the highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)

        Parameters:
        ----------
        n_count: top n zipcodes

        Returns:
        ---------------------
        List of zipcodes
        """
        df_units=preprocess.drop_df_duplicates(self.df_units)
        df = df_units.join(self.df_primary_person, on=['CRASH_ID','UNIT_NBR'], how='inner')\
                                .dropna(subset=["DRVR_ZIP"]) \
                                .filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")) \
                                .groupby("DRVR_ZIP")\
                                .agg(countDistinct("CRASH_ID")) \
                                .withColumnRenamed("count(CRASH_ID)","CRASH_ID_COUNT")\
                                .orderBy(col("CRASH_ID_COUNT").desc()).limit(n_count)
        

        return [row[0] for row in df.collect()]

    def get_count_of_crashes_with_insurance_and_no_damage(self):
        """
        Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance

        Parameters:
        ----------

        Returns:
        ---------------------
        dataframe count
        """
        df_damages=preprocess.drop_df_duplicates(self.df_damages)
        df_units=preprocess.drop_df_duplicates(self.df_units)
        df_No_Damages= df_damages.filter(col("DAMAGED_PROPERTY").contains("NONE"))
    
        df = df_No_Damages.join(df_units, on=["CRASH_ID"], how='inner') \
            .filter(
                    ((df_units.VEH_DMAG_SCL_1_ID > "DAMAGED 4") & (~df_units.VEH_DMAG_SCL_1_ID.isin(["NA", "INVALID VALUE"]))) | 
                    ((df_units.VEH_DMAG_SCL_2_ID > "DAMAGED 4") & (~df_units.VEH_DMAG_SCL_2_ID.isin(["NA", "INVALID VALUE"])))
                ) \
            .filter(col("FIN_RESP_TYPE_ID").contains("INSURANCE"))\
            .filter(col("VEH_BODY_STYL_ID").contains("CAR"))\
            .agg(countDistinct("CRASH_ID")) \
            .withColumnRenamed("count(CRASH_ID)","CRASH_ID_COUNT")

        return df.first().CRASH_ID_COUNT
    
    def get_top_n_vehicle_colours_with_crashes(self, input_df, n_count):
        """
        Find top n vehicle colours from the input dataframe involved in crashes

        Parameters:
        ----------
        input_df: input dataframe
        n_count: top n vehicle colours

        Returns:
        ---------------------
        List of Vehicle colours
        """

        return [row[0] for row in input_df.filter(input_df.VEH_COLOR_ID != "NA")\
                                        .groupby("VEH_COLOR_ID")\
                                        .agg(countDistinct("CRASH_ID","UNIT_NBR")) \
                                        .withColumnRenamed("count(CRASH_ID, UNIT_NBR)","count")\
                                        .orderBy(col("count").desc()).limit(n_count).collect()]
    
    def get_top_n_state_with_offences(self, input_df, n_count):
        """
        Find top n states from the input dataframe with offences

        Parameters:
        ----------
        input_df: input dataframe
        n_count: top n state

        Returns:
        ---------------------
        List of States
        """
        
        return [row[0] for row in input_df.filter(col("VEH_LIC_STATE_ID").cast("int").isNull()).groupby("VEH_LIC_STATE_ID")\
                                            .agg(countDistinct("CRASH_ID","UNIT_NBR","CITATION_NBR")) \
                                            .withColumnRenamed("count(CRASH_ID, UNIT_NBR, CITATION_NBR)","count")\
                                            .orderBy(col("count").desc()).limit(n_count).collect()]
    

    def get_top_n_vehicle_brand_with_conditions(self, n_state, n_vehicle_colours, n_vehicle_brands):
        """
        Find the top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences, has licensed drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences
        Parameters:
        ----------
        n_state: top n state
        n_vehicle_colours: top n vehicle colours
        n_vehicle_brands top n vehicle brands

        Returns:
        ---------------------
        List of Vehicle Brands
        """
        df_charges=preprocess.drop_df_duplicates(self.df_charges)
        df_units=preprocess.drop_df_duplicates(self.df_units)

        df_units_charges= df_charges.join(df_units, on=['CRASH_ID','UNIT_NBR'], how='inner')\
                                    .select(col("CRASH_ID"),col("UNIT_NBR"),col("VEH_COLOR_ID"),col("VEH_LIC_STATE_ID"),col("VEH_MAKE_ID"),col("CITATION_NBR"),
                                            col("CHARGE"),col("PRSN_NBR"))
        top_n_state_with_offences= self.get_top_n_state_with_offences(df_units_charges,n_state)
        top_n_vehicle_colours=self.get_top_n_vehicle_colours_with_crashes(df_units,n_vehicle_colours)
        
        df_units_charges_for_licensed_person=self.df_primary_person.join(df_units_charges, on=['CRASH_ID','UNIT_NBR','PRSN_NBR'], how='inner')\
                                                                    .filter(df_units_charges.CHARGE.contains("SPEED")) \
                                                                    .filter(self.df_primary_person.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]))\
                                                                    .filter(df_units_charges.VEH_COLOR_ID.isin(top_n_vehicle_colours)) \
                                                                    .filter(df_units_charges.VEH_LIC_STATE_ID.isin(top_n_state_with_offences))\
                                                                    .groupby("VEH_MAKE_ID") \
                                                                    .agg(countDistinct("CRASH_ID","UNIT_NBR"))\
                                                                    .withColumnRenamed("count(CRASH_ID, UNIT_NBR)","count")\
                                                                    .orderBy(col("count").desc()).limit(n_vehicle_brands)
        
        return [row[0] for row in df_units_charges_for_licensed_person.collect()]