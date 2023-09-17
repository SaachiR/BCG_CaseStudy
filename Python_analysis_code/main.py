import pandas as pd

parent_path='C:/Users/Asus/OneDrive/Desktop/BCG_test_python'

df_Charges = pd.read_csv( parent_path +'/Data/Charges_use.csv')
df_Damages = pd.read_csv( parent_path +'/Data/Damages_use.csv')
df_Endorse = pd.read_csv( parent_path +'/Data/Endorse_use.csv')
df_Primary_Person = pd.read_csv( parent_path +'/Data/Primary_Person_use.csv')
df_Units = pd.read_csv( parent_path +'/Data/Units_use.csv')
df_Restrict = pd.read_csv( parent_path +'/Data/Restrict_use.csv')

# print(df_Primary_Person.columns) 

# 1. Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
# print(df_Primary_Person[['CRASH_ID','PRSN_GNDR_ID']].head(5))
# print(df_Primary_Person[['CRASH_ID','UNIT_NBR']].duplicated().sum()) --find the granularity of the data
# print(df_Primary_Person.sort_values('CRASH_ID').head(5))
Primary_Person_Male_Accident_count=df_Primary_Person[df_Primary_Person['PRSN_GNDR_ID']=='MALE'][['CRASH_ID']].nunique()
print(Primary_Person_Male_Accident_count)

# 2. Analysis 2: How many two wheelers are booked for crashes?
# print(len(df_Units)-(df_Units.duplicated().sum())) 
# print(df_Units.duplicated().sum())
# print(df_Units[df_Units.duplicated()==True].head(5))
df_Units_without_duplicates=df_Units.drop_duplicates()
# print(len(df_Units_without_duplicates))
# print(len(df_Units))
# print(df_Units_without_duplicates[df_Units_without_duplicates[['CRASH_ID','UNIT_NBR','VEH_DMAG_AREA_1_ID', 'VEH_DMAG_AREA_2_ID']].duplicated()==True].head(5))
Two_wheeler_crash_count=df_Units_without_duplicates[df_Units_without_duplicates['VEH_BODY_STYL_ID'].str.contains("MOTORCYCLE",na=False)]['CRASH_ID'].count()
print(Two_wheeler_crash_count)

# 3. Analysis 3: Which state has highest number of accidents in which females are involved?
print(df_Primary_Person[df_Primary_Person['PRSN_GNDR_ID']=='FEMALE'][['CRASH_ID','DRVR_LIC_STATE_ID']].groupby('DRVR_LIC_STATE_ID').nunique().nlargest(1,'CRASH_ID',keep='all'))

# 4. Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death

df_VEH_MAKE_IDs_with_Death_Injury=(df_Units[df_Units['DEATH_CNT']>0].groupby('VEH_MAKE_ID')['TOT_INJRY_CNT']
                                                             .sum()
                                                             .rank(method='first',ascending=False).astype('int32')
                                                             .to_frame().reset_index()
                                                             .rename(columns={'TOT_INJRY_CNT':'RNK_INJRY_CNT'})
                                                             )

top_5th_to_15th_VEH_MAKE_IDs=(df_VEH_MAKE_IDs_with_Death_Injury[(df_VEH_MAKE_IDs_with_Death_Injury['RNK_INJRY_CNT']>=5) & (df_VEH_MAKE_IDs_with_Death_Injury['RNK_INJRY_CNT']<=15)]['VEH_MAKE_ID'].to_list())                                                                                                           
print(top_5th_to_15th_VEH_MAKE_IDs)

# 5. Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
df_crash_veh_make_detail=(df_Units_without_duplicates.groupby(['CRASH_ID','UNIT_NBR','VEH_BODY_STYL_ID']).count().reset_index()[['CRASH_ID','UNIT_NBR','VEH_BODY_STYL_ID']])
df_crash_veh_make_ethnicity_detail=pd.merge(df_Primary_Person,df_crash_veh_make_detail,on=['CRASH_ID','UNIT_NBR'],how='inner')
df_crash_veh_make_ethnicity_agg_detail=df_crash_veh_make_ethnicity_detail.groupby(['VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID'])\
                                                                         .count()['CRASH_ID'].reset_index()\
                                                                         .rename(columns={'CRASH_ID':'CRASH_ID_COUNT'})
df_crash_veh_make_ethnicity_agg_detail['ethnicity_rnk']=df_crash_veh_make_ethnicity_agg_detail.sort_values('CRASH_ID_COUNT',ascending=False).groupby(['VEH_BODY_STYL_ID']).cumcount()+1
print(df_crash_veh_make_ethnicity_agg_detail[df_crash_veh_make_ethnicity_agg_detail['ethnicity_rnk']==1][['VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID']].to_string())

# 6. Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash 
# (Use Driver Zip Code)
df_crash_due_to_alcohol_detail=(df_Units_without_duplicates[(df_Units_without_duplicates['CONTRIB_FACTR_1_ID'].str.contains("ALCOHOL",na=False)) |
                                                      (df_Units_without_duplicates['CONTRIB_FACTR_2_ID'].str.contains("ALCOHOL",na=False)) ]
                          .groupby(['CRASH_ID','UNIT_NBR']).count().reset_index()[['CRASH_ID','UNIT_NBR']])
df_crash_due_to_alcohol_zipcode_detail=pd.merge(df_Primary_Person,df_crash_due_to_alcohol_detail,on=['CRASH_ID','UNIT_NBR'],how='inner')
df=(df_crash_due_to_alcohol_zipcode_detail[['DRVR_ZIP','CRASH_ID']].groupby('DRVR_ZIP').nunique().nlargest(5,'CRASH_ID',keep='all'))
print(df.to_string())

# 7. Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
df_Damages_without_duplicates=df_Damages.drop_duplicates()
df_no_damages=df_Damages_without_duplicates[(df_Damages_without_duplicates['DAMAGED_PROPERTY'].str.contains('NONE',na=False))]
df_Units_without_duplicates_fil=df_Units_without_duplicates[(df_Units_without_duplicates['VEH_BODY_STYL_ID'].str.contains('CAR',na=False)) &
                            (df_Units_without_duplicates['FIN_RESP_TYPE_ID'].str.contains('INSURANCE',na=False)) &
                            (
                                (df_Units_without_duplicates['VEH_DMAG_SCL_1_ID']>'DAMAGED 4') | (df_Units_without_duplicates['VEH_DMAG_SCL_2_ID']>'DAMAGED 4') |
                                (df_Units_without_duplicates['VEH_DMAG_SCL_1_ID']=="NO DAMAGE") | (df_Units_without_duplicates['VEH_DMAG_SCL_2_ID']>"NO DAMAGE")  
                            )]
df_units_no_damage_detail=pd.merge(df_Units_without_duplicates_fil,df_no_damages,on=['CRASH_ID'],how='inner')
print(df_units_no_damage_detail['CRASH_ID'])

# 8. Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers,
#  used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)


df_Charges_without_duplicates = df_Charges.groupby(['CRASH_ID','UNIT_NBR','CHARGE','PRSN_NBR']).count().reset_index()
df_Units_without_duplicates=df_Units_without_duplicates.groupby(['CRASH_ID','UNIT_NBR','VEH_COLOR_ID','VEH_LIC_STATE_ID','VEH_MAKE_ID']).count().reset_index()
df_units_charges=pd.merge(df_Units_without_duplicates, df_Charges_without_duplicates,on=['CRASH_ID','UNIT_NBR'],how='inner')
top_25_state_with_offences=(df_units_charges.groupby('VEH_LIC_STATE_ID')['CRASH_ID'].count().reset_index().nlargest(25,'CRASH_ID',keep='all')['VEH_LIC_STATE_ID'].to_list())
top_10_vehicle_colours=(df_Units_without_duplicates.groupby('VEH_COLOR_ID')['CRASH_ID'].count().reset_index().nlargest(10,'CRASH_ID',keep='all')['VEH_COLOR_ID'].to_list())

df_licensed_person=df_Primary_Person[df_Primary_Person['DRVR_LIC_TYPE_ID'].isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])][['CRASH_ID','UNIT_NBR',"PRSN_NBR"]]\
                                                                          .drop_duplicates(subset=['CRASH_ID','UNIT_NBR','PRSN_NBR'])

df_units_charges_final=pd.merge(df_units_charges,df_licensed_person,on=['CRASH_ID','UNIT_NBR',"PRSN_NBR"],how='inner')
df_units_charges_final_1=df_units_charges_final[(df_units_charges_final['VEH_COLOR_ID'].isin(top_10_vehicle_colours)) 
                                        & (df_units_charges_final['VEH_LIC_STATE_ID'].isin(top_25_state_with_offences)) 
                                        & (df_units_charges_final['CHARGE'].str.contains('SPEED',na=False))]

df_units_charges_final_1_agg=df_units_charges_final_1[['VEH_MAKE_ID','CRASH_ID','UNIT_NBR']].groupby(['VEH_MAKE_ID','CRASH_ID','UNIT_NBR']).count().reset_index()\
                                                                                            .groupby('VEH_MAKE_ID').agg(total_vehicle_count=('VEH_MAKE_ID','count'))\
                                                                                            .nlargest(10,'total_vehicle_count',keep='all')

print(df_units_charges_final_1_agg)
