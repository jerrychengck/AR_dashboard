# Databricks notebook source
#Insert PySpark librarires to notebook
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC #Date Variables

# COMMAND ----------

#pull a year worth of dates due to Wine being a 52 week dashboard

current_date = to_date(current_date())
end_date = expr("date_sub(to_date(current_date()), dayofweek(current_date()) -1)")
start_date = date_sub(end_date, 364)
end_date_py = date_sub(end_date, 364)
start_date_py = date_sub(start_date, 364)

# COMMAND ----------

# MAGIC %md
# MAGIC #Adding Full Franchise 
# MAGIC #####(Date: 04-02-2025)

# COMMAND ----------

full_franchise_vw = spark.read.table('db_general.om_site_info_general')
full_franchise_vw = full_franchise_vw.filter(col('division_desc').isin('9500 - TMC Full Franchise POS','9600 - Northern Tier - Full Franch POS'))\
                                      .select('site_number', 'Region_ID', 'division_desc', 'BU_simplified_name').distinct()

# COMMAND ----------

# MAGIC %md 
# MAGIC #Setup Tables

# COMMAND ----------

# DATE DATA (1 year of data due to Wine being a 52 week report)
calendar_vw = spark.read.table('dl_edw_na.calendar_date').filter(col('calendar_date').between(start_date_py,end_date))\
                                                         .join(spark.read.table('dl_edw_na.fiscal_week'), on = ['fiscal_week_key'], how = 'inner')\
                                                         .join(spark.read.table('dl_edw_na.fiscal_period'), on = ['fiscal_period_key'], how = 'inner')\
                                                         .select('calendar_date','fiscal_week_end_date','fiscal_year_key','fiscal_week_number')\
                                                         .withColumnRenamed('calendar_date','business_date')\
                                                         .withColumnRenamed('fiscal_week_end_date','week_end_date')\
                                                         .withColumnRenamed('fiscal_year_key','fiscal_year')\
                                                         .withColumnRenamed('fiscal_week_number','fiscal_week')\
                                                         .withColumn('fiscal_year_week',concat_ws('-','fiscal_year','fiscal_week'))\
                                                         .withColumn('same_day_ly',date_sub('business_date',364))\
                                                         .withColumn('same_week_end_ly',date_sub('week_end_date',364))

brand_vw = spark.read.table('db_general.ar_dashboard_brand_mapping').withColumnRenamed('Category', 'unified_category')

site_fixture_vw = spark.read.table('db_general.ar_dashboard_site_fixture')                                    

# COMMAND ----------

age_restricted_categories = ['Cigarettes', 'Instant Lottery Sales', 'Online Lotto Sales', 'Liquor', 'Wine','Other Tobacco Products', 'Beer', 'E-Cigs Juul PODS', 'E-Cigs Juul Devices']

edw = spark.read.table('db_curated.edw_sales_vw').alias('edw')
ff = full_franchise_vw.alias('ff')

#mapping BU from om_site_info table on site_number
sales_cy_vw = edw.join(ff, (col('edw.site_number') == col('ff.site_number')) & (col('edw.region') == col('ff.Region_ID')), 'left')\
                         .withColumn('business_unit', when((col('edw.business_unit') == 'Unknown') &
                                                            (col('edw.site_number') == col('ff.site_number')),
                                                            col('ff.BU_simplified_name')).otherwise(col('edw.business_unit')))\
                         .filter(
                              (col('edw.unified_category').isin(*age_restricted_categories)) &
                              (col('edw.business_date').between(start_date, end_date)) &
                              (col('business_unit') != 'Unknown')
                         )\
                         .withColumn('item_UPC_desc', concat_ws('||', col('edw.upc'), col('edw.item_desc')))\
                         .groupBy(
                              col('edw.site_number'), col('edw.country'), col('edw.state'), col('business_unit'), col('edw.region'),
                              col('edw.market'), col('edw.latitude'), col('edw.longitude'), col('edw.week_end_date'), col('edw.fiscal_year'),
                              col('edw.fiscal_week'), col('edw.unified_department'), col('edw.unified_category'),
                              col('edw.unified_sub_category'), col('edw.department_desc'), col('edw.category_desc'),
                              col('edw.sub_category_desc'), col('edw.product_key'), col('item_UPC_desc'), col('edw.item_desc'),
                              trim(col('edw.brand')).alias('brand'), col('edw.manufacturer'), col('edw.size_desc'), col('edw.package_qty'),
                              col('edw.environment_name'), col('edw.promotion_key'), col('edw.promotion_id'), col('edw.promotion_desc'),
                              col('edw.promotion_type')
                         )\
                         .agg(
                              sum(col('edw.single_units')).alias('units_cy'),
                              sum(col('edw.sales_usd')).alias('sales_cy')
                         )\
                         .withColumnRenamed('environment_name', 'sys_environment_name')\
                         .withColumn('units_py', lit(0))\
                         .withColumn('sales_py', lit(0))


sales_py_full_vw = edw.join(ff, (col('edw.site_number') == col('ff.site_number')) & (col('edw.region') == col('ff.Region_ID')), 'left')\
                         .withColumn('business_unit', when((col('edw.business_unit') == 'Unknown') &
                                                            (col('edw.site_number') == col('ff.site_number')),
                                                            col('ff.BU_simplified_name')).otherwise(col('edw.business_unit')))\
                         .filter((col('unified_category').isin(*age_restricted_categories))&
                                                               (col('business_date').between(start_date_py, end_date_py)) &
                                                               (col('business_unit') != 'Unknown'))\
                         .withColumn('units_cy',lit(0))\
                         .withColumn('sales_cy',lit(0))

sales_py_vw = sales_py_full_vw.join(calendar_vw.alias("calendar_vw"), ([sales_py_full_vw.business_date == calendar_vw.same_day_ly]), how = 'inner')\
                              .withColumn('item_UPC_desc', concat_ws('||', 'upc', 'item_desc'))\
                              .groupBy('edw.site_number','country','state','business_unit','region','market','latitude','longitude',
                                       'calendar_vw.week_end_date', 'calendar_vw.fiscal_year', 
                                       'calendar_vw.fiscal_week', 'unified_department','unified_category','unified_sub_category','department_desc','category_desc','sub_category_desc',
                                       'product_key', 'item_upc_desc', 'item_desc', trim(col('brand')).alias('brand'),'manufacturer','size_desc','package_qty','environment_name', 
                                       'promotion_key', 'promotion_id', 'promotion_desc', 'promotion_type', 'units_cy', 'sales_cy')\
                              .agg(sum('package_qty').alias('units_py'),
                                   sum('sales_usd').alias('sales_py'))\
                              .withColumnRenamed('environment_name','sys_environment_name')\
                              
union_vw = sales_cy_vw.union(sales_py_vw)

full_vw = union_vw.groupBy('site_number', 'country', 'state', 'business_unit', 'region','market', 'latitude','longitude', 'week_end_date',
                           'fiscal_year','fiscal_week','unified_department', 'unified_category', 'unified_sub_category','department_desc',
                           'category_desc','sub_category_desc','product_key', 'item_UPC_desc', 'item_desc', 'brand','manufacturer','size_desc', 
                           'package_qty','sys_environment_name', 'promotion_key', 'promotion_id', 'promotion_desc', 'promotion_type')\
                  .agg(sum('units_cy').alias('units_cy'),
                       sum('units_py').alias('units_py'),
                       sum('sales_cy').alias('sales_cy'),
                       sum('sales_py').alias('sales_py'))\
                  .withColumn('brand_desc', when (col('unified_category') == 'Beer', col('manufacturer'))
                                           .otherwise(col('brand')))
                  
#Logic that identifies same_site
                       #Return Touch Button Sales
same_site_vw = full_vw.withColumn('tb_sales_cy', when(col('item_desc').like('%TB LIQUOR%'),col('sales_cy')).otherwise(0))\
                      .withColumn('tb_sales_py',when(col('item_desc').like('%TB LIQUOR%'),col('sales_py')).otherwise(0))\
                      .groupBy('site_number', 'week_end_date', 'unified_category', 'unified_sub_category')\
                      .agg(sum('units_cy').alias('units_cy'),
                           sum('units_py').alias('units_py'),
                           sum('sales_cy').alias('sales_cy'),
                           sum('tb_sales_cy').alias('tb_sales_cy'),  
                           sum('sales_py').alias('sales_py'),   
                           sum('tb_sales_py').alias('tb_sales_py'))\
                      .withColumn('tb_prcnt_cy',(col('tb_sales_cy') / col('sales_cy')))\
                      .withColumn('tb_prcnt_py',(col('tb_sales_py') / col('sales_py')))\
                      .fillna(0)\
                      .withColumn('liquor_site_cy', when((col('sales_cy') > 0) & # site has Liquor Sales
                                                         (col('tb_prcnt_cy') != 1) & # 100% of sales cannot come from touch button
                                                         # TB percent can be between 20 and 75% if sales are above 100, otherwise tb percent needs to be below 20%
                                                         (((col('tb_prcnt_cy').between(0.20,0.75)) & (col('sales_cy') >= 100)) | (col('tb_prcnt_cy') < 0.20)), 1)
                                                   .otherwise(0))\
                      .withColumn('liquor_site_py', when((col('sales_py') > 0) &
                                                         (col('tb_prcnt_py') != 1) &
                                                         (((col('tb_prcnt_py').between(0.20,0.75)) & (col('sales_py') >= 100)) | (col('tb_prcnt_py') < 0.20)), 1)
                                                   .otherwise(0))\
                      .withColumn('same_site', when((col('unified_category') == 'Liquor'),
                                                          when((col('liquor_site_cy') == 1) & (col('liquor_site_py') == 1), 'Same Site')
                                                          .when(col('liquor_site_cy') == 1, 'CY Only')
                                                          .when(col('liquor_site_py') == 1, 'PY Only')
                                                          .otherwise(lit('exclude')))
                                               .otherwise(when((col('units_cy') != 0) & (col('units_py') != 0), 'Same Site')
                                                     .when((col('units_cy') != 0) & (col('units_py') == 0), 'CY Only')
                                                     .otherwise('PY Only')))\
                      .filter(col('same_site') != 'exclude')\
                      .select('site_number', 'week_end_date', 'unified_sub_category', 'same_site')                         

final_vw = full_vw.join(brand_vw, on = (['brand_desc', 'country', 'unified_category']), how = 'left')\
                  .join((site_fixture_vw), on = (['site_number']), how = 'left')\
                  .join((same_site_vw), on = (['week_end_date', 'site_number', 'unified_sub_category']))\
                  .fillna('Other', subset = 'brand_group')\
                  .fillna('No', subset= ['wine_fixture', 'pb_rack', 'breezer_cooler', 'standing_cooler'])\
                  .fillna('None', subset= 'fixture_group')\
                  .withColumn('fiscal_year_week',concat_ws('|','fiscal_year','fiscal_week'))\
                  .withColumn('site_week_key',concat_ws('|','site_number','week_end_date'))

# COMMAND ----------

# MAGIC %md
# MAGIC #Write Final Tables

# COMMAND ----------

# Set float types to decimal(9,2)
site_date_cols =   ["site_week_key","site_number","week_end_date", "unified_category"]
edw_product_cols = ["product_key", "unified_department", "unified_category", "unified_sub_category","sub_category_desc",
                    'manufacturer', "brand_group","brand","size_desc","package_qty","item_UPC_desc"]
site_cols =        ["site_number","business_unit","region","market","latitude","longitude","state","country",
                    "wine_fixture","pb_rack","fixture_group","breezer_cooler","standing_cooler", "unified_category"]
date_cols =        ["week_end_date","fiscal_year","fiscal_week","fiscal_year_week"]
promo_cols =       ["promotion_key","promotion_id","promotion_desc","promotion_type"]

# COMMAND ----------

# Replaces the arguments for .withColumn(x, col(x).cast(DecimalType(9,2))) into more readable code, returning a tuple. Make sure to unpack it with a * when using
# 04022025 adding FF 
decimal = lambda x: (x, col(x).cast(DecimalType(9,2)))

edw_products_table = final_vw.select(*edw_product_cols).distinct()\
                             .withColumn(*decimal('package_qty'))

site_table_13 =      final_vw.filter((col('week_end_date').between(date_sub(current_date, 91), current_date)) &
                                     (col('unified_category') != 'Wine'))\
                             .select(*site_cols+['units_cy','units_py'])\
                             .groupBy(*site_cols)\
                             .agg(sum('units_cy').alias('units_cy'),
                                  sum('units_py').alias('units_py'))\
                             .withColumn('same_site', when((col('units_cy') != 0) & (col('units_py') != 0), 'Same Site')
                                                     .when((col('units_cy') != 0) & (col('units_py') == 0), 'CY Only')
                                                     .otherwise('PY Only'))\
                             .select(*site_cols+["same_site"])\
                             .join(full_franchise_vw, on = (['site_number']), how = 'left')\
                              .withColumn("Franchise_Flag", when(col('division_desc').isin('9500 - TMC Full Franchise POS', 
                                                                                           '9600 - Northern Tier - Full Franch POS'), "Y").otherwise("N"))\
                              .select(*site_cols, "same_site", "Franchise_Flag")\
                             .distinct()


site_table_52 =      final_vw.filter((col('unified_category') == 'Wine'))\
                             .select(*site_cols+['units_cy','units_py'])\
                             .groupBy(*site_cols)\
                             .agg(sum('units_cy').alias('units_cy'),
                                  sum('units_py').alias('units_py'))\
                             .withColumn('same_site', when((col('units_cy') != 0) & (col('units_py') != 0), 'Same Site')
                                                     .when((col('units_cy') != 0) & (col('units_py') == 0), 'CY Only')
                                                     .otherwise('PY Only'))\
                             .select(*site_cols+["same_site"])\
                              .join(full_franchise_vw, on = (['site_number']), how = 'left')\
                              .withColumn("Franchise_Flag", when(col('division_desc').isin('9500 - TMC Full Franchise POS', 
                                                                                           '9600 - Northern Tier - Full Franch POS'), "Y").otherwise("N"))\
                             .select(*site_cols, "same_site", "Franchise_Flag")\
                             .distinct()

site_table =         site_table_13.union(site_table_52)

date_table =         final_vw.select(*date_cols).distinct()

sw1 = final_vw.select("site_number").distinct()\
              .crossJoin(final_vw.select("week_end_date").distinct())\
              .crossJoin(final_vw.select("unified_category").distinct())
sw2 = final_vw.groupby("site_number")\
              .agg(min(col("week_end_date")).alias("site_min_date"),max(col("week_end_date")).alias("site_max_date"))

site_week_table = sw1.join((sw2), on = (['site_number']), how = 'left')\
                     .filter((col("week_end_date") >= col("site_min_date"))&(col("week_end_date") <= col("site_max_date")))\
                     .withColumn('site_week_key',concat_ws('|','site_number','week_end_date'))\
                     .select(*site_date_cols)

promo_table = final_vw.select(*promo_cols).distinct()

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions","auto")

# COMMAND ----------

edw_products_table.write\
                  .format('delta')\
                  .mode('overwrite')\
                  .option('overwriteSchema', 'true')\
                  .saveAsTable('db_general.ar_dashboard_product')

# COMMAND ----------

site_table.write\
          .format('delta')\
          .mode('overwrite')\
          .option('overwriteSchema', 'true')\
          .saveAsTable('db_general.ar_dashboard_site')

# COMMAND ----------

date_table.write\
          .format('delta')\
          .mode('overwrite')\
          .option('overwriteSchema', 'true')\
          .saveAsTable('db_general.ar_dashboard_date')

# COMMAND ----------

site_week_table.write\
               .format('delta')\
               .mode('overwrite')\
               .option('overwriteSchema', 'true')\
               .saveAsTable('db_general.ar_dashboard_site_week')

# COMMAND ----------

promo_table.write\
           .format('delta')\
           .mode('overwrite')\
           .option('overwriteSchema', 'true')\
           .saveAsTable('db_general.ar_dashboard_promotion')
