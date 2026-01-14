from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.sql.types import*
from pyspark.sql.window import Window 
from threshold import threshold_value
spark=SparkSession.builder.appName("firstSequence").getOrCreate()
input=""
marketFilePath=""
df=spark.read.options(header="true",inferSchema="true").csv(input)
minQtyThreshold=threshold_value["minQtyThreshold"]
TickSize= threshold_value0["TickSize"]
minTickProximty= threshold_value["minTickProximty"]
maxTickProximaty= threshold_value["maxTickProximaty"]
ReductionStartOffset=threshold_value["ReductionStartOffset"]
ReductionDuration=threshold_value["ReductionDuration"]
minOrderReductionThreshold=threshold_value["minOrderReductionThreshold"]
minTickReEntranceProximity=threshold_value["minTickReEntranceProximity"]
maxTickReEntraceProximity=threshold_value["maxTickReEntraceProximity"]
minTickChange=threshold_value["minTickChange"]
epsilon=threshold_value["epsilon"]
reEntranceWindowDuration=threshold_value["reEntranceWindowDuration"]
minReEntryQuantity=threshold_value["minReEntryQuantity"]
MinAvgTickSpread=threshold_value["MinAvgTickSpread"]
MinMaxTickSpread=threshold_value["MinMaxTickSpread"]
df_order=spark.read.options(header="true",inferSchema="true").csv(marketFilePath)
df_order=df_order.withColumn("TickSpread",((col("BestAsk")-col("BestBid"))/(TickSize)))\
                 .withColumn("OrderTimeUnix",unix_timestamp(col("Timestamp")))
MaxTickSpread=df_order.agg(max("TickSpread"))
AverageTickSpread=df_order.agg(avg("TickSpread"))
#ReductionWndow=1minuteAfterTrigger
#lookBackWindow=60second Before Trigger
#1Creating the dataframe with active records  & sells only and adding unixTimestamp.
df_activep=df.withColumn("UnixTimeStamp",unix_timestamp(col("Timestamp")))\
            .filter((col("Event_Type").isin("New","Cancel","Modify"))\
            &(col("Side").isin("Sell")))\
            .orderBy(col("UnixTimestamp"))\
            .withColumn("AdjustedQty",when(col("Event_Type")=="New",col("Quantity"))\
            .when(col("Event_Type")=="Modify",col("Quantity"))\
            .when(col("Event_Type")=="Cancel",(-1)*col("Quantity")))
df_acive=df_activep.join(df_order,on=(df_order.OrderTimeUnix=df_acive.UnixTimeStamp),how='inner')
#2.00 Calculating the First Trigger point and finding the Pnear And Pfar
df_trigger1=df_acive.filter(col("Quantity")>minQtyThreshold)
Ptrigger=df_trigger1.select(col("Price")).first()[0]
Ttrigger=df_trigger1.select(col("UnixTimeStamp")).first()[0]
Pnear=Ptrigger+minTickProximaty*TickSize
Pfar=Ptrigger+maxTickProximaty*TickSize
#3.00 Filtering the from records falling under Pnear and Pfar and calculating total sum.
df_trigger=df_active.filter((col("Price").between(Pnear,Pfar))) 
cumulativeQuantityUnderTriggerWindow=df_trigger.agg(sum("Quantity")).first()[0]
df_trigger=df_trigger.withColumn("CumD1",lit(DoubleType(cumulativeQuantityUnderTriggerWindow)))
if cumulativeQuantityUnderTriggerWindow>=minQtyThreshold:
    #Now finding the ReductionWindow for cummulativeReductionQty
    reductionStartWindowTimeStamp=Ttrigger-ReductionStartOffset
    reductionEndWindowTimeStamp=Ttrigger+ReductionDuration
    df2_reduction=df_trigger.filter(col("UnixTimeStamp")\
                  .between(reductionStartWindowTimeStamp,reductionEndWindowTimeStamp))
    df2CummulativeQty=df2_reduction.agg(sum("Quantity"))
    df2_reduction=df2_reduction.withColumn("cumD2",lit(DoubleType(df2CummulativeQty)))
    total_reductionFraction=(cumulativeQuantityUnderTriggerWindow-df2CummulativeQty)\
                            /(cumulativeQuantityUnderTriggerWindow)
    if total_reductionFraction >=minOrderReductionThreshold:
        reEntranceStartWindowTimeStamp=reductionEndWindowTimeStamp+epsilon
        reEntranceEndWindowTimeStam=reEntranceStartWindowTimeStamp+reEntranceWindowDuration
        df3_reEntry=df2_reduction.filter(col("UnixTimeStamp")\
                    .between(reEntranceStartWindowTimeStamp,reEntranceEndWindowTimeStam))
        df3CummulativeQty=df3_reEntry.agg(sum("Quantity"))
        df3_reEntry=df3_reEntry.withColumn("CumD3",lit(DoubleType(df3CummulativeQty)))
        total_ReEntryFraction=(df3CummulativeQty)/(cumulativeQuantityUnderTriggerWindow)
        if ((AvgTickSpread < MinAvgTickSpread)|(MaxTickSpread < MinMaxTickSpread))\
            & (total_ReEntryFraction<minReEntryQuantity):
            lookBackdf=df_activep.filter(col("UnixTimeStamp").between((Ttrigger-60000),Ttrigger))
            avgLookBackTickSpread=lookBackdf.agg(avg("TickSpread")).first()[0]
            look95Percentile=lookBackdf.approxQuantile("TickSpread",[0.95],0.01).first()[0]                             
            lookBackMultiplier=(MaxTickSpread/look95Percentile)
            if AverageTickSpread==avgLookBackTickSpread*lookBackMultiplier:
                print("Trigger Alert for Sequential Best Ask-Best Bid Abnormal Alert")
                df3_reEntry.show()




            





