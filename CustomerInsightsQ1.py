from datetime import datetime
from generic import sc
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.types import StructField, IntegerType, StringType, StructType
from Coding_Questions.CustomerInsights import RDDParser as rp

rdd_cust = sc .textFile("data/Customer.txt")
rdd_prod = sc .textFile("data/Product.txt")
rdd_sales = sc .textFile("data/Sales.txt")
rdd_refund = sc .textFile("data/Refund.txt")

scc = SparkSession.builder.appName("CodingQuestion").getOrCreate()
rdd_cust_mapped = rdd_cust.map(rp.parseLineCustomer)
rdd_prod_mapped = rdd_prod.map(rp.parseLineProduct)
rdd_sales_mapped = rdd_sales.map(rp.parseLineSales)
rdd_refund_mapped = rdd_refund.map(rp.parseLineRefund)

df_customer = scc.createDataFrame(rdd_cust_mapped, rp.getCustomerColumns())
df_product = scc.createDataFrame(rdd_prod_mapped, rp.getProductColumns())
df_sales = scc.createDataFrame(rdd_sales_mapped, rp.getSalesColumns())
df_refund = scc.createDataFrame(rdd_refund_mapped, rp.getRefundColumns())

# df_sp_join = df_sales.join(df_product,on=["ProductID"])
# df_gb = df_sp_join.groupBy(["ProductID", "ProductName","ProductType"])
# df_sum = df_gb.agg(sf.sum("TotalAmount").alias("TotalSum"))
# df_avg = df_gb.agg({"TotalAmount":"mean"}).withColumnRenamed("avg(TotalAmount)","TotalAvg")
df_refund_dt = df_refund.withColumn("RefundTimestamp", sf.to_timestamp("RefundTimestamp",'MM/dd/yyyy HH:mm:ss'))
df_sales_dt = df_sales.withColumn("Timestamp", sf.to_timestamp("Timestamp",'MM/dd/yyyy HH:mm:ss'))

# df_fil_refu = df_refund_dt.filter(sf.year(df_refund_dt.RefundTimestamp) == "2013")
df_fil_sales = df_sales_dt.filter((sf.year(df_sales_dt.Timestamp) == "2013") & (sf.month(df_sales_dt.Timestamp) == "5"))
# df_match = df_fil_sales.join(df_refund_dt, df_fil_sales.TransactionID == df_refund_dt.OrgTransactionID)

df_sort_desc_sales = df_fil_sales.groupBy("CustomerID").agg(sf.sum("TotalAmount").alias("TotalAmount")).orderBy("TotalAmount",ascending=False)
df_purchases = df_sort_desc_sales.limit(10)

df_customer.createOrReplaceTempView("CustDetails")
df_purchases.createOrReplaceTempView("CustPurchases")
df_sales_dt.createOrReplaceTempView("Sales")
df_product.createOrReplaceTempView("Products")

# print(scc.sql("select CustPurchases.CustomerID, CustDetails.CustomerFirstName, CustPurchases.TotalAmount from CustPurchases JOIN CustDetails ON CustPurchases.CustomerID == CustDetails.CustomerID").show())

# df_prod_sales = df_product.join(df_sales_dt, df_product.ProductID == df_sales_dt.TransactionID, how="inner")
# print(df_prod_sales.show())
#
# print(scc.sql("select * from Products where ProductID NOT IN (select ProductID from Sales)").show())
givendate = "2013-03-02"

df_sales_calc = df_sales_dt.filter(sf.to_date(df_sales_dt.Timestamp) == givendate)

print(df_sales_calc.groupBy(["CustomerID", "ProductID","Timestamp"]).agg(sf.count("ProductID").alias("Count_PrdID")).orderBy("Count_PrdID",descending=True).show())