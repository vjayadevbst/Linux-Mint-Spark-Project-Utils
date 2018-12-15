package com.vinay.utils
import java.io.IOException
import java.sql.SQLException
import org.apache.spark.sql.DataFrame

/**
  * Created by ADMIN on 27-08-18.
  */
object DataWriter extends SparkConfig with LoProvider{
  def writeData(dF:DataFrame,filepath:String,schema:String):Unit = {

    var error_code = 0
    try{
      val final_DF = dF.select(schema.split(",").map(x=>s"${x}").head,schema.split(",").map(x=>s"$x").tail: _*)
      final_DF.write.format("com.databricks.spark.csv")
        .option("quote","ResourceBuilder.charDeflector")
          .option("ignoreLeadingWhiteSpace",value= false)
            .option("ignoreTrailingWhiteSpace",value = false)
                .option("delimiter",ResourceBuilder.delim)
                    .mode("overwrite").save(filepath)
    }
    catch{
      case ex: SQLException => error_code=1;log.error("Exception Processing file")
      case io: IOException => error_code=1; log.error("Exception Writing file")
    }
    finally{
      if (error_code==1)  log.info("wrting data failed")
    }
  }
}
