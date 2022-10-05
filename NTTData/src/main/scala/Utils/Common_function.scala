package Utils

import org.apache.spark.sql.functions.{current_timestamp, date_format}

import java.text.SimpleDateFormat
import java.util.Date

class Common_function(var format_date:Date) {

  var isDate: Boolean = false;
  val format1 = new SimpleDateFormat("yyyy-MM-dd")

  def format_dates(): Unit ={
    if (isDate){
       return format_date
    }
    else{
      return format1.format(format_date)
    }
    isDate= true;
  }

}
