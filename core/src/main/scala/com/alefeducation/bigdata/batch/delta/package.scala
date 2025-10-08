package com.alefeducation.bigdata.batch

import com.alefeducation.util.DataFrameUtility
import org.apache.spark.sql.DataFrame

package object delta {

  object Alias {

    val Delta = "delta"

    val Events = "events"

  }

  object Columns {

    val EventDateDw = "eventDateDw"

    val EventDate = "eventdate"

    val DateDwId = "date_dw_id"

    val OccurredOn = "occurredOn"

    val LoadTime = "loadtime"

    val EventType = "event_type"

  }

  object Builder {

    val DefaultWithoutColumns: List[String] = Columns.LoadTime :: Columns.EventType :: Nil

    val DefaultUniqueIdColumns: List[String] = "uuid" :: Nil

    val DefaultImmutableColumns: List[String] = Columns.OccurredOn :: Columns.EventDate :: Nil

    implicit class Transformations(df: DataFrame) {

      import com.alefeducation.bigdata.batch.BatchUtils._

      def withoutColumns(withoutColumns: List[String] = Nil): DataFrame = {
        val without = withoutColumns ++ Builder.DefaultWithoutColumns
        df.drop(without: _*)
      }

      def flatten: DataFrame = DataFrameUtility.flatten(df)

      def withRenamedEventDateColumn: DataFrame = df.withColumnRenamed(Columns.EventDateDw, Columns.EventDate)

      def withEventDateColumn(isFact: Boolean): DataFrame = {
        import df.sparkSession.implicits._
        import org.apache.spark.sql.functions._

        if (isFact) {
          df.withColumn(Columns.EventDate, date_format($"occurredOn", "yyyy-MM-dd"))
            .withColumn(Columns.DateDwId, $"${Columns.EventDate}")
        } else df.drop($"${Columns.EventDate}")

      }

      def withStatusColumn(isFact: Boolean): DataFrame = {
        if (isFact) df
        else df.addStatusColumn()
      }

    }

  }

}
