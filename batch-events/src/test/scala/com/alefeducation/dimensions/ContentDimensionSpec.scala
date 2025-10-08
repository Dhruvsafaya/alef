package com.alefeducation.dimensions

import com.alefeducation.bigdata.batch.delta.DeltaSink
import com.alefeducation.bigdata.commons.testutils.{ExpectedFields, SparkSuite}
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.types.{DateType, TimestampType}

class ContentDimensionSpec extends SparkSuite with BaseDimensionSpec {

  import ExpectedFields._

  test("Test empty DataFrame case") {
    implicit val transformer = new ContentDimension(ContentDimensionName, spark)

    val fixtures = List(
      SparkFixture(key = ParquetContentCreatedSink, value = ""),
      SparkFixture(key = ParquetContentUpdatedSink, value = ""),
      SparkFixture(key = ParquetContentPublishedSink, value = ""),
      SparkFixture(key = ParquetContentDeletedSink, value = "")
    )

    withSparkFixtures(fixtures, sinks => assert(sinks.isEmpty))
  }

  test("Test content Created/Updated/Published/Deleted Events ") {
    implicit val transformer = new ContentDimension(ContentDimensionName, spark)
    val fixtures = List(
      SparkFixture(
        key = ParquetContentCreatedSink,
        value =
          """
            |[
            |{"eventType":"ContentCreatedEvent","loadtime":"2020-04-15T08:43:00.357Z","contentId":29546,"title":"Samvel Test Content","description":"Samvel Test Content","tags":"Samvel Test Content,samvel,content,test","organisation":"shared","fileName":"PR.jpg","fileContentType":"image/jpeg","fileSize":55733,"fileUpdatedAt":"2020-04-15T08:43:00.000Z","conditionOfUse":"MIT Open","knowledgeDimension":"CONCEPTUAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L3","mediaType":"IMAGE","status":1,"contentLocation":"2e/e2/b8/2a/29546","authoredDate":"2020-04-15","createdAt":1586940180172,"createdBy":33,"contentLearningResourceTypes":["DOK1"],"cognitiveDimensions":["UNDERSTANDING","REMEMBERING"],"copyrights":["ALEF","SHUTTERSTOCK","UAE_NATIONAL_ARCHIVES"],"occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415"},
            |{"eventType":"ContentCreatedEvent","loadtime":"2020-04-15T08:51:32.608Z","contentId":29547,"title":"CCLevents Test","description":"CCLevents Test","tags":"sam","organisation": "test-2","fileName":"PR.jpg","fileContentType":"image/jpeg","fileSize":55733,"fileUpdatedAt":"2020-04-15T08:51:32.000Z","conditionOfUse":"asdas","knowledgeDimension":"CONCEPTUAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L2","mediaType":"IMAGE","status":1,"contentLocation":"3a/7c/99/a3/29547","authoredDate":"2020-04-15","createdAt":1586940692502,"createdBy":33,"contentLearningResourceTypes":["DOK1"],"cognitiveDimensions":["UNDERSTANDING","APPLYING","REMEMBERING"],"copyrights":["UAE_NATIONAL_ARCHIVES"],"occurredOn":"2020-04-15 08:51:32.601","eventDateDw":"20200415"},
            |{"eventType":"ContentCreatedEvent","loadtime":"2020-04-15T08:53:31.996Z","contentId":29548,"title":"v","description":"CCLevents Test","tags":"as","organisation": "test-3","fileName":"Linux-file-system-hierarchy-Linux-file-structure-optimized.jpg","fileContentType":"image/jpeg","fileSize":111933,"fileUpdatedAt":"2020-04-15T08:53:31.000Z","conditionOfUse":"","knowledgeDimension":"CONCEPTUAL","difficultyLevel":"MEDIUM","language":"ARABIC","lexicalLevel":"L2","mediaType":"IMAGE","status":1,"contentLocation":"d3/35/89/e1/29548","authoredDate":"2020-04-15","createdAt":1586940811927,"createdBy":33,"contentLearningResourceTypes":["DOK2"],"cognitiveDimensions":["UNDERSTANDING"],"copyrights":[],"occurredOn":"2020-04-15 08:53:31.995","eventDateDw":"20200415"},
            |{"eventType":"ContentCreatedEvent","loadtime":"2020-04-15T12:18:30.547Z","contentId":29549,"title":"Test va1","description":"Test va1","tags":"sam,anton","organisation": "test-4","fileName":"PR.jpg","fileContentType":"image/jpeg","fileSize":55733,"fileUpdatedAt":"2020-04-15T12:18:30.000Z","conditionOfUse":"asdasd","knowledgeDimension":"CONCEPTUAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L2","mediaType":"IMAGE","status":1,"contentLocation":"af/20/aa/8c/29549","authoredDate":"2020-04-15","createdAt":1586953110483,"createdBy":33,"contentLearningResourceTypes":["DOK3"],"cognitiveDimensions":["UNDERSTANDING","REMEMBERING"],"copyrights":["UAE_NATIONAL_ARCHIVES"],"occurredOn":"2020-04-15 12:18:30.547","eventDateDw":"20200415"},
            |{"eventType":"ContentCreatedEvent","loadtime":"2020-04-15T13:52:56.599Z","contentId":29550,"title":"myt tetes","description":"my descrp","tags":"ks,asd","organisation": "test-5","fileName":"PR.jpg","fileContentType":"image/jpeg","fileSize":55733,"fileUpdatedAt":"2020-04-15T13:52:56.000Z","conditionOfUse":"szdasd","knowledgeDimension":"PROCEDURAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L3","mediaType":"IMAGE","status":1,"contentLocation":"ba/f0/0f/20/29550","authoredDate":"2020-04-15","createdAt":1586958776530,"createdBy":33,"contentLearningResourceTypes":["DOK3"],"cognitiveDimensions":["APPLYING"],"copyrights":["UAE_NATIONAL_ARCHIVES","ALEF"],"occurredOn":"2020-04-15 13:52:56.597","eventDateDw":"20200415"},
            |{"eventType":"ContentCreatedEvent","loadtime":"2020-04-16T07:27:25.540Z","contentId":29551,"title":"Test4","description":"Test4","tags":"sam","organisation": "test-created","fileName":"Linux-file-system-hierarchy-Linux-file-structure-optimized.jpg","fileContentType":"image/jpeg","fileSize":111933,"fileUpdatedAt":"2020-04-16T07:27:25.000Z","conditionOfUse":"Test4","knowledgeDimension":"CONCEPTUAL","difficultyLevel":"MEDIUM","language":"ARABIC","lexicalLevel":"L2","mediaType":"IMAGE","status":1,"contentLocation":"4d/60/a3/85/29551","authoredDate":"2020-04-16","createdAt":1587022045424,"createdBy":33,"contentLearningResourceTypes":["DOK1"],"cognitiveDimensions":["ANALYZING","UNDERSTANDING"],"copyrights":["SHUTTERSTOCK"],"occurredOn":"2020-04-16 07:27:25.539","eventDateDw":"20200416"}
            |]
            |""".stripMargin
      ),
      SparkFixture(
        key = ParquetContentUpdatedSink,
        value =
          """
            |[
            |{"eventType":"ContentUpdatedEvent","loadtime":"2020-04-15T05:20:18.978Z","contentId":29530,"title":"test_zip","description":"test_zipa2","tags":"test_zip","organisation": "org","fileName":"DOK2_1.zip","fileContentType":"application/zip","fileSize":5080741,"fileUpdatedAt":"2020-03-17T12:39:33.000Z","conditionOfUse":"","knowledgeDimension":"FACTUAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L1","mediaType":"HTML","status":4,"contentLocation":"56/62/0d/45/29530","authoredDate":"2020-03-17","updatedAt":1586928018894,"contentLearningResourceTypes":["DOK1"],"cognitiveDimensions":["REMEMBERING"],"copyrights":[],"occurredOn":"2020-04-15 05:20:18.896","eventDateDw":"20200415"},
            |{"eventType":"ContentUpdatedEvent","loadtime":"2020-04-15T05:21:36.269Z","contentId":29530,"title":"test_zip","description":"test_zipa","tags":"test_zip","organisation": "test updated org","fileName":"DOK2_1.zip","fileContentType":"application/zip","fileSize":5080741,"fileUpdatedAt":"2020-03-17T12:39:33.000Z","conditionOfUse":"","knowledgeDimension":"FACTUAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L1","mediaType":"HTML","status":4,"contentLocation":"56/62/0d/45/29530","authoredDate":"2020-03-17","updatedAt":1586928096267,"contentLearningResourceTypes":["DOK1"],"cognitiveDimensions":["REMEMBERING"],"copyrights":[],"occurredOn":"2020-04-15 05:21:36.267","eventDateDw":"20200415"},
            |{"eventType":"ContentUpdatedEvent","loadtime":"2020-04-15T05:21:56.525Z","contentId":29536,"title":"pt5","description":"pt5","tags":"pt5","organisation": "test updated org","fileName":"2019_AE_MA6_MLO_146_VD_001_AR.mp4","fileContentType":"video/mp4","fileSize":10966558,"fileUpdatedAt":"2020-03-19T08:31:45.000Z","conditionOfUse":"","knowledgeDimension":"CONCEPTUAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L10","mediaType":"VIDEO","status":4,"contentLocation":"24/6f/d8/97/29536","authoredDate":"2020-03-19","updatedAt":1586928116521,"contentLearningResourceTypes":["METACOGNITIVE_PROMPT"],"cognitiveDimensions":["ANALYZING"],"copyrights":[],"occurredOn":"2020-04-15 05:21:56.521","eventDateDw":"20200415"},
            |{"eventType":"ContentUpdatedEvent","loadtime":"2020-04-15T08:51:44.368Z","contentId":29547,"title":"CCLevents Test","description":"CCLevents Test","tags":"sam","organisation": "test updated org","fileName":"PR.jpg","fileContentType":"image/jpeg","fileSize":55733,"fileUpdatedAt":"2020-04-15T08:51:32.000Z","conditionOfUse":"asdas","knowledgeDimension":"CONCEPTUAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L2","mediaType":"IMAGE","status":4,"contentLocation":"3a/7c/99/a3/29547","authoredDate":"2020-04-15","updatedAt":1586940704329,"contentLearningResourceTypes":["DOK1"],"cognitiveDimensions":["APPLYING","REMEMBERING","UNDERSTANDING"],"copyrights":["UAE_NATIONAL_ARCHIVES"],"occurredOn":"2020-04-15 08:51:44.330","eventDateDw":"20200415"},
            |{"eventType":"ContentUpdatedEvent","loadtime":"2020-04-15T09:20:13.059Z","contentId":29548,"title":"v2","description":"CCLevents Test","tags":"as","organisation": "test updated org","fileName":"Linux-file-system-hierarchy-Linux-file-structure-optimized.jpg","fileContentType":"image/jpeg","fileSize":111933,"fileUpdatedAt":"2020-04-15T08:53:31.000Z","conditionOfUse":"","knowledgeDimension":"CONCEPTUAL","difficultyLevel":"MEDIUM","language":"ARABIC","lexicalLevel":"L2","mediaType":"IMAGE","status":1,"contentLocation":"d3/35/89/e1/29548","authoredDate":"2020-04-15","updatedAt":1586942413054,"contentLearningResourceTypes":["DOK2"],"cognitiveDimensions":["UNDERSTANDING"],"copyrights":[],"occurredOn":"2020-04-15 09:20:13.054","eventDateDw":"20200415"},
            |{"eventType":"ContentUpdatedEvent","loadtime":"2020-04-15T12:12:14.674Z","contentId":29548,"title":"v3","description":"CCLevents Test","tags":"as","organisation": "test updated org","fileName":"Linux-file-system-hierarchy-Linux-file-structure-optimized.jpg","fileContentType":"image/jpeg","fileSize":111933,"fileUpdatedAt":"2020-04-15T08:53:31.000Z","conditionOfUse":"","knowledgeDimension":"CONCEPTUAL","difficultyLevel":"MEDIUM","language":"ARABIC","lexicalLevel":"L2","mediaType":"IMAGE","status":1,"contentLocation":"d3/35/89/e1/29548","authoredDate":"2020-04-15","updatedAt":1586952734671,"contentLearningResourceTypes":["DOK2"],"cognitiveDimensions":["UNDERSTANDING"],"copyrights":[],"occurredOn":"2020-04-15 12:12:14.671","eventDateDw":"20200415"},
            |{"eventType":"ContentUpdatedEvent","loadtime":"2020-04-15T12:14:47.459Z","contentId":29548,"title":"v4","description":"CCLevents Test","tags":"as","organisation": "test updated org","fileName":"Linux-file-system-hierarchy-Linux-file-structure-optimized.jpg","fileContentType":"image/jpeg","fileSize":111933,"fileUpdatedAt":"2020-04-15T08:53:31.000Z","conditionOfUse":"","knowledgeDimension":"CONCEPTUAL","difficultyLevel":"MEDIUM","language":"ARABIC","lexicalLevel":"L2","mediaType":"IMAGE","status":1,"contentLocation":"d3/35/89/e1/29548","authoredDate":"2020-04-15","updatedAt":1586952887457,"contentLearningResourceTypes":["DOK2"],"cognitiveDimensions":["UNDERSTANDING"],"copyrights":[],"occurredOn":"2020-04-15 12:14:47.457","eventDateDw":"20200415"},
            |{"eventType":"ContentUpdatedEvent","loadtime":"2020-04-15T14:42:47.844Z","contentId":29550,"title":"myt tete","description":"my descrp","tags":"ks,asd","organisation": "test updated org","fileName":"PR.jpg","fileContentType":"image/jpeg","fileSize":55733,"fileUpdatedAt":"2020-04-15T13:52:56.000Z","conditionOfUse":"szdasd","knowledgeDimension":"PROCEDURAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L3","mediaType":"IMAGE","status":1,"contentLocation":"ba/f0/0f/20/29550","authoredDate":"2020-04-15","updatedAt":1586961767841,"contentLearningResourceTypes":["DOK3"],"cognitiveDimensions":["APPLYING"],"copyrights":["UAE_NATIONAL_ARCHIVES","ALEF"],"occurredOn":"2020-04-15 14:42:47.841","eventDateDw":"20200415"},
            |{"eventType":"ContentUpdatedEvent","loadtime":"2020-04-15T14:43:25.321Z","contentId":29550,"title":"myt tetes","description":"my descrp","tags":"ks,asd","organisation": "test updated org","fileName":"PR.jpg","fileContentType":"image/jpeg","fileSize":55733,"fileUpdatedAt":"2020-04-15T13:52:56.000Z","conditionOfUse":"szdasd","knowledgeDimension":"PROCEDURAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L3","mediaType":"IMAGE","status":1,"contentLocation":"ba/f0/0f/20/29550","authoredDate":"2020-04-15","updatedAt":1586961805320,"contentLearningResourceTypes":["DOK3"],"cognitiveDimensions":["APPLYING"],"copyrights":["UAE_NATIONAL_ARCHIVES","ALEF"],"occurredOn":"2020-04-15 14:43:25.320","eventDateDw":"20200415"},
            |{"eventType":"ContentUpdatedEvent","loadtime":"2020-04-15T14:56:53.523Z","contentId":29550,"title":"myt tetes","description":"my descr","tags":"ks,asd","organisation": "test updated org","fileName":"PR.jpg","fileContentType":"image/jpeg","fileSize":55733,"fileUpdatedAt":"2020-04-15T13:52:56.000Z","conditionOfUse":"szdasd","knowledgeDimension":"PROCEDURAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L3","mediaType":"IMAGE","status":1,"contentLocation":"ba/f0/0f/20/29550","authoredDate":"2020-04-15","updatedAt":1586962613522,"contentLearningResourceTypes":["DOK3"],"cognitiveDimensions":["APPLYING"],"copyrights":["UAE_NATIONAL_ARCHIVES","ALEF"],"occurredOn":"2020-04-15 14:56:53.522","eventDateDw":"20200415"},
            |{"eventType":"ContentUpdatedEvent","loadtime":"2020-04-15T14:58:21.121Z","contentId":29550,"title":"myt tetesc","description":"my descr","tags":"ks,asd","organisation": "test updated org","fileName":"PR.jpg","fileContentType":"image/jpeg","fileSize":55733,"fileUpdatedAt":"2020-04-15T13:52:56.000Z","conditionOfUse":"szdasd","knowledgeDimension":"PROCEDURAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L3","mediaType":"IMAGE","status":1,"contentLocation":"ba/f0/0f/20/29550","authoredDate":"2020-04-15","updatedAt":1586962701120,"contentLearningResourceTypes":["DOK3"],"cognitiveDimensions":["APPLYING"],"copyrights":["UAE_NATIONAL_ARCHIVES","ALEF"],"occurredOn":"2020-04-15 14:58:21.120","eventDateDw":"20200415"},
            |{"eventType":"ContentUpdatedEvent","loadtime":"2020-04-15T15:15:14.409Z","contentId":29550,"title":"myt tetesc","description":"my descrp","tags":"ks,asd","organisation": "test updated org","fileName":"PR.jpg","fileContentType":"image/jpeg","fileSize":55733,"fileUpdatedAt":"2020-04-15T13:52:56.000Z","conditionOfUse":"szdasd","knowledgeDimension":"PROCEDURAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L3","mediaType":"IMAGE","status":1,"contentLocation":"ba/f0/0f/20/29550","authoredDate":"2020-04-15","updatedAt":1586963714406,"contentLearningResourceTypes":["DOK3"],"cognitiveDimensions":["APPLYING"],"copyrights":["ALEF","UAE_NATIONAL_ARCHIVES"],"occurredOn":"2020-04-15 15:15:14.406","eventDateDw":"20200415"},
            |{"eventType":"ContentUpdatedEvent","loadtime":"2020-04-16T06:42:22.013Z","contentId":29550,"title":"myt tetes","description":"my descrp","tags":"ks,asd","organisation": "test updated org","fileName":"PR.jpg","fileContentType":"image/jpeg","fileSize":55733,"fileUpdatedAt":"2020-04-15T13:52:56.000Z","conditionOfUse":"szdasd","knowledgeDimension":"PROCEDURAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L3","mediaType":"IMAGE","status":1,"contentLocation":"ba/f0/0f/20/29550","authoredDate":"2020-04-15","updatedAt":1587019342010,"contentLearningResourceTypes":["DOK3"],"cognitiveDimensions":["APPLYING"],"copyrights":["UAE_NATIONAL_ARCHIVES","ALEF"],"occurredOn":"2020-04-16 06:42:22.010","eventDateDw":"20200416"},
            |{"eventType":"ContentUpdatedEvent","loadtime":"2020-04-16T07:25:59.638Z","contentId":29550,"title":"myt tetesscp","description":"my descrp","tags":"ks,asd","organisation": "test updated org","fileName":"PR.jpg","fileContentType":"image/jpeg","fileSize":55733,"fileUpdatedAt":"2020-04-15T13:52:56.000Z","conditionOfUse":"szdasd","knowledgeDimension":"PROCEDURAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L3","mediaType":"IMAGE","status":1,"contentLocation":"ba/f0/0f/20/29550","authoredDate":"2020-04-15","updatedAt":1587021959635,"contentLearningResourceTypes":["DOK3"],"cognitiveDimensions":["APPLYING"],"copyrights":["ALEF","UAE_NATIONAL_ARCHIVES"],"occurredOn":"2020-04-16 07:25:59.635","eventDateDw":"20200416"},
            |{"eventType":"ContentUpdatedEvent","loadtime":"2020-04-16T07:45:45.060Z","contentId":29550,"title":"myt tetesscp","description":"my descrp","tags":"ks,asd","organisation": "test updated org","fileName":"PR.jpg","fileContentType":"image/jpeg","fileSize":55733,"fileUpdatedAt":"2020-04-15T13:52:56.000Z","conditionOfUse":"szdasd","knowledgeDimension":"PROCEDURAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L3","mediaType":"IMAGE","status":4,"contentLocation":"ba/f0/0f/20/29550","authoredDate":"2020-04-15","updatedAt":1587023145042,"contentLearningResourceTypes":["DOK3"],"cognitiveDimensions":["APPLYING"],"copyrights":["ALEF","UAE_NATIONAL_ARCHIVES"],"occurredOn":"2020-04-16 07:45:45.042","eventDateDw":"20200416"},
            |{"eventType":"ContentUpdatedEvent","loadtime":"2020-04-16T17:51:25.387Z","contentId":29550,"title":"myt tetesscp","description":"my descrp","tags":"ks,asd","organisation": "test updated org","fileName":"PR.jpg","fileContentType":"image/jpeg","fileSize":55733,"fileUpdatedAt":"2020-04-15T13:52:56.000Z","conditionOfUse":"szdasd","knowledgeDimension":"PROCEDURAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L3","mediaType":"IMAGE","status":4,"contentLocation":"ba/f0/0f/20/29550","authoredDate":"2020-04-15","updatedAt":1587059485374,"contentLearningResourceTypes":["DOK3"],"cognitiveDimensions":["APPLYING"],"copyrights":["UAE_NATIONAL_ARCHIVES","ALEF"],"occurredOn":"2020-04-16 17:51:25.374","eventDateDw":"20200416"}
            |]
            |""".stripMargin
      ),
      SparkFixture(
        key = ParquetContentPublishedSink,
        value =
          """
            |[
            |{"eventType":"ContentPublishedEvent","loadtime":"2020-04-15T08:51:44.374Z","contentId":29547,"publishedDate":"2020-04-15","publisherId":33,"publisherName":"Fahim","occurredOn":"2020-04-15 08:51:44.367","eventDateDw":"20200415"},
            |{"eventType":"ContentPublishedEvent","loadtime":"2020-04-16T07:45:45.060Z","contentId":29550,"publishedDate":"2020-04-16","publisherId":33,"publisherName":"Fahim","occurredOn":"2020-04-16 07:45:45.059","eventDateDw":"20200416"}
            |]
            |""".stripMargin
      ),
      SparkFixture(
        key = ParquetContentDeletedSink,
        value =
          """
            |[
            |{"eventType":"ContentDeletedEvent","loadtime":"2020-04-15T08:48:34.409Z","contentId":29546,"occurredOn":"2020-04-15 08:48:34.397","eventDateDw":"20200415"},
            |{"eventType":"ContentDeletedEvent","loadtime":"2020-04-15T15:22:41.848Z","contentId":29549,"occurredOn":"2020-04-15 15:22:41.846","eventDateDw":"20200415"},
            |{"eventType":"ContentDeletedEvent","loadtime":"2020-04-16T07:45:32.218Z","contentId":29551,"occurredOn":"2020-04-16 07:45:32.216","eventDateDw":"20200416"}
            |]
            |""".stripMargin
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        val commonColsRS =
          Seq("content_created_time", "content_dw_created_time", "content_updated_time", "content_deleted_time", "content_dw_updated_time")
        val colToExcludeFromRS =
          Seq("content_description", "content_copyrights", "occurredOn")
        val expectedCreatedColsRS = ContentCreatedDimensionCols.values.toSeq.diff(colToExcludeFromRS) ++ commonColsRS
        val expectedUpdatedColsRS = ContentDimensionCols.values.toSeq.diff(colToExcludeFromRS) ++ commonColsRS
        val expectedPublishedColsRS = ContentPublishedDimensionCols.values.toSeq.diff(Seq("occurredOn")) ++ commonColsRS
        val expectedDeletedColsRS = ContentDeletedDimensionCols.values.toSeq.diff(Seq("occurredOn")) ++ commonColsRS

        val commonColsPQ = Seq("eventDateDw", "eventType", "eventdate", "loadtime")
        val expectedCreatedColsPQ = ContentCreatedDimensionCols.keys.toSeq ++ commonColsPQ
        val expectedUpdatedColsPQ = ContentDimensionCols.keys.toSeq ++ commonColsPQ ++ Seq("updatedAt")
        val expectedPublishedColsPQ = ContentPublishedDimensionCols.keys.toSeq ++ commonColsPQ :+ "publisherName"
        val expectedDeletedColsPQ = ContentDeletedDimensionCols.keys.toSeq ++ commonColsPQ

        testSinkBySinkName(sinks, ParquetContentCreatedSink, expectedCreatedColsPQ, 6)
        testSinkBySinkName(sinks, ParquetContentUpdatedSink, expectedUpdatedColsPQ, 16)
        testSinkBySinkName(sinks, ParquetContentPublishedSink, expectedPublishedColsPQ, 2)
        testSinkBySinkName(sinks, ParquetContentDeletedSink, expectedDeletedColsPQ, 3)

        testSinkBySinkName(sinks, RedshiftContentCreatedSink, expectedCreatedColsRS, 6)
        testSinkBySinkName(sinks, RedshiftContentUpdatedSink, expectedUpdatedColsRS, 5) // 5 ot of 15 because take only last records
        testSinkBySinkName(sinks, RedshiftContentPublishedSink, expectedPublishedColsRS, 2)
        testSinkBySinkName(sinks, RedshiftContentDeletedSink, expectedDeletedColsRS, 3)

        testSinkBySinkName(
          sinks,
          DeltaContentCreatedSink,
          expectedCreatedColsRS ++
            Seq(
              "content_published_date",
              "content_publisher_id",
              "content_description",
              "content_copyrights"
            ),
          6
        )

        val deltaCreatedSink = sinks.collectFirst { case s: DeltaSink if s.name == DeltaContentCreatedSink => s }.get
        val deltaCreatedSinkDF = deltaCreatedSink.output.cache()
        val deltaCreateExpectedFields = List(
          ExpectedField(name = "content_published_date", dataType = DateType)
        )
        assertExpectedFields(deltaCreatedSinkDF.schema.fields.filter(_.name == "content_published_date").toList, deltaCreateExpectedFields)
        testSinkBySinkName(
          sinks,
          DeltaContentUpdatedSink,
          expectedUpdatedColsRS ++ Seq("content_description",
                                       "content_copyrights"),
          5
        )
        testSinkBySinkName(sinks, DeltaContentPublishedSink, expectedPublishedColsRS, 2)
        testSinkBySinkName(sinks, DeltaContentDeletedSink, expectedDeletedColsRS, 3)

      }
    )

  }

  test("Test handleColsForDeleteEvt function") {
    val spark2 = spark
    import spark2.implicits._

    val transformer = new ContentDimension(ContentDimensionName, spark)
    val df = Seq((999, "testValue")).toDF("number", "word")
    val resultDF = df.transform(transformer.handleColsForDeleteEvt)

    resultDF.columns should contain("content_status")
    resultDF.first.getAs[Int]("content_status") shouldBe 4

  }

  test("Test handleColsForPublishEvt function") {
    val spark2 = spark
    import spark2.implicits._

    val transformer = new ContentDimension(ContentDimensionName, spark)
    val df = Seq((999, "testValue", "2020-04-15")).toDF("number", "word", "publishedDate")
    val resultDF = df.transform(transformer.handleColsForPublishEvt)

    resultDF.dtypes(2)._2 shouldBe "DateType"
  }

  test("Test handleColsForCreateEvt function") {
    val spark2 = spark
    import spark2.implicits._

    val transformer = new ContentDimension(ContentDimensionName, spark)
    val df = Seq((999, "testValue", "1586940180172")).toDF("number", "word", "createdAt")
    val resultDF = df.transform(transformer.handleColsForCreateEvt).cache

    resultDF.columns should contain("content_status")
    resultDF.first.getAs[Int]("content_status") shouldBe 1
    assertExpectedFields(resultDF.schema.fields.filter(_.name == "createdAt").toList,
                         List(
                           ExpectedField(name = "createdAt", dataType = TimestampType)
                         ))
  }

  test("Test transformTimeAndStatus function") {
    val spark2 = spark
    import spark2.implicits._

    val transformer = new ContentDimension(ContentDimensionName, spark)
    val df = Seq(
      ("DRAFT", "2020-04-15", "1586940180172"),
      ("PUBLISHED", "2020-04-16", "1586940180172"),
      ("RE-PUBLISHED", "2020-04-16", "1586940180172"),
      ("UNKNOWN_VALUE", "2020-04-20", "1586940180172")
    ).toDF("status", "authoredDate", "fileUpdatedAt")

    val resultDF = df.transform(transformer.transformTimeAndStatus).cache

    resultDF.dtypes(1)._2 shouldBe "DateType"
    resultDF.select("status").collect().map(_(0)).toSeq shouldBe Seq(1, 4, 5, 999)

    //    resultDF.first.getAs("fileUpdatedAt").toString shouldBe "2020-04-15 12:43:00.0"
  }

}
