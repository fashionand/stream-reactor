package com.datamountaineer.streamreactor.connect.cassandra.source

import java.net.InetAddress
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util
import java.util.UUID

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraConfigConstants, CassandraConfigSource, CassandraSettings}
import com.datastax.driver.core.{CodecRegistry, _}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import org.apache.kafka.connect.data.{Decimal, Schema, SchemaBuilder, Struct, Timestamp}
import org.apache.kafka.connect.errors.DataException
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

class TestCassandraTypeConverter extends WordSpec
  with TestConfig
  with Matchers
  with MockitoSugar {

  val OPTIONAL_DATE_SCHEMA = org.apache.kafka.connect.data.Date.builder().optional().build()
  val OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().build()
  val OPTIONAL_DECIMAL_SCHEMA = Decimal.builder(18).optional().build()
  val uuid = UUID.randomUUID()

  val codecRegistry: CodecRegistry = new CodecRegistry();

  "should handle null when converting a Cassandra row schema to a Connect schema" in {
    val cols = TestUtils.getColumnDefs
    val row = mock[Row]
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)

    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(false))

    val json="{\"studentkey\":\"34630506\",\"product\":2,\"productmodule\":1,\"businesskey\":\"e32b7a85-cd56-4b68-9480-18aaad68d3f9|a50ff232-5b21-4501-9374-5ca0dd848140|699984c7-2416-40bb-9a87-175c91dee827|9c090031-5cca-4eae-846b-5dbba8d4b894\",\"resultkey\":\"3b67a5c0-2d64-11eb-99ca-f12361c64815\",\"actualscore\":14.0,\"createdby\":null,\"details\":[{\"activitykey\":\"9c090031-5cca-4eae-846b-5dbba8d4b894\",\"activityversion\":\"20201123-065512-46e106b44588ccd9\",\"questionkey\":\"/content/adam/courses/highflyers/book-3/unit-2/activities/homework/hf-wordninja-activity10-vocabulary/a10q1/jcr:content/stimulusText\",\"questionversion\":null,\"answer\":null,\"expectedscore\":2.0,\"actualscore\":2.0,\"duration\":null,\"starttime\":null,\"endtime\":null,\"extension\":null},{\"activitykey\":\"9c090031-5cca-4eae-846b-5dbba8d4b894\",\"activityversion\":\"20201123-065512-46e106b44588ccd9\",\"questionkey\":\"/content/adam/courses/highflyers/book-3/unit-2/activities/homework/hf-wordninja-activity10-vocabulary/a10q2/jcr:content/stimulusText\",\"questionversion\":null,\"answer\":null,\"expectedscore\":2.0,\"actualscore\":2.0,\"duration\":null,\"starttime\":null,\"endtime\":null,\"extension\":null},{\"activitykey\":\"9c090031-5cca-4eae-846b-5dbba8d4b894\",\"activityversion\":\"20201123-065512-46e106b44588ccd9\",\"questionkey\":\"/content/adam/courses/highflyers/book-3/unit-2/activities/homework/hf-wordninja-activity10-vocabulary/a10q3/jcr:content/stimulusText\",\"questionversion\":null,\"answer\":null,\"expectedscore\":2.0,\"actualscore\":2.0,\"duration\":null,\"starttime\":null,\"endtime\":null,\"extension\":null},{\"activitykey\":\"9c090031-5cca-4eae-846b-5dbba8d4b894\",\"activityversion\":\"20201123-065512-46e106b44588ccd9\",\"questionkey\":\"/content/adam/courses/highflyers/book-3/unit-2/activities/homework/hf-wordninja-activity10-vocabulary/a10q4/jcr:content/stimulusText\",\"questionversion\":null,\"answer\":null,\"expectedscore\":2.0,\"actualscore\":2.0,\"duration\":null,\"starttime\":null,\"endtime\":null,\"extension\":null},{\"activitykey\":\"9c090031-5cca-4eae-846b-5dbba8d4b894\",\"activityversion\":\"20201123-065512-46e106b44588ccd9\",\"questionkey\":\"/content/adam/courses/highflyers/book-3/unit-2/activities/homework/hf-wordninja-activity10-vocabulary/a10q5/jcr:content/stimulusText\",\"questionversion\":null,\"answer\":null,\"expectedscore\":2.0,\"actualscore\":2.0,\"duration\":null,\"starttime\":null,\"endtime\":null,\"extension\":null},{\"activitykey\":\"9c090031-5cca-4eae-846b-5dbba8d4b894\",\"activityversion\":\"20201123-065512-46e106b44588ccd9\",\"questionkey\":\"/content/adam/courses/highflyers/book-3/unit-2/activities/homework/hf-wordninja-activity10-vocabulary/a10q6/jcr:content/stimulusText\",\"questionversion\":null,\"answer\":null,\"expectedscore\":2.0,\"actualscore\":2.0,\"duration\":null,\"starttime\":null,\"endtime\":null,\"extension\":null},{\"activitykey\":\"9c090031-5cca-4eae-846b-5dbba8d4b894\",\"activityversion\":\"20201123-065512-46e106b44588ccd9\",\"questionkey\":\"/content/adam/courses/highflyers/book-3/unit-2/activities/homework/hf-wordninja-activity10-vocabulary/a10q7/jcr:content/stimulusText\",\"questionversion\":null,\"answer\":null,\"expectedscore\":2.0,\"actualscore\":2.0,\"duration\":null,\"starttime\":null,\"endtime\":null,\"extension\":null}],\"duration\":0,\"endtime\":1606119406392,\"expectedscore\":14.0,\"extension\":\"{\\\"groupId\\\":\\\"100221391\\\"}\",\"route\":\"{\\\"treeRevision\\\":\\\"20201123-0746-5fbb68bb6577da38498aaadf\\\",\\\"bookContentId\\\":\\\"e32b7a85-cd56-4b68-9480-18aaad68d3f9\\\",\\\"bookContentRevision\\\":\\\"20201123-065512-46e106b44588ccd9\\\",\\\"unitContentId\\\":\\\"a50ff232-5b21-4501-9374-5ca0dd848140\\\",\\\"unitContentRevision\\\":\\\"20201123-065512-46e106b44588ccd9\\\",\\\"lessonContentId\\\":\\\"699984c7-2416-40bb-9a87-175c91dee827\\\",\\\"lessonContentRevision\\\":\\\"20201123-065512-46e106b44588ccd9\\\",\\\"learningUnitContentId\\\":\\\"9c090031-5cca-4eae-846b-5dbba8d4b894\\\",\\\"learningUnitContentRevision\\\":\\\"20201123-065512-46e106b44588ccd9\\\",\\\"parentContentPath\\\":\\\"highflyers/cn-3/book-3/unit-2/assignment-2\\\"}\",\"starttime\":1606119360723}"
    val node = JsonNodeFactory.instance.objectNode()
    val jsonNode=cassandraTypeConverter.mapper.readTree(json)
    val testNode=JsonNodeFactory.instance.pojoNode(json)

    val date=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(0)

    val detailsNode=jsonNode.get("details")
//    jsonNode.get("details").elements().remove()
//    node.set("details", JsonNodeFactory.instance.textNode(cassandraTypeConverter.mapper.writeValueAsString(detailsNode)))
    // jsonNode.asInstanceOf[ObjectNode].put("details", JsonNodeFactory.instance.textNode(cassandraTypeConverter.mapper.writeValueAsString(detailsNode)))

    //    testNode..put("details",JsonNodeFactory.instance.textNode(cassandraTypeConverter.mapper.writeValueAsString(detailsNode)))

   var timeStr="2019-08-15T04:11:08.847Z"
//    print(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(timeStr).getTime)
    var schemaBuilder=SchemaBuilder.struct();
    schemaBuilder.field("startTime",Schema.OPTIONAL_INT64_SCHEMA)
   var testSchema= schemaBuilder.build();
    val structItem = new Struct(testSchema)
    structItem.put("startTime",new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(timeStr).getTime)
    print(structItem.toString)

    var source = "[{\"activityKey\":\"3fa85f64-5717-4562-b3fc-2c963f66afa9\",\"activityVersion\":\"123\",\"questionKey\":null,\"questionVersion\":\"123\",\"answer\":{\"brand\":\"Nissan\",\"doors\":8},\"expectedScore\":null,\"actualScore\":1.0,\"duration\":0,\"startTime\":\"2019-08-15T03:11:08.847Z\",\"endTime\":\"2019-08-15T04:11:08.847Z\",\"extension\":[\"ab\",\"cd\"]}]";
    //    var response=JacksonWrapper.deserialize[AnswerModel](source)
    //    println ("obj= " + response)
    //    var response=cassandraTypeConverter.mapper.reader(source,Answers[].class)
    //    var response=cassandraTypeConverter.mapper.readValue[Array[Answers]](source, new TypeReference[Array[Answers]]() { })
    //    var response=Json.toListWithGeneric(source,classOf[AnswerCollectionModel])
    //    var response=Json.toListWithGeneric(source,classOf[AnswerCollectionModel])
    //    cassandraTypeConverter.mapper.readValue[Map[String, Any]](source)
    //    var response=cassandraTypeConverter.mapper.readValue[Map[String, Any]](source)
    val response = cassandraTypeConverter.mapper.readValue[Array[util.HashMap[String, String]]](source, classOf[Array[util.HashMap[String, String]]])
    val hashmap: util.HashMap[String, String] = response(0)
    hashmap.entrySet().foreach(subCol => {
      if (subCol != null)
        if(subCol.getValue==null)
          println(s"type:")
        else
          println(s"type: ${cassandraTypeConverter.mapJavaTypesToSchema(subCol.getValue.getClass.getTypeName)},key is ${subCol.getKey} is array ${subCol.getValue.getClass.getTypeName.contains("lang")}, value type is ${subCol.getValue.getClass.getTypeName} ")
      else
        println("type:" + subCol.getKey)
    }
    )
    //    for (subCol <- hashmap.keySet()) {
    //      if(hashmap.get(subCol)!=null) {
    //        println ("type:" + hashmap.get(subCol).getClass.getTypeName)
    //      }
    ////      DateType
    ////      new UserType(hashmap.get(subCol).getClass.getTypeName)
    //
    //    }
    //    var response=cassandraTypeConverter.mapper.readValue[Array[AnswerModel]](source, classOf[Array[AnswerModel]])
    println("obj= " + response)
    val schema = cassandraTypeConverter.convertToConnectSchema(row, null, "test")
    val schemaFields = schema.fields().asScala
    schemaFields.size shouldBe 0
    schema.name() shouldBe "test"
  }


  "should convert a Cassandra row schema to a Connect schema" in {
    val cols = TestUtils.getColumnDefs
    val row = mock[Row]
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)

    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(false))
    val schema = cassandraTypeConverter.convertToConnectSchema(row,cols.asScala.toList, "test")
    val testString2:String="route|extension|answer"
    val test=testString2.split("|").contains("extension")
    val testString:String="java.util.LinkedHashMap|{\"A\":1980,\"B\":\"CD\",\"C\":0}"
    val result= cassandraTypeConverter.removeSplitChar(testString)
    val schemaFields = schema.fields().asScala
    schemaFields.size shouldBe cols.asList().size()
    schema.name() shouldBe "test"
    checkCols(schema)
  }

  "should convert a Cassandra row to a Struct" in {
    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(false))
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)
    val colDefList = cassandraTypeConverter.getStructColumns(row, Set.empty)
    val sr: Struct = cassandraTypeConverter.convert(row, "test", colDefList, None)
    val schema = sr.schema()
    checkCols(schema)
    sr.get("timeuuidCol").toString shouldBe uuid.toString
    sr.get("intCol") shouldBe 0
    sr.get("mapCol") shouldBe ('empty)
  }

  "should convert a Cassandra row to a Struct with map in sub struct" in {
    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(false))
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)

    when(row.getMap("mapCol", classOf[String], classOf[String])).thenReturn(new java.util.HashMap[String,String] {
      put("sub1","sub1value");
    })

    val colDefList = cassandraTypeConverter.getStructColumns(row, Set.empty)
    val sr: Struct = cassandraTypeConverter.convert(row, "test", colDefList, None)
    val schema = sr.schema()
    checkCols(schema)

    sr.getMap("mapCol").get("sub1").toString shouldBe "sub1value"
  }

  "should convert a Cassandra row to a Struct with map in json" in {
    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(true))
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)

    when(row.getMap("mapCol", classOf[String], classOf[String])).thenReturn(new java.util.HashMap[String,String] {
      put("sub1","sub1value");
    })

    val colDefList = cassandraTypeConverter.getStructColumns(row, Set.empty)
    val sr: Struct = cassandraTypeConverter.convert(row, "test", colDefList, None)
    val schema = sr.schema()
    checkCols(schema)

    sr.get("mapCol") shouldBe "{\"sub1\":\"sub1value\"}"
  }

  "should convert a Cassandra row to a Struct with list in sub struct" in {
    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(false))
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)

    when(row.getList("listCol", classOf[String])).thenReturn(new java.util.ArrayList[String]{
      add("A");
      add("B");
      add("C");
    })

    val colDefList = cassandraTypeConverter.getStructColumns(row, Set.empty)
    val sr: Struct = cassandraTypeConverter.convert(row, "test", colDefList, None)
    val schema = sr.schema()
    checkCols(schema)

    sr.getArray("listCol").get(0).toString shouldBe "A"
    sr.getArray("listCol").get(1).toString shouldBe "B"
    sr.getArray("listCol").get(2).toString shouldBe "C"
  }

  "should convert a Cassandra row to a Struct with list in json" in {
    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(true))
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)

    when(row.getList("listCol", classOf[String])).thenReturn(new java.util.ArrayList[String]{
      add("A");
      add("B");
      add("C");
    })

    val colDefList = cassandraTypeConverter.getStructColumns(row, Set.empty)
    val sr: Struct = cassandraTypeConverter.convert(row, "test", colDefList, None)
    val schema = sr.schema()
    checkCols(schema)

    sr.get("listCol") shouldBe "[\"A\",\"B\",\"C\"]"
  }

  "should convert a Cassandra row to a Struct with set" in {
    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(false))
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)

    when(row.getSet("setCol", classOf[String])).thenReturn(new java.util.HashSet[String]{
      add("A");
      add("B");
      add("C");
    })
    if("route|extension|answer".split('|').contains("answer")){
     var test= cassandraTypeConverter.removeSplitChar("java.util.LinkedHashMap|{\"treeRevision\":\"TreeRevisionTest-qSLtiUgPXd\",\"schemaVersion\":1,\"courseContentId\":\"85c3bc30-351a-4670-8c98-58e7c8493c12\",\"bookContentId\":\"6697380d-6091-42a1-9be3-69afc9e960c2\",\"unitContentId\":\"7661c0a8-9841-4299-87a0-d72ebd2ed664\",\"lessonContentId\":\"44a39671-c478-4923-a386-6e4865c68eda\",\"learningUnitContentId\":\"c84c6ad9-d71c-42ae-8b24-b5bb0d10d8c3\",\"groupId\":null}");
    }
    val colDefSet = cassandraTypeConverter.getStructColumns(row, Set.empty)
    val sr: Struct = cassandraTypeConverter.convert(row, "test", colDefSet, None)
    val schema = sr.schema()
    checkCols(schema)

    sr.getArray("setCol").get(0).toString shouldBe "A"
    sr.getArray("setCol").get(1).toString shouldBe "B"
    sr.getArray("setCol").get(2).toString shouldBe "C"
  }

  "should convert a Cassandra row to a Struct no columns" in {
    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(false))
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)
    val colDefList = null
    val sr: Struct = cassandraTypeConverter.convert(row, "test", colDefList, None)
    val schema = sr.schema()
    schema.defaultValue() shouldBe null
  }

  "should convert a Cassandra row to a Struct and ignore some" in {
    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(false))
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)

    val ignoreList = Set("intCol", "floatCol")
    val colDefList = cassandraTypeConverter.getStructColumns(row, ignoreList)
    val sr: Struct = cassandraTypeConverter.convert(row, "test", colDefList, None)

    sr.get("timeuuidCol").toString shouldBe uuid.toString
    sr.get("mapCol") shouldBe ('empty)

    try {
      sr.get("intCol")
      fail()
    } catch {
      case _: DataException => // Expected, so continue
    }

    try {
      sr.get("floatCol")
      fail()
    } catch {
      case _: DataException => // Expected, so continue
    }

  }

  def mockRow(row: Row) = {
    when(row.getString("uuid")).thenReturn("string")
    when(row.getInet("inetCol")).thenReturn(InetAddress.getByName("127.0.0.1"))
    when(row.getString("asciiCol")).thenReturn("string")
    when(row.getString("textCol")).thenReturn("string")
    when(row.getString("varcharCol")).thenReturn("string")
    when(row.getBool("booleanCol")).thenReturn(true)
    when(row.getShort("smallintCol")).thenReturn(0.toShort)
    when(row.getInt("intCol")).thenReturn(0)
    when(row.getDecimal("decimalCol")).thenReturn(new java.math.BigDecimal(0))
    when(row.getFloat("floatCol")).thenReturn(0)
    when(row.getLong("counterCol")).thenReturn(0.toLong)
    when(row.getLong("bigintCol")).thenReturn(0.toLong)
    when(row.getLong("varintCol")).thenReturn(0.toLong)
    when(row.getDouble("doubleCol")).thenReturn(0.toDouble)
    when(row.getString("timeuuidCol")).thenReturn("111111")
    when(row.getBytes("blobCol")).thenReturn(ByteBuffer.allocate(10))
    when(row.getList("listCol", classOf[String])).thenReturn(new java.util.ArrayList[String])
    when(row.getSet("setCol", classOf[String])).thenReturn(new java.util.HashSet[String])
    when(row.getMap("mapCol", classOf[String], classOf[String])).thenReturn(new java.util.HashMap[String, String])
    when(row.getDate("dateCol")).thenReturn(com.datastax.driver.core.LocalDate.fromDaysSinceEpoch(1))
    when(row.getTime("timeCol")).thenReturn(0)
    when(row.getTimestamp("timestampCol")).thenReturn(new java.util.Date)
    when(row.getUUID("timeuuidCol")).thenReturn(uuid)
    //when(row.getTupleValue("tupleCol")).thenReturn(new TupleValue("tuple"))
  }

  def checkCols(schema: Schema) = {
    schema.field("uuidCol").schema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    schema.field("inetCol").schema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    schema.field("asciiCol").schema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    schema.field("textCol").schema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    schema.field("varcharCol").schema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    schema.field("booleanCol").schema().`type`() shouldBe Schema.OPTIONAL_BOOLEAN_SCHEMA.`type`()
    schema.field("smallintCol").schema().`type`() shouldBe Schema.INT16_SCHEMA.`type`()
    schema.field("intCol").schema().`type`() shouldBe Schema.OPTIONAL_INT32_SCHEMA.`type`()
    schema.field("decimalCol").schema().`type`() shouldBe OPTIONAL_DECIMAL_SCHEMA.`type`()
    schema.field("floatCol").schema().`type`() shouldBe Schema.OPTIONAL_FLOAT32_SCHEMA.`type`()
    schema.field("counterCol").schema().`type`() shouldBe Schema.OPTIONAL_INT64_SCHEMA.`type`()
    schema.field("bigintCol").schema().`type`() shouldBe Schema.OPTIONAL_INT64_SCHEMA.`type`()
    schema.field("varintCol").schema().`type`() shouldBe Schema.OPTIONAL_INT64_SCHEMA.`type`()
    schema.field("doubleCol").schema().`type`() shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA.`type`()
    schema.field("timeuuidCol").schema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    schema.field("blobCol").schema().`type`() shouldBe Schema.OPTIONAL_BYTES_SCHEMA.`type`()
    schema.field("timeCol").schema().`type`() shouldBe Schema.OPTIONAL_INT64_SCHEMA.`type`()
    schema.field("timestampCol").schema().`type`() shouldBe OPTIONAL_TIMESTAMP_SCHEMA.`type`()
    schema.field("dateCol").schema().`type`() shouldBe OPTIONAL_DATE_SCHEMA.`type`()
  }

  def getSettings(mappingCollectionToJson: Boolean) = {
    val config = Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_SINK_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.KCQL -> "INSERT INTO cassandra-source SELECT * FROM orders PK created",
      CassandraConfigConstants.POLL_INTERVAL -> "1000",
      CassandraConfigConstants.MAPPING_COLLECTION_TO_JSON -> mappingCollectionToJson.toString,
      CassandraConfigConstants.COLUMN_REMOVE_METADATA -> "",
      CassandraConfigConstants.COLUMN_STRING_TO_JSON -> ""

    )
    val taskConfig = CassandraConfigSource(config);
    CassandraSettings.configureSource(taskConfig).toList.head
  }

}
