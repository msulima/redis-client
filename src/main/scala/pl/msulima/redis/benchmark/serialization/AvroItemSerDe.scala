package pl.msulima.redis.benchmark.serialization

import java.io.ByteArrayOutputStream
import java.time.Instant

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.util.Utf8
import pl.msulima.redis.benchmark.domain.Item


class AvroItemSerDe {

  private val itemSchema = new Schema.Parser().parse(this.getClass.getResourceAsStream("/item.avsc"))

  def serialize(item: Item): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    val writer = new GenericDatumWriter[GenericRecord](itemSchema)

    val data = new GenericData.Record(itemSchema)
    data.put("id", item.id)
    data.put("name", item.name)
    data.put("price", item.price.toString())
    data.put("sellerId", item.sellerId)
    data.put("bidCount", item.bidCount)
    data.put("imageUrl", item.imageUrl.orNull)
    data.put("startDate", item.startDate.getEpochSecond)
    data.put("endDate", item.endDate.getEpochSecond)

    writer.write(data, encoder)

    encoder.flush()
    out.close()
    out.toByteArray
  }

  def deserialize(bytes: Array[Byte]): Item = {
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    val reader = new GenericDatumReader[GenericRecord](itemSchema)

    val data: GenericRecord = reader.read(null, decoder)

    val id = data.get("id").asInstanceOf[Utf8].toString
    val name = data.get("name").asInstanceOf[Utf8].toString
    val price = BigDecimal(data.get("price").asInstanceOf[Utf8].toString)
    val sellerId = data.get("sellerId").asInstanceOf[Utf8].toString
    val bidCount = data.get("bidCount").asInstanceOf[Int]
    val imageUrl = Option(data.get("imageUrl").asInstanceOf[Utf8]).map(_.toString)
    val startDate = Instant.ofEpochSecond(data.get("startDate").asInstanceOf[Long])
    val endDate = Instant.ofEpochSecond(data.get("endDate").asInstanceOf[Long], 0)

    Item(
      id = id,
      name = name,
      price = price,
      sellerId = sellerId,
      bidCount = bidCount,
      imageUrl = imageUrl,
      startDate = startDate,
      endDate = endDate
    )
  }
}
