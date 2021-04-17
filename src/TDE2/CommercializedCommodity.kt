package TDE2

import TDE2.Auxiliar.Attrs
import TDE2.Auxiliar.CommercializedCommodityWritable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.log4j.BasicConfigurator
import java.io.File
import kotlin.system.exitProcess



fun main() {
    configureHadoop()

    val configuration = Configuration()
    val tdeName = "CommercializedCommodity"
    val input = Path("./in/transactions.csv")
    val intermediario = Path("./output/intermediario_$tdeName.tmp")
    val output = Path("./output/$tdeName.txt")

    // Deleta o folder de resultado antes de gerar um novo
    val outFile = File(output.toString())
    val outFileIntermediario = File(intermediario.toString())
    outFile.deleteRecursively()
    outFileIntermediario.deleteRecursively()

    val job = Job(configuration, tdeName).apply {

        mapperClass = MapCommercializedCommodity::class.java
        reducerClass = ReduceCommercializedCommodity::class.java
//        combinerClass = ForestFireReducer::class.java

        // definicao dos tipos de saida
        mapOutputKeyClass = Text::class.java // chave de saida do map
        mapOutputValueClass = LongWritable::class.java// valor de saida do map
        outputKeyClass = Text::class.java // chave de saida do reduce
        outputValueClass = LongWritable::class.java // valor de saida do reduce
    }

    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, intermediario)

    if (!job.waitForCompletion(true)) {
        println("Deu erro no job1")
        exitProcess(1)
    }

    val job2 = Job(configuration, "${tdeName}2").apply { // uso do apply é opcional

        mapperClass = MapCommercializedCommodity2::class.java
        reducerClass = ReduceCommercializedCommodity2::class.java

        mapOutputKeyClass = Text::class.java // chave de saida do map
        mapOutputValueClass = CommercializedCommodityWritable::class.java// valor de saida do map
        outputKeyClass = Text::class.java // chave de saida do reduce
        outputValueClass = CommercializedCommodityWritable::class.java // valor de saida do reduce
    }

    FileInputFormat.addInputPath(job2, intermediario)
    FileOutputFormat.setOutputPath(job2, output)

    if (!job2.waitForCompletion(true)) {
        println("Deu erro no job2")
        exitProcess(1)
    }

    exitProcess(if (job.waitForCompletion(true)) 0 else 1)
}

private fun configureHadoop() {
    System.setProperty("hadoop.home.dir", "/opt/hadoop-3.2.1")
    BasicConfigurator.configure()
}


class MapCommercializedCommodity : Mapper<LongWritable, Text, Text, LongWritable>() {
    override fun map(key: LongWritable, value: Text, con: Context) {
        val line = value.toString().toLowerCase()
        if (line.startsWith("country_or_area")) return

        val values = line.split(";")
        val year = values[Attrs.YEAR.value]
        val qtyName = values[Attrs.QUANTITY_NAME.value]
        if(year != "2016" || qtyName == "no quantity") return


        val flow = values[Attrs.FLOW.value]
        val commodityCode = values[Attrs.COMM_CODE.value]
        val qty = values[Attrs.QUANTITY.value].toLong()

        con.write(Text("$commodityCode|$flow|"), LongWritable(qty))
    }
}

class ReduceCommercializedCommodity : Reducer<Text, LongWritable, Text, LongWritable>() {
    override fun reduce(key: Text, values: Iterable<LongWritable>, con: Context) {
        val v1: Long = values.sumOf { it.get() }
        con.write(key, LongWritable(v1))
    }
}

class MapCommercializedCommodity2 : Mapper<LongWritable, Text, Text, CommercializedCommodityWritable>() {
    override fun map(key: LongWritable, value: Text, con: Context) {
        val line = value.toString()

        val lines = line.split("|")
        val commCode = lines[0]
        val flow = lines[1]
        val sum = lines[2].trim().toLong()

        con.write(Text(flow), CommercializedCommodityWritable(commCode, sum))
    }
}

class ReduceCommercializedCommodity2 : Reducer<Text, CommercializedCommodityWritable, Text, CommercializedCommodityWritable>() {
    override fun reduce(key: Text, values: Iterable<CommercializedCommodityWritable>, con: Context) {

        var pivot = CommercializedCommodityWritable("", -1)

        // TODO implementar comparação a nivel de objeto
        values.forEach {
            if(it.sum > pivot.sum)
                pivot = it
        }

        con.write(key, pivot)
    }
}