package TDE2

import TDE2.Auxiliar.Attrs
import TDE2.Auxiliar.OcorrencyAndPriceForAvgWritable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DoubleWritable
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
    configuration.set("mapred.textoutputformat.separatorText", ",");
    val tdeName = "averageCommPricePerYear"
    val input = Path("./in/transactions.csv")
    val output = Path("./output/$tdeName.txt")

    // Deleta o folder de resultado antes de gerar um novo
    val outFile = File(output.toString())
    outFile.deleteRecursively()

    val job = Job(configuration, tdeName).apply {

        mapperClass = MapAverageCommPricePerYear::class.java
        reducerClass = ReduceAverageCommPricePerYear::class.java
//        combinerClass = ForestFireReducer::class.java

        // definicao dos tipos de saida
        mapOutputKeyClass = Text::class.java // chave de saida do map
        mapOutputValueClass = OcorrencyAndPriceForAvgWritable::class.java// valor de saida do map
        outputKeyClass = Text::class.java // chave de saida do reduce
        outputValueClass = DoubleWritable::class.java // valor de saida do reduce
    }

    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, output)

    exitProcess(if (job.waitForCompletion(true)) 0 else 1)
}

private fun configureHadoop() {
    System.setProperty("hadoop.home.dir", "/opt/hadoop-3.2.1")
    BasicConfigurator.configure()
}

class MapAverageCommPricePerYear : Mapper<LongWritable, Text, Text, OcorrencyAndPriceForAvgWritable>() {
    override fun map(key: LongWritable, value: Text, con: Context) {
        val line = value.toString().toLowerCase()

        if (line.startsWith("country_or_area")) return

        val values = line.split(";")
        val year = values[Attrs.YEAR.value]
        val price = values[Attrs.TRADE_USD.value].toDouble()
        val ocorrency: Double = 1.0

        con.write(Text(year), OcorrencyAndPriceForAvgWritable(ocorrency, price))

    }
}

class ReduceAverageCommPricePerYear : Reducer<Text, OcorrencyAndPriceForAvgWritable, Text, DoubleWritable>() {
    override fun reduce(key: Text, values: Iterable<OcorrencyAndPriceForAvgWritable>, con: Context) {

        var somaPrecos: Double = 0.0
        var qtd: Double = 0.0

        values.forEach {
            somaPrecos += it.price
            qtd += it.ocorrencia
        }

        val avg: Double = somaPrecos / qtd

        con.write(key, DoubleWritable(avg))
    }
}