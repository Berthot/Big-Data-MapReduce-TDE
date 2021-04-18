package TDE2

import TDE2.Auxiliar.Attrs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
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
    val tdeName = "transactionPerFlowTypeAndYear"
    val input = Path("./in/transactions.csv")
    val output = Path("./output/$tdeName.txt")

    // Deleta o folder de resultado antes de gerar um novo
    val outFile = File(output.toString())
    outFile.deleteRecursively()

    val job = Job(configuration, tdeName).apply {

        mapperClass = MapTransPerFlowTypeAndYear::class.java
        reducerClass = ReduceTransPerFlowTypeAndYear::class.java
//        combinerClass = ForestFireReducer::class.java

        // definicao dos tipos de saida
        mapOutputKeyClass = Text::class.java // chave de saida do map
        mapOutputValueClass = LongWritable::class.java// valor de saida do map
        outputKeyClass = Text::class.java // chave de saida do reduce
        outputValueClass = LongWritable::class.java // valor de saida do reduce
    }

    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, output)

    exitProcess(if (job.waitForCompletion(true)) 0 else 1)
}

private fun configureHadoop() {
    System.setProperty("hadoop.home.dir", "/opt/hadoop-3.2.1")
    BasicConfigurator.configure()
}

class MapTransPerFlowTypeAndYear : Mapper<LongWritable, Text, Text, LongWritable>() {
    override fun map(key: LongWritable, value: Text, con: Context) {
        val line = value.toString().toLowerCase()
        if (line.startsWith("country_or_area")) return

        val values = line.split(";")
        val year = values[Attrs.YEAR.value]
        val flow = values[Attrs.FLOW.value]

        con.write(Text("${year}-${flow}"), LongWritable(1))
    }
}

class ReduceTransPerFlowTypeAndYear : Reducer<Text, LongWritable, Text, LongWritable>() {
    override fun reduce(key: Text, values: Iterable<LongWritable>, con: Context) {
        val transactionTotal: Long = values.sumOf { it.get() }
        con.write(key, LongWritable(transactionTotal))
    }
}