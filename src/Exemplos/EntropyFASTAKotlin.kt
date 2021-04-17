package Exemplos
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
import kotlin.math.log10
import kotlin.system.exitProcess


fun main() {

    System.setProperty("hadoop.home.dir", "/opt/hadoop-3.2.1")
    BasicConfigurator.configure()
    val configuration = Configuration()

    val input = Path("./in/transactions.csv")
    val intermediario = Path("./output/intermediario.tmp")
    val output = Path("./output/entropia.txt")

    // Deleta o folder de resultado antes de gerar um novo
    val outFile = File(output.toString())
    val outFileIntermediario = File(intermediario.toString())
    outFile.deleteRecursively()
    outFileIntermediario.deleteRecursively()

    val job = Job(configuration, "contagemGenetica").apply {

        mapperClass = MapEtapaA::class.java
        reducerClass = ReduceEtapaA::class.java
//        combinerClass = ForestFireReducer::class.java

        // definicao dos tipos de saida
        mapOutputKeyClass = Text::class.java // chave de saida do map
        mapOutputValueClass = LongWritable::class.java// valor de saida do map
        outputKeyClass = Text::class.java // chave de saida do reduce
        outputValueClass = DoubleWritable::class.java // valor de saida do reduce
    }

    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, intermediario)

    exitProcess(if (job.waitForCompletion(true)) 0 else 1)
}


class MapEtapaA : Mapper<LongWritable, Text, Text, LongWritable>() {
    override fun map(key: LongWritable, value: Text, con: Context) {
        val linha = value.toString()
        if (linha.startsWith(">")) return

        val caracteres = linha.toCharArray()

        caracteres.forEach { c ->
            val chaveSaida = Text(c.toString())
            val valorSaida = LongWritable(1)

            con.write(chaveSaida, valorSaida)
            con.write(Text("total"), LongWritable(1))
        }
    }
}

class ReduceEtapaA : Reducer<Text, LongWritable, Text, LongWritable>() {
    override fun reduce(key: Text, values: Iterable<LongWritable>, con: Context) {
        var soma: Long = 0
        values.forEach {
            soma += it.get()
        }
        con.write(key, LongWritable(soma))
    }
}

class MapEtapaB : Mapper<LongWritable, Text, Text, BaseQtdWritableKotlin>() {
    override fun map(key: LongWritable, value: Text, con: Context) {
        val linha = value.toString()
        val campos = linha.split("\t")
        val chave = campos[0]
        val qtd: Long = campos[1].toLong()

        con.write(Text("entropia"), BaseQtdWritableKotlin(chave, qtd))
    }
}

class ReduceEtapaB : Reducer<Text, BaseQtdWritableKotlin, Text, DoubleWritable>() {
    override fun reduce(key: Text, values: Iterable<BaseQtdWritableKotlin>, con: Context) {

        val qtdTotal: Long = values.first {
            it.texto == "total"
        }.qtd

        values.forEach {
            if (it.texto != "total") {
                val qtdCaracter = it.qtd
                val prob: Double = qtdCaracter / qtdTotal.toDouble()
                val entropia: Double = -prob * log10(prob) / log10(2.0)

                con.write(Text(it.texto), DoubleWritable(entropia))
            }
        }
    }
}