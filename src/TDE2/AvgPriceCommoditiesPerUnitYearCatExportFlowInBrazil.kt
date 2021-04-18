package TDE2
// AvgPriceCommoditiesPerUnitYearCatExportFlowInBrazil

import TDE2.Auxiliar.Attrs
import TDE2.Auxiliar.CommercializedCommodityWritable
import TDE2.Auxiliar.CommodityUnitTypePriceQtdWritable
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
    val tdeName = "avgPriceCommoditiesPerUnitYearCatExportFlowInBrazil"
    val input = Path("./in/transactions.csv")
    val output = Path("./output/$tdeName.txt")

    // Deleta o folder de resultado antes de gerar um novo
    val outFile = File(output.toString())
    outFile.deleteRecursively()

    val job = Job(configuration, tdeName).apply {

        mapperClass = MapAvgPriceCommoditiesPerUnitYearCatExportFlowInBrazil::class.java
        reducerClass = ReduceAvgPriceCommoditiesPerUnitYearCatExportFlowInBrazil::class.java
//        combinerClass = ForestFireReducer::class.java

        // definicao dos tipos de saida
        mapOutputKeyClass = Text::class.java // chave de saida do map
        mapOutputValueClass = CommodityUnitTypePriceQtdWritable::class.java// valor de saida do map
        outputKeyClass = Text::class.java // chave de saida do reduce
        outputValueClass = CommodityUnitTypePriceQtdWritable::class.java // valor de saida do reduce
    }

    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, output)



    exitProcess(if (job.waitForCompletion(true)) 0 else 1)
}

private fun configureHadoop() {
    System.setProperty("hadoop.home.dir", "/opt/hadoop-3.2.1")
    BasicConfigurator.configure()
}

class MapAvgPriceCommoditiesPerUnitYearCatExportFlowInBrazil : Mapper<LongWritable, Text, Text, CommodityUnitTypePriceQtdWritable>() {
    override fun map(key: LongWritable, value: Text, con: Context) {
        val line = value.toString().toLowerCase()
        if (line.startsWith("country_or_area")) return

        val values = line.split(";")
        if(values[Attrs.QUANTITY_NAME.value] == "no quantity") return

        val year = values[Attrs.YEAR.value]
        val commodity = values[Attrs.COMM_CODE.value]
        val unitType = values[Attrs.QUANTITY_NAME.value]
        val price = values[Attrs.TRADE_USD.value].toLong()
        val qty = values[Attrs.QUANTITY.value].toDouble().toLong()

        val dataObj = CommodityUnitTypePriceQtdWritable(commodity, price, qty)

        con.write(Text("${year}-'${unitType}'"), dataObj)
    }
}

class ReduceAvgPriceCommoditiesPerUnitYearCatExportFlowInBrazil :
    Reducer<Text, CommodityUnitTypePriceQtdWritable, Text, CommodityUnitTypePriceQtdWritable>() {

    override fun reduce(key: Text, values: Iterable<CommodityUnitTypePriceQtdWritable>, con: Context) {
        var pivot = CommodityUnitTypePriceQtdWritable()

        values.forEach {
            if(it.price > pivot.price)
                pivot = it
        }

        con.write(key, pivot)
    }
}