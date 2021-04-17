package TDE2.Auxiliar

import org.apache.hadoop.io.WritableComparable
import java.io.DataInput
import java.io.DataOutput
import java.util.*

class CommercializedCommodityWritable: WritableComparable<CommercializedCommodityWritable> {

    var commodityCode: String = ""
    var sum: Long = 0

    constructor(texto: String, qtd: Long) {
        this.commodityCode = texto
        this.sum = qtd
    }

    constructor(): super()

    override fun toString(): String {
        return "CommodityAndSum(commodityCode=$commodityCode, sum=$sum)"
    }

    override fun write(p0: DataOutput) {
        p0.writeUTF(commodityCode)
        p0.writeLong(sum)
    }

    override fun readFields(p0: DataInput) {
        commodityCode = p0.readUTF()
        sum = p0.readLong()
    }

    override fun compareTo(other: CommercializedCommodityWritable): Int {
        return hashCode().compareTo(other.hashCode())
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CommercializedCommodityWritable

        if (commodityCode != other.commodityCode) return false
        if (sum != other.sum) return false

        return true
    }

    override fun hashCode(): Int {
        return Objects.hash(commodityCode, sum)
    }
}