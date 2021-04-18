package TDE2.Auxiliar

import org.apache.hadoop.io.WritableComparable
import java.io.DataInput
import java.io.DataOutput
import java.util.*

class CommodityUnitTypePriceQtdWritable: WritableComparable<CommodityUnitTypePriceQtdWritable> {
    var commodity: String = ""
    var price: Long = -1
    var qty: Long = 0

    constructor(commodity: String, price: Long, qty: Long) {
        this.commodity = commodity
        this.price = price
        this.qty = qty
    }

    constructor(): super()

    override fun toString(): String {
//        return "commodity:${commodity}-price:${price}-qty:${qty}"
        return "commodity:${commodity}-price:${price}"
    }

    override fun write(p0: DataOutput) {
        p0.writeUTF(commodity)
        p0.writeLong(price)
        p0.writeLong(qty)
    }

    override fun readFields(p0: DataInput) {
        commodity = p0.readUTF()
        price = p0.readLong()
        qty = p0.readLong()
    }

    override fun compareTo(other: CommodityUnitTypePriceQtdWritable): Int {
        return hashCode().compareTo(other.hashCode())
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CommodityUnitTypePriceQtdWritable

        if (commodity != other.commodity) return false
        if (price != other.price) return false
        if (qty != other.qty) return false

        return true
    }

    override fun hashCode(): Int {
        return Objects.hash(commodity, price, qty)
    }
}