package TDE2.Auxiliar
import org.apache.hadoop.io.WritableComparable
import java.io.DataInput
import java.io.DataOutput
import java.util.*


class AveragePricePerYearWritable: WritableComparable<AveragePricePerYearWritable>  {

    var ocorrencia: Double = 0.0
    var price: Double = 0.0

    constructor(ocorrencia: Double, price: Double) {
        this.ocorrencia = ocorrencia
        this.price = price
    }

    constructor(): super()

    override fun toString(): String {
        return "AveragePricePerYearWritable(ocorrencia=$ocorrencia, price=$price)"
    }

    override fun write(p0: DataOutput) {
        p0.writeDouble(ocorrencia)
        p0.writeDouble(price)
    }

    override fun readFields(p0: DataInput) {
        ocorrencia = p0.readDouble()
        price = p0.readDouble()
    }

    override fun compareTo(other: AveragePricePerYearWritable): Int {
        return hashCode().compareTo(other.hashCode())
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as AveragePricePerYearWritable

        if (ocorrencia != other.ocorrencia) return false
        if (price != other.price) return false

        return true
    }

    override fun hashCode(): Int {
        return Objects.hash(ocorrencia, price)
    }
}