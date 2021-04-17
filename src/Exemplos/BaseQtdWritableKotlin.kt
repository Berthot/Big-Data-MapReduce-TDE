package Exemplos

import org.apache.hadoop.io.WritableComparable
import java.io.DataInput
import java.io.DataOutput
import java.util.*

class BaseQtdWritableKotlin: WritableComparable<BaseQtdWritableKotlin> {

    var texto: String = ""
    var qtd: Long = 0

    constructor(texto: String, qtd: Long) {
        this.texto = texto
        this.qtd = qtd
    }

    constructor(): super()

    override fun toString(): String {
        return "Exemplos.BaseQtdWritableKotlin(texto=$texto, qtd=$qtd)"
    }

    override fun write(p0: DataOutput) {
        p0.writeUTF(texto)
        p0.writeLong(qtd)
    }

    override fun readFields(p0: DataInput) {
        texto = p0.readUTF()
        qtd = p0.readLong()
    }

    override fun compareTo(other: BaseQtdWritableKotlin): Int {
        return hashCode().compareTo(other.hashCode())
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as BaseQtdWritableKotlin

        if (texto != other.texto) return false
        if (qtd != other.qtd) return false

        return true
    }

    override fun hashCode(): Int {
        return Objects.hash(texto, qtd)
    }
}