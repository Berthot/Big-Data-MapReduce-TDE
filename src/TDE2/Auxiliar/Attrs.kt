package TDE2.Auxiliar
//country_or_area;year;comm_code;commodity;flow;trade_usd;weight_kg;quantity_name;quantity;category
enum class Attrs(val value: Int) {
    COUNTRY_OR_AREA(0),
    YEAR(1),
    COMM_CODE(2),
    COMMODITY(3),
    FLOW(4),
    TRADE_USD(5),
    WEIGHT_KG(6),
    QUANTITY_NAME(7),
    QUANTITY(8),
    CATEGORY(9)
}