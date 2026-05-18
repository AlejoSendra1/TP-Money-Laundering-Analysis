package transaction

import (
	"time"
)

type Transaction struct {
	Timestamp     time.Time // Fecha y hora (ISO 8601)
	FromBank      int       // Código de entidad origen
	ToBank        int       // Código de entidad destino
	FromAccount   string    // CBU/CVU o ID de cuenta origen
	ToAccount     string    // CBU/CVU o ID de cuenta destino
	Amount        float64   // Monto de la operación
	Currency      string    // Moneda original (ej: "USD", "EUR", "ARS")
	PaymentFormat string    // Formato (ej: "Wire", "ACH", "Cheque")
}

type TransactionResultQuery1 struct {
	FromBank    int
	FromAccount string
	ToBank      int
	ToAccount   string
	Amount      float64
}
