package transaction

import (
	"time"
)

type QueryID int

const (
	Query1 QueryID = iota + 1
	Query2
	Query3
	Query4
	Query5
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
type LowAmountTransfer struct {
	FromBank    int
	FromAccount string
	ToBank      int
	ToAccount   string
	Amount      float64
}
type PaymentFormatAverage struct {
	PaymentFormat string
	Average       float64
	Count         int
}
type MaxBankTransaction struct {
	BankCode int
	Account  string
	Amount   float64
}

type ThresholdFilteredTransfer struct {
	FromBank      int
	FromAccount   string
	PaymentFormat string
	Amount        float64
}
type QueryResult struct {
	QueryID      QueryID
	Transactions interface{}
}
type QueriesResult struct {
	ClientID int64
	Results  map[QueryID]QueryResult
}
