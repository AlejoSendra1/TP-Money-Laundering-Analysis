package transaction

import "time"

type Transaction struct {
	Id                 uint64
	Timestamp          time.Time
	From_Bank          uint64
	Account            string
	To_Bank            uint64
	Account_1          string
	Amount_Received    float64
	Receiving_Currency string
	Amount_Paid        float64
	Payment_Currency   string
	Payment_Format     string
}
