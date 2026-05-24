package control

import (
	"encoding/json"
	"tp_distribuidos/common/middleware"
)

const (
	TypeEOF = iota // 0
)

// Sirve para coordinar el envio de EOF entre los workers de sums
type ControlMessage struct {
	Type     uint8 `json:"type"`
	ClientID int64 `json:"client_id"`
}

func SerializeControlMessage(ctrlMsg ControlMessage) (*middleware.Message, error) {
	body, err := json.Marshal(ctrlMsg)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}
	return &message, nil
}

func DeserializeControlMessage(message *middleware.Message) (*ControlMessage, error) {
	var ctrlMsg ControlMessage
	err := json.Unmarshal([]byte(message.Body), &ctrlMsg)
	if err != nil {
		return nil, err
	}
	return &ctrlMsg, nil
}
