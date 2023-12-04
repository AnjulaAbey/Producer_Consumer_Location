package producer

type Message struct {
	CustomerId int64   `json:"ID"`
	Latitude   float64 `json:"latitude"`
	Longitude  float64 `json:"longitude"`
}
