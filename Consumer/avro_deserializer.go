package consumer

import (
	"github.com/linkedin/goavro"
	"producer_consumer/Producer"
)

func deserializeEvent(avroData []byte) (producer.Message, error) {
	codec, err := goavro.NewCodec(`{
        "type": "record",
        "name": "LocationEvent",
        "fields": [
            {"name": "ID", "type": "long"},
            {"name": "latitude", "type": "double"},
            {"name": "longitude", "type": "double"}
        ]
    }`)
	if err != nil {
		return producer.Message{}, err
	}

	native, _, err := codec.NativeFromTextual(avroData)
	if err != nil {
		return producer.Message{}, err
	}

	eventMap := native.(map[string]interface{})
	return producer.Message{
		CustomerId: eventMap["ID"].(int64),
		Latitude:   eventMap["latitude"].(float64),
		Longitude:  eventMap["longitude"].(float64),
	}, nil
}
