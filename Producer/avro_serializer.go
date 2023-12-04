package producer

import "github.com/linkedin/goavro"

// SerializeEvent serializes the Message
func SerializeEvent(event Message) ([]byte, error) {
	codec, err := goavro.NewCodec(`{
        "type": "record",
        "name": "message",
        "fields": [
            {"name": "ID", "type": "long"},
            {"name": "latitude", "type": "double"},
            {"name": "longitude", "type": "double"}
        ]
    }`)
	if err != nil {
		return nil, err
	}

	avroData, err := codec.TextualFromNative(nil, map[string]interface{}{
		"ID":        event.CustomerId,
		"latitude":  event.Latitude,
		"longitude": event.Longitude,
	})
	if err != nil {
		return nil, err
	}

	return avroData, nil
}
