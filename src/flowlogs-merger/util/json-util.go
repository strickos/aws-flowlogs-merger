package util

import (
	"encoding/json"
	"strings"
)

/*
FromJSON parses a JSON blob into the provided object.
*/
func FromJSON(data *string, obj interface{}) error {
	err := json.Unmarshal([]byte(*data), obj)
	return err
}

/*
ToJSON converts the provided object into a JSON string.
*/
func ToJSON(obj interface{}) *string {
	data, err := json.Marshal(obj)
	var str string
	if err != nil {
		str = "{ \"error\": \"" + strings.Replace(err.Error(), "\"", "\\\"", -1) + "\" }"
	} else {
		str = string(data)
	}
	return &str
}
