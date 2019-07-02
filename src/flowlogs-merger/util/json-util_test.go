package util

import (
	"testing"
)

type jsonTestType struct {
	Dude   string  `json:"dude"`
	Amount float32 `json:"amount"`
	Flag   bool    `json:"flag"`
}

func Test_FromJSON(t *testing.T) {
	jsonString := "{ \"dude\":\"sweet\", \"amount\": 10.56, \"flag\":true }"
	val := jsonTestType{}
	err := FromJSON(&jsonString, &val)
	if err != nil {
		t.Errorf("Error parsing JSON. \"%e\"", err)
	}

	if val.Dude != "sweet" {
		t.Errorf("Field not set correctly, Expected: \"%s\", Got: \"%s\"", "sweet", val.Dude)
	}
	if val.Amount != 10.56 {
		t.Errorf("Field not set correctly, Expected: \"%f\", Got: \"%f\"", 10.56, val.Amount)
	}
	if !val.Flag {
		t.Errorf("Field not set correctly, Expected: \"%t\", Got: \"%t\"", true, val.Flag)
	}
}

func Test_ToJSON(t *testing.T) {
	val := jsonTestType{Amount: 10.56, Dude: "sweet", Flag: true}
	jsonString := ToJSON(&val)
	expectedString := "{\"dude\":\"sweet\",\"amount\":10.56,\"flag\":true}"

	if *jsonString != expectedString {
		t.Errorf("JSON output not correct, Expected: \"%s\", Got: \"%s\"", expectedString, *jsonString)
	}
}

func Benchmark_FromJSON(b *testing.B) {
	jsonString := "{ \"dude\":\"sweet\", \"amount\": 10.56, \"flag\":true }"
	val := jsonTestType{}
	for i := 0; i < b.N; i++ {
		FromJSON(&jsonString, &val)
	}
}
