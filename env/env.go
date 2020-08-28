package env

import (
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

func init() {
	var dotenvFile string = os.Getenv("DOTENV_PATH")
	err := godotenv.Load(dotenvFile)

	if err != nil {
		log.Fatal("Error loading .env file")
	}
}
func GetString(key string) string {
	return os.Getenv(key)
}
func Get(key string, out interface{}, def interface{}, convertWith func(string) (interface{}, error)) error {
	sResult := os.Getenv(key)

	val := reflect.ValueOf(out)
	if val.Kind() != reflect.Ptr {
		panic("out must be a pointer")
	}

	switch out.(type) {
	case *int:
		if len(sResult) > 0 {
			iResult, err := strconv.Atoi(sResult)
			if err != nil {
				return err
			}
			val.Elem().SetInt(int64(iResult))
			return nil
		}
	case *string:
		if len(sResult) > 0 {
			val.Elem().SetString(sResult)
			return nil
		}
	case *bool:
		switch strings.ToUpper(sResult) {
		case "TRUE", "1":
			val.Elem().SetBool(true)
			return nil
		case "FALSE", "0":
			val.Elem().SetBool(false)
			return nil
		}
	default:
		if convertWith != nil {
			value, err := convertWith(sResult)
			if err != nil {
				return err
			}
			test := reflect.ValueOf(value)
			// fmt.Printf("type of val: %v\n", val.Elem().Type().String())
			// fmt.Printf("type of test: %v\n", test.Type().String())
			val.Elem().Set(test.Convert(val.Elem().Type()))
			return nil
		}
	}
	defVal := reflect.ValueOf(def)
	if !defVal.IsNil() {
		if defVal.Elem().Type() != val.Elem().Type() {
			panic("out and def types dont match")
		}
		val.Elem().Set(defVal)
	}

	return nil
}
