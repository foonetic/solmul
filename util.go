package solmul

import "go.mongodb.org/mongo-driver/bson"

// ArrayContains check if a value is in an array.
func ArrayContains[T comparable](array []T, value T) bool {
	for _, v := range array {
		if v == value {
			return true
		}
	}
	return false
}

func MapKeys[K comparable, V any](m map[K]V) (keys []K) {
	for k := range m {
		keys = append(keys, k)
	}
	return
}

func ToJson[T any](v T) ([]byte, error) {
	return bson.MarshalExtJSON(v, false, false)
}

func FindSubscriptionType(method string) string {
	for _, sub_type := range subscrptionTypes {
		if method == sub_type+"Subscribe" {
			return sub_type
		}
	}

	return ""
}

func ChanPipe[T1 any, T2 any](upstream <-chan T1, downstream chan<- T2, f func(T1) T2) {
	for {
		msg, ok := <-upstream
		if !ok {
			close(downstream)
			break
		}
		downstream <- f(msg)
	}
}
