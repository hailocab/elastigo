package core

import (
	"encoding/json"
	"fmt"
	"github.com/hailocab/elastigo/api"
)

func get(pretty bool, index string, _type string, id string, source interface{}) (api.BaseResponse, error) {
	var url string
	retval := api.BaseResponse{Source: source}
	if len(_type) > 0 {
		url = fmt.Sprintf("/%s/%s/%s?%s", index, _type, id, api.Pretty(pretty))
	} else {
		url = fmt.Sprintf("/%s/%s?%s", index, id, api.Pretty(pretty))
	}
	body, err := api.DoCommand("GET", url, nil)
	if err != nil {
		return retval, err
	}

	// marshall into json
	jsonErr := json.Unmarshal(body, &retval)
	if jsonErr != nil {
		return retval, jsonErr
	}

	//fmt.Println(body)
	return retval, err
}

// The get API allows to get a typed JSON document from the index based on its id.
// GET - retrieves the doc
// HEAD - checks for existence of the doc
// http://www.elasticsearch.org/guide/reference/api/get.html
// TODO: make this implement an interface
func Get(pretty bool, index string, _type string, id string) (api.BaseResponse, error) {
	return get(pretty, index, _type, id, nil)
}

// Same as Get but with custom source type.
func GetCustom(pretty bool, index string, _type string, id string, source interface{}) (api.BaseResponse, error) {
	return get(pretty, index, _type, id, source)
}

// The API also allows to check for the existance of a document using HEAD

func Exists(pretty bool, index string, _type string, id string) (bool, error) {

	var url string

	var response map[string]interface{}

	if len(_type) > 0 {
		url = fmt.Sprintf("/%s/%s/%s?fields=_id%s", index, _type, id, api.Pretty(pretty))
	} else {
		url = fmt.Sprintf("/%s/%s?fields=_id%s", index, id, api.Pretty(pretty))
	}

	req, err := api.ElasticSearchRequest("HEAD", url)

	if err != nil {
		fmt.Println(err)
	}

	httpStatusCode, _, err := req.Do(&response)

	if err != nil {
		return false, err
	}
	if httpStatusCode == 404 {
		return false, err
	} else {
		return true, err
	}
}
