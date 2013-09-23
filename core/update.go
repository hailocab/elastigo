package core

import (
	"encoding/json"
	"fmt"
	"github.com/hailocab/elastigo/api"
)

// The update API allows to update a document based on a script provided. The operation gets the document
// (collocated with the shard) from the index, runs the script (with optional script language and parameters),
// and index back the result (also allows to delete, or ignore the operation). It uses versioning to make sure
// no updates have happened during the “get” and “reindex”. (available from 0.19 onwards).
// Note, this operation still means full reindex of the document, it just removes some network roundtrips
// and reduces chances of version conflicts between the get and the index. The _source field need to be enabled
// for this feature to work.
//
// http://www.elasticsearch.org/guide/reference/api/update.html
// TODO: finish this, it's fairly complex
func Update(pretty bool, index string, _type string, id string, data string, externalVersion bool, version int64) (api.BaseResponse, error) {
	var url string
	var retval api.BaseResponse

	queryString := api.Pretty(pretty)

	// If we're using external versioning, add this and the version number to index as to the query string
	if externalVersion {
		queryString = fmt.Sprintf("%s&version_type=external&version=%v", queryString, version)
	}

	// Build the final url
	url = fmt.Sprintf("/%s/%s/%s/_update?%s", index, _type, id, queryString)

	// Execute request
	body, err := api.DoCommand("POST", url, data)
	if err != nil {
		return retval, err
	}

	// Marshall response into json
	jsonErr := json.Unmarshal(body, &retval)
	if jsonErr != nil {
		return retval, jsonErr
	}

	fmt.Println(body)
	return retval, err
}
