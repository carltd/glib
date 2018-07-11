package glib

import "gopkg.in/mgo.v2/bson"

// NewId - return a global unique id
func NewId() string {
	return bson.NewObjectId().Hex()
}
