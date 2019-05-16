package model

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/globalsign/mgo/bson"
)

type (
	DocumentStruct map[string]DocumentStructField

	DocumentStructField struct {
		Index         int
		JSON          string
		JSONOmitempty bool
		BSON          string
		BSONOmitempty bool
		Populate      struct {
			Index int
			Name  string
			Find  string
		}
		Children DocumentStruct
	}

	DocumentPopulate struct {
		Document DocumentInterface
		Populate Populate
	}

	documentStructCache struct {
		m     sync.RWMutex
		value map[reflect.Type]DocumentStruct
	}

	DocumentBase struct {
		IsNew   bool              `json:"-" bson:"-"`
		Context context.Context   `json:"-" bson:"-"`
		Model   ModelInterface    `json:"-" bson:"-"`
		Ref     DocumentInterface `json:"-" bson:"-"`
		Old     DocumentInterface `json:"-" bson:"-"`
	}

	DocumentInterface interface {
		New(ctx context.Context, model ModelInterface, document DocumentInterface, isNew bool) DocumentInterface
		Modified(child bool) (paths []string)
		IsModified(name string) (modified bool)

		// 重置到版本号
		// Reset(i int) (err error)
		Save() (err error)
		Insert() (err error)
		Update() (err error)
		UpdateAndFind(update interface{}, isNew bool) (err error)
		Delete() (err error)
		Restore() (err error)
		Validate() (err error)

		Populate(document DocumentInterface) (documentPopulate *DocumentPopulate)
	}
)

var documentStructCacheVal = &documentStructCache{}

func (document *DocumentBase) New(ctx context.Context, model ModelInterface, doc DocumentInterface, isNew bool) DocumentInterface {
	document.Context = ctx
	document.Model = model
	document.IsNew = isNew
	document.Ref = doc
	document.ResetDocumentOld()
	return document.Ref
}

func (document *DocumentBase) Modified(child bool) (paths []string) {
	if document.Ref == nil || document.Old == nil {
		return
	}
	var depth int
	if child {
		depth = 5
	}
	return ValueModified(nil, depth, reflect.Indirect(reflect.ValueOf(document.Ref)), reflect.Indirect(reflect.ValueOf(document.Old)))
}

func (document *DocumentBase) IsModified(path string) bool {
	if document.Ref == nil || document.Old == nil {
		return false
	}
	names := []string{}
	if path != "" {
		names = strings.Split(path, ".")
	}
	return !ValueEqual(names, reflect.Indirect(reflect.ValueOf(document.Ref)), reflect.Indirect(reflect.ValueOf(document.Old)))
}

func (document *DocumentBase) Save() (err error) {
	if document.IsNew {
		return document.Insert()
	}
	return document.Update()
}

func (document *DocumentBase) ResetDocumentOld() {
	if document.Ref == nil {
		document.Old = nil
		return
	}
	documentOldv := reflect.Indirect(reflect.ValueOf(document.Ref))
	documentOldvPtr := reflect.New(documentOldv.Type())
	documentOldvPtr.Elem().Set(documentOldv)
	document.Old = documentOldvPtr.Interface().(DocumentInterface)
	return
}

func (document *DocumentBase) Validate() (err error) {
	if Validator != nil {
		if err = Validator.ValidateDocument(document.Ref); err != nil {
			return
		}
	}
	if err = document.Model.DoEvent("validate", document.Ref); err != nil {
		return
	}
	return
}

func (document *DocumentBase) Insert() (err error) {

	if !document.IsNew {
		err = errors.New("Document isNew=false")
		return
	}
	if document.Ref == nil {
		err = errors.New("Document=nil")
		return
	}
	documentv := reflect.Indirect(reflect.ValueOf(document.Ref))
	field := documentv.FieldByName("ID")
	if !field.IsValid() {
		err = errors.New("Document Field IsValid")
		return
	}
	val := field.Interface()
	switch val.(type) {
	case bson.ObjectId:
		// ObjectId 自动创建
		if val.(bson.ObjectId) == "" {
			field.Set(reflect.ValueOf(bson.NewObjectId()))
		}
	case string:
		// 不能为空
		if val.(string) == "" {
			err = errors.New("Document ID empty")
			return
		}
	case int, int16, int32, int64:
		// 不能为 0
		if val.(int64) == 0 {
			err = errors.New("Document ID empty")
			return
		}
	}

	if err = document.Model.DoEvent("save", document.Ref); err != nil {
		return
	}
	if err = document.Model.DoEvent("insert", document.Ref); err != nil {
		return
	}

	// 插入
	if err = document.Model.DB(document.Context).Insert(document.Ref); err != nil {
		return
	}
	document.ResetDocumentOld()
	document.IsNew = false
	return
}

func (document *DocumentBase) Update() (err error) {
	if document.IsNew {
		err = errors.New("Document isNew=true")
		return
	}
	if document.Ref == nil {
		err = errors.New("Document=nil")
		return
	}
	documentv := reflect.Indirect(reflect.ValueOf(document.Ref))
	documentOldv := reflect.Indirect(reflect.ValueOf(document.Old))
	var documentStruct DocumentStruct
	if documentStruct, err = DocumentStructParse(documentv.Type()); err != nil {
		return
	}

	if err = document.Model.DoEvent("save", document.Ref); err != nil {
		return
	}

	if err = document.Model.DoEvent("update", document.Ref); err != nil {
		return
	}

	set := map[string]interface{}{}
	unset := map[string]interface{}{}
	for _, sieldStruct := range documentStruct {
		// bson 不储存
		if sieldStruct.BSON == "" {
			continue
		}
		v1 := documentv.Field(sieldStruct.Index)
		v2 := documentOldv.Field(sieldStruct.Index)

		// 相同 不储存
		if reflect.DeepEqual(v1.Interface(), v2.Interface()) {
			continue
		}

		// 遍历
		switch v1.Kind() {
		case reflect.Slice, reflect.Map, reflect.String:
			if sieldStruct.BSONOmitempty && v1.Len() == 0 {
				unset[sieldStruct.BSON] = ""
			} else {
				set[sieldStruct.BSON] = v1.Interface()
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if sieldStruct.BSONOmitempty && v1.Int() == 0 {
				unset[sieldStruct.BSON] = ""
			} else {
				set[sieldStruct.BSON] = v1.Interface()
			}
		default:
			val := v1.Interface()
			if sieldStruct.BSONOmitempty && val == nil {
				unset[sieldStruct.BSON] = ""
			} else {
				set[sieldStruct.BSON] = v1.Interface()
			}
		}
	}
	update := bson.M{}
	if len(set) != 0 {
		update["$set"] = set
	}
	if len(unset) != 0 {
		update["$unset"] = unset
	}
	if len(update) == 0 {
		return
	}

	id := documentOldv.FieldByName("ID").Interface()

	if err = document.Model.Query(document.Context).ID(id).Update(update); err != nil {
		return
	}
	document.ResetDocumentOld()
	return
}

func (document *DocumentBase) UpdateAndFind(update interface{}, isNew bool) (err error) {
	if document.IsNew {
		err = errors.New("Document isNew=true")
		return
	}
	if document.Ref == nil {
		err = errors.New("Document=nil")
		return
	}
	documentV1 := reflect.ValueOf(document.Ref)
	documentV2 := reflect.New(documentV1.Elem().Type())

	documentOldv := reflect.ValueOf(document.Old).Elem()
	id := documentOldv.FieldByName("ID").Interface()

	query := document.Model.Query(document.Context)

	// document
	if err = query.ID(id).UpdateAndFind(update, documentV2.Interface(), isNew); err != nil {
		return
	}
	documentV1.Elem().Set(documentV2.Elem())
	return
}

func (document *DocumentBase) Delete() (err error) {
	documentOldv := reflect.Indirect(reflect.ValueOf(document.Old))
	id := documentOldv.FieldByName("ID").Interface()
	if err = document.Model.Query(document.Context).ID(id).NeDeleted().Delete(); err != nil {
		return
	}
	return
}
func (document *DocumentBase) Restore() (err error) {
	documentOldv := reflect.Indirect(reflect.ValueOf(document.Old))
	id := documentOldv.FieldByName("ID").Interface()
	if err = document.Model.Query(document.Context).ID(id).EqDeleted().Restore(); err != nil {
		return
	}
	return
}

func (document *DocumentBase) Populate(doc DocumentInterface) *DocumentPopulate {
	return &DocumentPopulate{Document: doc}
}

func (documentPopulate *DocumentPopulate) Path(path string, value *Query) *DocumentPopulate {
	if documentPopulate.Populate == nil {
		documentPopulate.Populate = Populate{}
	}
	documentPopulate.Populate[path] = value
	return documentPopulate
}

func (documentPopulate *DocumentPopulate) Exec() (err error) {
	if documentPopulate.Populate != nil {
		err = documentPopulate.Populate.One(documentPopulate.Document)
	}
	return
}

func ValueModified(base []string, depth int, v1, v2 reflect.Value) (paths []string) {
	return paths
}

func ValueEqual(path []string, v1, v2 reflect.Value) bool {
	if !v1.IsValid() || !v2.IsValid() {
		return v1.IsValid() == v2.IsValid()
	}
	if v1.Kind() != v2.Kind() {
		return false
	}
	if v1.Type() != v2.Type() {
		return false
	}
	switch v1.Kind() {
	case reflect.Ptr:
		if v1.Pointer() == v2.Pointer() {
			return true
		}
		return ValueEqual(path, v1.Elem(), v2.Elem())
	case reflect.Map:
		if v1.Pointer() == v2.Pointer() {
			return true
		}
		if v1.IsNil() != v2.IsNil() {
			return false
		}
		if v1.Len() != v2.Len() {
			return false
		}
		if len(path) == 0 {
			return reflect.DeepEqual(v1.Interface(), v2.Interface())
		}
		key := reflect.ValueOf(path[0])
		return ValueEqual(path[:1], v1.MapIndex(key), v2.MapIndex(key))
	case reflect.Slice:
		if v1.Pointer() == v2.Pointer() {
			return true
		}
		if v1.IsNil() != v2.IsNil() {
			return false
		}
		if v1.Len() != v2.Len() {
			return false
		}
		if len(path) == 0 {
			return reflect.DeepEqual(v1.Interface(), v2.Interface())
		}
		for i := 0; i < v1.Len(); i++ {
			if !ValueEqual(path, v1.Index(i), v2.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Struct:
		if len(path) == 0 {
			return reflect.DeepEqual(v1.Interface(), v2.Interface())
		}
		return ValueEqual(path[:1], v1.FieldByName(path[0]), v2.FieldByName(path[0]))
	default:
		return reflect.DeepEqual(v1.Interface(), v2.Interface())
	}
}

func (cache *documentStructCache) Get(key reflect.Type) (value DocumentStruct, ok bool) {
	cache.m.RLock()
	defer cache.m.RUnlock()
	value, ok = cache.value[key]
	return
}

func (cache *documentStructCache) Set(key reflect.Type, value DocumentStruct) {
	cache.m.Lock()
	defer cache.m.Unlock()
	if cache.value == nil {
		cache.value = map[reflect.Type]DocumentStruct{}
	}
	cache.value[key] = value
	if len(cache.value) > 10000 {
		log.Panicln("[DocumentStructParse]", key)
	}
	return
}

func DocumentStructParse(t reflect.Type) (documentStruct DocumentStruct, err error) {
	// 指针
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// 不是 struct 结构
	if t.Kind() != reflect.Struct {
		err = fmt.Errorf("Document not struct (%s)", t.String())
		return
	}

	// 有缓存 返回
	{
		var ok bool
		if documentStruct, ok = documentStructCacheVal.Get(t); ok {
			return
		}
	}
	documentStruct = DocumentStruct{}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Name == "DocumentBase" {
			continue
		}
		jsonTag := strings.SplitN(field.Tag.Get("json"), ",", 2)
		jsonName := jsonTag[0]
		if jsonName == "" {
			jsonName = field.Name
		} else if jsonName == "-" {
			jsonName = ""
		}

		bsonTag := strings.SplitN(field.Tag.Get("bson"), ",", 2)
		bsonName := bsonTag[0]
		if bsonName == "" {
			bsonName = field.Name
		} else if bsonName == "-" {
			bsonName = ""
		}

		populateTag := strings.SplitN(field.Tag.Get("populate"), ",", 2)
		populateName := populateTag[0]
		var populateFind string
		if len(populateTag) == 2 {
			populateFind = populateTag[1]
		}
		if populateName == "-" {
			populateName = ""
		}
		if populateFind != "" && populateName == "" {
			populateName = "ID"
		}
		if populateName != "" && populateFind == "" {
			populateFind = "ID"
		}

		if jsonName == "" && bsonName == "" && populateName == "" {
			continue
		}

		// 结构体信息
		value := DocumentStructField{
			Index:         i,
			JSON:          jsonName,
			JSONOmitempty: len(jsonTag) > 1 && jsonTag[1] == "omitempty",
			BSON:          bsonName,
			BSONOmitempty: len(bsonTag) > 1 && bsonTag[1] == "omitempty",
		}

		if populateName != "" {
			// 带有  populateName 的 不再解析
			if field, ok := t.FieldByName(populateName); ok {
				value.Populate.Index = field.Index[0]
				value.Populate.Name = populateName
				value.Populate.Find = populateFind
			}
		} else if value.BSON != "" {
			elem := field.Type

			// 切片 找子级
			if elem.Kind() == reflect.Slice {
				elem = elem.Elem()
			}

			// 指针 找子级
			if elem.Kind() == reflect.Ptr {
				elem = elem.Elem()
			}

			// 结构体允许嵌套
			if elem.Kind() == reflect.Struct && elem != reflect.TypeOf(time.Time{}) {
				if value.Children, err = DocumentStructParse(elem); err != nil {
					return
				}
			}
		}
		documentStruct[field.Name] = value
	}

	// 设置缓存
	documentStructCacheVal.Set(t, documentStruct)
	return
}
