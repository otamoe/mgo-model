package model

import (
	"context"
	"log"
	"reflect"
	"strings"

	"github.com/globalsign/mgo"
)

type (
	ModelEventNext func() (err error)
	ModelEventFunc func(document DocumentInterface, next ModelEventNext) (err error)
	ModelInterface interface {
		OnEvent(name string, funcs ...ModelEventFunc)
		DoEvent(name string, document DocumentInterface) (err error)
		DB(ctx context.Context) (c *mgo.Collection)
		Exists(ctx context.Context) (exists bool, err error)
		Create(ctx context.Context) (err error)
		Update(ctx context.Context) (updated []string, err error)
		Drop(ctx context.Context) (err error)
		Query(ctx context.Context) (query *Query)
		DocumentStruct() DocumentStruct
	}
	Model struct {
		Name     string
		GetFunc  func(value interface{}, query *Query) *Query
		Document DocumentInterface
		Indexs   []mgo.Index
		Events   map[string][]ModelEventFunc
	}
)

var CONTEXT = "mongo"

func (model *Model) OnEvent(name string, funcs ...ModelEventFunc) {
	if model.Events == nil {
		model.Events = make(map[string][]ModelEventFunc, 0)
	}
	model.Events[name] = append(model.Events[name], funcs...)
}

func (model *Model) DoEvent(name string, document DocumentInterface) (err error) {
	if model.Events == nil {
		return
	}
	events := model.Events[name]
	if events == nil {
		return
	}
	i := 0
	var next ModelEventNext
	next = func() (err error) {
		if len(events) > i {
			i++
			err = events[i-1](document, next)
		}
		return
	}
	for len(events) > i {
		if err = next(); err != nil {
			return
		}
	}
	return
}

func (model *Model) DB(ctx context.Context) (c *mgo.Collection) {
	names := strings.SplitN(model.Name, ".", 2)
	if len(names) == 1 {
		names = []string{"", names[0]}
	}
	c = ctx.Value(CONTEXT).(*mgo.Session).DB(names[0]).C(names[1])
	return
}

func (model *Model) Exists(ctx context.Context) (exists bool, err error) {
	names := strings.SplitN(model.Name, ".", 2)
	if len(names) == 1 {
		names = []string{"", names[0]}
	}
	db := ctx.Value(CONTEXT).(*mgo.Session).DB(names[0])
	var collectionNames []string
	if collectionNames, err = db.CollectionNames(); err != nil {
		return
	}
	for _, collectionName := range collectionNames {
		if collectionName == names[1] {
			exists = true
			return
		}
	}
	return
}

func (model *Model) Create(ctx context.Context) (err error) {
	var exists bool
	if exists, err = model.Exists(ctx); err != nil || exists {
		return
	}
	collection := model.DB(ctx)
	if err = collection.Create(&mgo.CollectionInfo{}); err != nil {
		return
	}
	if _, err = model.Update(ctx); err != nil {
		return
	}
	return
}

func (model *Model) Update(ctx context.Context) (updated []string, err error) {
	collection := model.DB(ctx)
	indexMaps := map[string]mgo.Index{}
	for _, index := range model.Indexs {
		if index.Name == "" {
			for _, field := range index.Key {
				if index.Name != "" {
					index.Name += "_"
				}
				field = strings.Replace(field, "@", "2d-", -1)
				field = strings.Replace(field, ":", "-", -1)
				field = strings.Replace(field, "$", "", -1)
				field = strings.Replace(field, "+", "", -1)
				index.Name += field
			}
		}
		indexMaps[index.Name] = index
	}
	var oldIndexs []mgo.Index
	oldIndexs, err = collection.Indexes()
	if err != nil && !strings.HasSuffix(err.Error(), "doesn't exist") {
		return
	}
	err = nil

	oldIndexMaps := map[string]mgo.Index{}
	for _, index := range oldIndexs {
		oldIndexMaps[index.Name] = index
	}

	// 删除 旧的 index
	for name, _ := range oldIndexMaps {
		if _, ok := indexMaps[name]; !ok && name != "_id_" {
			updated = append(updated, "del."+name)
			if err = collection.DropIndexName(name); err != nil {
				return
			}
		}
	}

	// 设置新的 index
	for name, index := range indexMaps {
		if oldIndex, ok := oldIndexMaps[name]; ok && reflect.DeepEqual(oldIndex, index) {
			continue
		}
		collection.DropIndexName(name)
		if err = collection.EnsureIndex(index); err != nil {
			return
		}
		updated = append(updated, "set."+name)
	}
	return
}

func (model *Model) Drop(ctx context.Context) (err error) {
	err = model.DB(ctx).DropCollection()
	return
}

func (model *Model) Query(ctx context.Context) (query *Query) {
	query = &Query{Context: ctx, Model: model, GetFunc: model.GetFunc, Options: QueryOptions{Name: model.Name}}
	return
}

func (model *Model) DocumentStruct() DocumentStruct {
	documentStruct, err := DocumentStructParse(reflect.TypeOf(model.Document))
	if err != nil {
		log.Panicln(err)
	}
	return documentStruct
}
