package model

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

type (
	QueryVariable string

	QueryOptions struct {
		Name   string                 `json:"name"`
		Fields map[string]interface{} `json:"fields,omitempty"`
		Sort   []string               `json:"sort,omitempty"`
		Skip   int                    `json:"skip,omitempty"`
		Limit  int                    `json:"limit,omitempty"`
		Hint   []string               `json:"hint,omitempty"`
		Trash  int                    `json:"trashed,omitempty"`
	}

	Query struct {
		Context  context.Context
		Model    ModelInterface
		GetFunc  func(value interface{}, query *Query) *Query
		Query    map[string]interface{}
		Populate Populate
		Options  QueryOptions
	}
)

func (query *Query) Find(value map[string]interface{}) *Query {
	query.Query = value
	return query
}

func (query *Query) Name(name string, operator string, value interface{}) *Query {
	if strings.HasPrefix(name, "$") {
		name = "..invalid$" + name
	}
	switch operator {
	case "":
	case "=":
	case "==":
		operator = "eq"
	case "!=":
		operator = "ne"
	case ">":
		operator = "gt"
	case ">=":
	case "=>":
		operator = "gte"
	case "<":
		operator = "lt"
	case "<=":
	case "=<":
		operator = "lte"
	}
	if query.Query == nil {
		query.Query = map[string]interface{}{}
	}
	if val, ok := query.Query[name].(map[string]interface{}); ok {
		val[operator] = value
		query.Query[name] = val
	} else {
		query.Query[name] = map[string]interface{}{operator: value}
	}
	return query
}

func (query *Query) Get(value interface{}) *Query {
	if query.GetFunc != nil {
		return query.GetFunc(value, query)
	}
	return query.ID(value)
}

func (query *Query) ID(value interface{}) *Query {
	if value == nil {
		value = ""
	}
	switch value.(type) {
	case string:
		hex := value.(string)
		if bson.IsObjectIdHex(hex) {
			value = bson.ObjectIdHex(hex)
		}
	}
	return query.Name("_id", "eq", value)
}

func (query *Query) Eq(name string, value interface{}) *Query {
	return query.Name(name, "eq", value)
}
func (query *Query) EqDeleted() *Query {
	documentStruct := query.Model.DocumentStruct()
	if tag, ok := documentStruct["Deleted"]; ok && tag.BSON != "" {
		return query.Name(tag.BSON, "eq", true)
	}
	if tag, ok := documentStruct["DeletedAt"]; ok && tag.BSON != "" {
		return query.Name(tag.BSON, "$exists", true)
	}
	return query
}

func (query *Query) NeDeleted() *Query {
	documentStruct := query.Model.DocumentStruct()
	if tag, ok := documentStruct["Deleted"]; ok && tag.BSON != "" {
		return query.Name(tag.BSON, "eq", false)
	}
	if tag, ok := documentStruct["DeletedAt"]; ok && tag.BSON != "" {
		return query.Name(tag.BSON, "$exists", false)
	}
	return query
}

func (query *Query) Ne(name string, value interface{}) *Query {
	return query.Name(name, "ne", value)
}

func (query *Query) Gt(name string, value interface{}) *Query {
	return query.Name(name, "gt", value)
}

func (query *Query) Gte(name string, value interface{}) *Query {
	return query.Name(name, "gte", value)
}
func (query *Query) Lt(name string, value interface{}) *Query {
	return query.Name(name, "lt", value)
}

func (query *Query) Lte(name string, value interface{}) *Query {
	return query.Name(name, "lte", value)
}
func (query *Query) In(name string, value interface{}) *Query {
	return query.Name(name, "in", value)
}
func (query *Query) Nin(name string, value interface{}) *Query {
	return query.Name(name, "nin", value)
}

func (query *Query) Regex(name string, pattern string, options string) *Query {
	return query.Name(name, "regex", bson.RegEx{pattern, options})
}

func (query *Query) Fields(fields map[string]interface{}) *Query {
	query.Options.Fields = fields
	return query
}

func (query *Query) Sort(fields ...string) *Query {
	query.Options.Sort = fields
	return query
}

func (query *Query) Skip(skip int) *Query {
	query.Options.Skip = skip
	return query
}

func (query *Query) Limit(limit int) *Query {
	query.Options.Limit = limit
	return query
}

func (query *Query) Hint(indexKey ...string) *Query {
	query.Options.Hint = indexKey
	return query
}

func (query *Query) Trash(trash int) *Query {
	query.Options.Trash = trash
	return query
}

func (query *Query) PopulatePath(path string, value *Query) *Query {
	if query.Populate == nil {
		query.Populate = Populate{}
	}
	query.Populate[path] = value
	return query
}

func (query *Query) One(document interface{}) (err error) {
	if err = query.Model.DB(query.Context).Find(query.Map()).Select(query.Options.Fields).Sort(query.Options.Sort...).Skip(query.Options.Skip).Limit(1).One(document); err != nil {
		err = mongoError(err)
		return
	}
	if query.Populate != nil {
		err = query.Populate.One(document)
	}
	return
}

func (query *Query) All(documents interface{}) (err error) {
	if err = query.Model.DB(query.Context).Find(query.Map()).Select(query.Options.Fields).Sort(query.Options.Sort...).Skip(query.Options.Skip).Limit(query.Options.Limit).All(documents); err != nil {
		err = mongoError(err)
		return
	}
	if query.Populate != nil {
		err = query.Populate.All(documents)
	}
	return
}

func (query *Query) Count() (n int, err error) {
	n, err = query.Model.DB(query.Context).Find(query.Map()).Skip(query.Options.Skip).Limit(query.Options.Limit).Count()
	err = mongoError(err)
	return
}

func (query *Query) Explain() (result map[string]interface{}, err error) {
	result = map[string]interface{}{}
	err = query.Model.DB(query.Context).Find(query.Map()).Select(query.Options.Fields).Sort(query.Options.Sort...).Skip(query.Options.Skip).Limit(query.Options.Limit).Explain(result)
	err = mongoError(err)
	return
}

func (query *Query) Update(update interface{}) (err error) {
	err = query.Model.DB(query.Context).Update(query.Map(), update)
	err = mongoError(err)
	return
}

func (query *Query) UpdateAll(update interface{}) (i int, err error) {
	var info *mgo.ChangeInfo
	if info, err = query.Model.DB(query.Context).UpdateAll(query.Map(), update); err != nil {
		err = mongoError(err)
		return
	}
	i = info.Updated
	if i == 0 && info.Matched != 0 {
		i = info.Matched
	}
	return
}

func (query *Query) UpdateAndFind(update interface{}, document interface{}, isNew bool) (err error) {
	if _, err = query.Model.DB(query.Context).Find(query.Map()).Select(query.Options.Fields).Sort(query.Options.Sort...).Skip(query.Options.Skip).Limit(query.Options.Limit).Apply(mgo.Change{Update: update, ReturnNew: isNew}, document); err != nil {
		err = mongoError(err)
		return
	}
	return
}

func (query *Query) Delete() (err error) {
	if update := query.deleteUpdate(); update != nil {
		err = query.Model.DB(query.Context).Update(query.Map(), update)
		err = mongoError(err)
		return
	}
	return query.ForceDelete()
}

func (query *Query) DeleteAll() (i int, err error) {
	if update := query.deleteUpdate(); update != nil {
		var info *mgo.ChangeInfo
		if info, err = query.Model.DB(query.Context).UpdateAll(query.Map(), update); err != nil {
			err = mongoError(err)
			return
		}
		i = info.Updated
		if i == 0 && info.Matched != 0 {
			i = info.Matched
		}
		return
	}
	return query.ForceDeleteAll()
}

func (query *Query) ForceDelete() (err error) {
	err = query.Model.DB(query.Context).Remove(query.Map())
	err = mongoError(err)
	return
}

func (query *Query) ForceDeleteAll() (i int, err error) {
	var info *mgo.ChangeInfo
	if info, err = query.Model.DB(query.Context).RemoveAll(query.Map()); err != nil {
		err = mongoError(err)
		return
	}
	i = info.Removed

	if i == 0 && info.Matched != 0 {
		i = info.Matched
	}
	return
}

func (query *Query) Restore() (err error) {
	if update := query.restoreUpdate(); update != nil {
		err = query.Model.DB(query.Context).Update(query.Map(), update)
		err = mongoError(err)
	} else {
		err = ErrNotFound
	}
	return
}

func (query *Query) RestoreAll() (i int, err error) {
	if update := query.restoreUpdate(); update != nil {
		var info *mgo.ChangeInfo
		if info, err = query.Model.DB(query.Context).UpdateAll(query.Map(), update); err != nil {
			err = mongoError(err)
			return
		}
		i = info.Updated
		if i == 0 && info.Matched != 0 {
			i = info.Matched
		}
	} else {
		err = ErrNotFound
	}
	return
}

func (query *Query) Map() (maps bson.M) {
	maps = bson.M{}
	var expr []map[string]interface{}
	for name, value := range query.Query {

		// 直接是变量
		if strings.HasPrefix(name, "$") {
			maps[name] = value
			continue
		}

		// 各种运算符
		if valueMap, ok := value.(map[string]interface{}); ok {
			for operator, val := range valueMap {
				//  运算符 没有 $ 的
				if !strings.HasPrefix(operator, "$") {
					operator = "$" + operator
				}
				// 写入
				switch val.(type) {
				case QueryVariable:
					expr = append(expr, map[string]interface{}{operator: []string{"$" + name, "$" + val.(string)}})
				default:
					// 只有 一个 eq 运算符
					if operator == "$eq" && len(valueMap) == 1 && reflect.TypeOf(val).Kind() != reflect.Map {
						maps[name] = val
						break
					}
					// 多个
					if smaps, ok := maps[name].(map[string]interface{}); ok {
						smaps[operator] = val
						maps[name] = smaps
					} else {
						maps[name] = map[string]interface{}{operator: val}
					}
				}
			}
			continue
		}
		// 直接写入到  map
		maps[name] = value
	}

	// expr
	if len(expr) != 0 {
		var mapexpr map[string]interface{}
		var ok bool
		if mapexpr, ok = maps["$expr"].(map[string]interface{}); !ok {
			mapexpr = map[string]interface{}{}
		}

		var and []interface{}
		if and, ok = mapexpr["$and"].([]interface{}); ok {
			for _, val := range expr {
				and = append(and, val)
			}
		} else {
			mapexpr = map[string]interface{}{}
			and = append(and, mapexpr)
		}
		mapexpr["$and"] = and
		maps["$expr"] = mapexpr
	}

	// 回收站 过滤器
	if query.Options.Trash > 0 {
		documentStruct := query.Model.DocumentStruct()
		if tag, ok := documentStruct["Deleted"]; ok && tag.BSON != "" {
			if _, ok = maps[tag.BSON]; !ok {
				maps[tag.BSON] = true
			}
		} else if tag, ok := documentStruct["DeletedAt"]; ok && tag.BSON != "" {
			if _, ok = maps[tag.BSON]; !ok {
				maps[tag.BSON] = map[string]interface{}{"$exists": true}
			}
		}
	} else if query.Options.Trash < 0 {
		documentStruct := query.Model.DocumentStruct()
		if tag, ok := documentStruct["Deleted"]; ok && tag.BSON != "" {
			if _, ok = maps[tag.BSON]; !ok {
				maps[tag.BSON] = false
			}
		} else if tag, ok := documentStruct["DeletedAt"]; ok && tag.BSON != "" {
			if _, ok = maps[tag.BSON]; !ok {
				maps[tag.BSON] = map[string]interface{}{"$exists": false}
			}
		}
	}
	return
}

func (query *Query) deleteUpdate() (update bson.M) {
	documentStruct := query.Model.DocumentStruct()
	set := bson.M{}
	if tag, ok := documentStruct["Deleted"]; ok && tag.BSON != "" {
		set[tag.BSON] = true
	}
	if tag, ok := documentStruct["DeletedAt"]; ok && tag.BSON != "" {
		set[tag.BSON] = time.Now()
	}
	if len(set) != 0 {
		update = map[string]interface{}{"$set": set}
	}
	return
}

func (query *Query) restoreUpdate() (update bson.M) {
	documentStruct := query.Model.DocumentStruct()
	if tag, ok := documentStruct["Deleted"]; ok && tag.BSON != "" {
		update = map[string]interface{}{}
		update["$set"] = map[string]interface{}{tag.BSON: false}
	}
	if tag, ok := documentStruct["DeletedAt"]; ok && tag.BSON != "" {
		update = map[string]interface{}{}
		if update == nil {
			update = map[string]interface{}{}
		}
		update["$unset"] = map[string]interface{}{tag.BSON: 1}
	}
	return
}

func mongoError(err error) error {
	if err == mgo.ErrNotFound {
		err = ErrNotFound
	}
	return err
}
