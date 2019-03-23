package model

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

type (
	Populate     map[string]*Query
	reflectValue struct {
		key    reflect.Value
		parent reflect.Value
		value  reflect.Value
	}
	reflectPath struct {
		key   reflect.Value
		index int
	}
)

func (populate Populate) One(document interface{}) (err error) {
	documents := &[]interface{}{document}
	return populate.All(documents)
}

func (populate Populate) All(documents interface{}) (err error) {
	documentsv := reflect.ValueOf(documents)

	// 必须是指针
	if documentsv.Kind() != reflect.Ptr {
		err = errors.New("documents argument must be a slice address")
		return
	}
	slicev := documentsv.Elem()
	if slicev.Kind() == reflect.Interface {
		slicev = slicev.Elem()
	}

	// 必须是切片
	if slicev.Kind() != reflect.Slice {
		err = errors.New("documents argument must be a slice address")
		return
	}

	// 0 长度的 不处理
	if slicev.Len() == 0 {
		return
	}

	// 解析单个 document 格式
	documentV := slicev.Index(0)
	if documentV.Kind() == reflect.Interface {
		documentV = documentV.Elem()
	}
	var documentStruct DocumentStruct
	if documentStruct, err = DocumentStructParse(documentV.Type()); err != nil {
		return
	}
	// 遍历填充
	chans := make(chan error, len(populate))
	for path, query := range populate {
		findPath := ""
		findValue := map[interface{}][]reflectValue{}
		names := strings.Split(path, ".")
		for i := 0; i < slicev.Len(); i++ {
			documentV = slicev.Index(i)
			if documentV.Kind() == reflect.Interface {
				documentV = documentV.Elem()
			}
			if findPath, err = reflectFind(findValue, names, documentV, documentStruct); err != nil {
				return
			}
		}
		if len(findValue) != 0 {
			var names []string
			if findPath != "" {
				names = strings.Split(findPath, ".")
			}
			// if err = reflectPopulate(names, findValue, query); err != nil {
			// 	return
			// }
			go reflectPopulateChan(names, findValue, query, chans)
		} else {
			chans <- nil
		}
	}

	for i := 0; i < len(populate); i++ {
		val := <-chans
		if err == nil && val != nil {
			err = val
		}
	}
	close(chans)
	return
}

func reflectPopulateChan(findPath []string, findValueMap map[interface{}][]reflectValue, query *Query, chans chan error) {
	chans <- reflectPopulate(findPath, findValueMap, query)
}

func reflectPopulate(findPath []string, findValueMap map[interface{}][]reflectValue, query *Query) (err error) {

	// 切片类型 切片 切片指针
	var sliceTyp reflect.Type
	var sliceVal reflect.Value
	var slicePtr reflect.Value
	for _, values := range findValueMap {
		sliceTyp = values[0].value.Type()
		if sliceTyp.Kind() == reflect.Slice {
			sliceTyp = sliceTyp.Elem()
		}
		sliceVal = reflect.MakeSlice(reflect.SliceOf(sliceTyp), 0, 0)
		slicePtr = reflect.New(sliceVal.Type())
		slicePtr.Elem().Set(sliceVal)
		break
	}

	var findValue []interface{}
	existsValue := sliceVal
	for key, values := range findValueMap {
		var skip bool
		for _, value := range values {
			if reflectValueEmpty(value.value) {
				continue
			}
			if value.value.Type() == sliceTyp {
				existsValue = reflect.Append(existsValue, value.value)
				skip = true
				break
			}
			if value.value.Kind() == reflect.Slice && value.value.Type().Elem() == sliceTyp {
				for i := 0; i < value.value.Len(); i++ {
					existsValue = reflect.Append(existsValue, value.value.Index(i))
				}
			}
		}
		if skip {
			continue
		}
		findValue = append(findValue, key)
	}

	// 已存在的
	if existsValue.Len() != 0 {
		if query.Populate != nil {
			slicePtr.Elem().Set(existsValue)
			query.Populate.All(slicePtr.Interface())
		}
		return
	}

	if len(findValue) == 0 {
		return
	}

	findReflectPath := []reflectPath{}
	if sliceTyp.Kind() == reflect.Struct || (sliceTyp.Kind() == reflect.Ptr && sliceTyp.Elem().Kind() == reflect.Struct) {
		var documentStruct DocumentStruct
		if documentStruct, err = DocumentStructParse(sliceTyp); err != nil {
			return
		}
		for i, key := range findPath {
			field, ok := documentStruct[key]
			if !ok {
				err = fmt.Errorf("Path not found {%s}", findPath)
				return
			}
			if field.BSON != "" {
				key = field.BSON
			} else if field.JSON != "" {
				key = field.JSON
			}
			documentStruct = field.Children
			findReflectPath = append(findReflectPath, reflectPath{key: reflect.ValueOf(key), index: field.Index})
			findPath[i] = key
		}
	} else {
		for _, val := range findPath {
			findReflectPath = append(findReflectPath, reflectPath{key: reflect.ValueOf(val), index: 0})
		}
	}

	sliceVal = reflect.MakeSlice(reflect.SliceOf(sliceTyp), 0, 0)
	slicePtr.Elem().Set(sliceVal)
	query.In(strings.Join(findPath, "."), findValue).All(slicePtr.Interface())

	sliceVal = slicePtr.Elem()
	for i := 0; i < sliceVal.Len(); i++ {
		field := sliceVal.Index(i)
		if err = reflectSet(field, field, findReflectPath, findValueMap); err != nil {
			return
		}
	}
	return
}

func reflectSet(value reflect.Value, field reflect.Value, fieldPath []reflectPath, findValueMap map[interface{}][]reflectValue) (err error) {
	if field.Kind() == reflect.Ptr {
		field = value.Elem()
	}
	path := fieldPath[0]
	if len(fieldPath) != 1 {
		switch field.Kind() {
		case reflect.Slice:
			for i := 0; i < field.Len(); i++ {
				if err = reflectSet(value, field.Index(i), fieldPath, findValueMap); err != nil {
					return
				}
			}
		case reflect.Map:
			field := field.MapIndex(path.key)
			if !field.IsValid() {
				err = fmt.Errorf("populate not key %v", path.key.Interface())
				return
			}
			if err = reflectSet(value, field, fieldPath[1:], findValueMap); err != nil {
				return
			}
		case reflect.Struct:
			field := field.Field(path.index)
			if !field.IsValid() {
				err = fmt.Errorf("populate not key %v", path.key.Interface())
				return
			}
			if err = reflectSet(value, field, fieldPath[1:], findValueMap); err != nil {
				return
			}
		default:
			err = fmt.Errorf("populate not key %v", path.key.Interface())
			return
		}
	} else {
		var idv reflect.Value
		switch field.Kind() {
		case reflect.Map:
			idv = field.MapIndex(path.key)
		case reflect.Struct:
			idv = field.Field(path.index)
		default:
			err = fmt.Errorf("populate not struct, map %s", field.Type().Kind().String())
			return
		}
		if !idv.IsValid() {
			err = fmt.Errorf("populate not key %v", path.key)
		}
		if idv.Kind() == reflect.Ptr {
			idv = idv.Elem()
		}
		ids := []interface{}{}
		if idv.Kind() == reflect.Slice {
			for i := 0; i < field.Len(); i++ {
				ids = append(ids, field.Index(i).Interface())
			}
		} else {
			ids = append(ids, idv.Interface())
		}
		var inc int
		for _, id := range ids {
			if findValues, ok := findValueMap[id]; ok {
				for _, findValue := range findValues {
					switch findValue.value.Kind() {
					case reflect.Ptr:
						findValue.value.Set(value)
					case reflect.Map:
						findValue.value.SetMapIndex(findValue.key, value)
					case reflect.Slice:
						findValue.value.Set(reflect.Append(findValue.value, value))
					default:
						findValue.value.Set(value)
					}
				}
				inc++
			}
		}
		if inc == 0 {
			err = fmt.Errorf("populate id not exists %v", ids)
			return
		}
	}
	return
}

func reflectFind(maps map[interface{}][]reflectValue, path []string, value reflect.Value, valueStruct DocumentStruct) (findPath string, err error) {
	name := path[0]

	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	if !value.IsValid() {
		return
	}
	if value.Kind() != reflect.Struct {
		err = fmt.Errorf("populate: path (%s) not struct  (%s)", name, value.Kind().String())
		return
	}

	var ok bool
	var fieldStruct DocumentStructField
	if fieldStruct, ok = valueStruct[name]; !ok {
		err = fmt.Errorf("populate: path (%s) not found", name)
		return
	}
	field := value.Field(fieldStruct.Index)

	if len(path) != 1 {
		fieldPath := path[1:]
		switch field.Kind() {
		case reflect.Map:
			for _, key := range field.MapKeys() {
				if findPath, err = reflectFind(maps, fieldPath, field.MapIndex(key), fieldStruct.Children); err != nil {
					return
				}
			}
		case reflect.Slice:
			for i := 0; i < field.Len(); i++ {
				if findPath, err = reflectFind(maps, fieldPath, field.Index(i), fieldStruct.Children); err != nil {
					return
				}
			}
		default:
			if findPath, err = reflectFind(maps, fieldPath, field, fieldStruct.Children); err != nil {
				return
			}
		}
	} else {
		if fieldStruct.Populate.Name == "" {
			err = fmt.Errorf("populate: path (%s) tag is empty", name)
			return
		}

		findPath = fieldStruct.Populate.Find

		// if !reflectValueEmpty(field) {
		// 	return
		// }

		idValue := value.Field(fieldStruct.Populate.Index)
		if reflectValueEmpty(idValue) {
			return
		}

		switch idValue.Kind() {
		case reflect.Map:
			for _, key := range idValue.MapKeys() {
				if val := idValue.MapIndex(key); !reflectValueEmpty(val) {
					id := val.Interface()
					maps[id] = append(maps[id], reflectValue{key: key, value: field})
				}
			}
		case reflect.Slice:
			for i := 0; i < idValue.Len(); i++ {
				if val := idValue.Index(i); !reflectValueEmpty(val) {
					id := val.Interface()
					maps[id] = append(maps[id], reflectValue{value: field})
				}
			}
		default:
			id := idValue.Interface()
			maps[id] = append(maps[id], reflectValue{value: field})
		}
	}
	return
}

func reflectValueEmpty(value reflect.Value) bool {
	if !value.IsValid() {
		return true
	}
	switch value.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice:
		return value.IsNil()
	case reflect.String:
		return value.String() == ""
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int() == 0
	default:
		return true
	}
}
