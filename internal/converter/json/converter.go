// Copyright 2022-2023 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package json

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/valyala/fastjson"

	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
)

type Converter struct{}

var converter = &Converter{}

func GetConverter() (message.Converter, error) {
	return converter, nil
}

func (c *Converter) Encode(d interface{}) ([]byte, error) {
	return json.Marshal(d)
}

func (c *Converter) Decode(b []byte) (interface{}, error) {
	var r0 interface{}
	err := json.Unmarshal(b, &r0)
	if err != nil {
		return nil, err
	}
	return r0, nil
}

type FastJsonConverter struct {
	sync.RWMutex
	isSchemaLess bool
	// ruleID -> schema
	schemaMap   map[string]map[string]*ast.JsonStreamField
	schema      map[string]*ast.JsonStreamField
	wildcardMap map[string]struct{}
}

func NewFastJsonConverter(key string, schema map[string]*ast.JsonStreamField, isSchemaLess bool) *FastJsonConverter {
	f := &FastJsonConverter{
		schemaMap:    make(map[string]map[string]*ast.JsonStreamField),
		schema:       schema,
		wildcardMap:  make(map[string]struct{}),
		isSchemaLess: isSchemaLess,
	}
	f.schemaMap[key] = schema
	return f
}

func (c *FastJsonConverter) MergeSchema(key string, newSchema map[string]*ast.JsonStreamField, isWildcard bool) error {
	c.Lock()
	defer c.Unlock()
	_, ok := c.schemaMap[key]
	if ok {
		return nil
	}
	c.schemaMap[key] = newSchema
	if isWildcard {
		c.wildcardMap[key] = struct{}{}
	} else {
		mergedSchema, err := mergeSchema(c.schema, newSchema)
		if err != nil {
			return err
		}
		c.schema = mergedSchema
	}
	return nil
}

func (c *FastJsonConverter) DetachSchema(key string) error {
	var err error
	c.Lock()
	defer c.Unlock()
	_, ok := c.schemaMap[key]
	if ok {
		delete(c.wildcardMap, key)
		delete(c.schemaMap, key)
		newSchema := make(map[string]*ast.JsonStreamField)
		for _, schema := range c.schemaMap {
			newSchema, err = mergeSchema(newSchema, schema)
			if err != nil {
				return err
			}
		}
		c.schema = newSchema
	}
	return nil
}

func mergeSchema(originSchema, newSchema map[string]*ast.JsonStreamField) (map[string]*ast.JsonStreamField, error) {
	resultSchema := make(map[string]*ast.JsonStreamField)
	for ruleID, oldSchemaField := range originSchema {
		resultSchema[ruleID] = oldSchemaField
	}
	for ruleID, newSchemaField := range newSchema {
		oldSchemaField, ok := originSchema[ruleID]
		if ok {
			switch {
			case oldSchemaField != nil && newSchemaField != nil:
				if oldSchemaField.Type != newSchemaField.Type {
					return nil, fmt.Errorf("column field type %v between current[%v] and new[%v] are not equal", ruleID, oldSchemaField.Type, newSchemaField.Type)
				}
				switch oldSchemaField.Type {
				case "struct":
					subResultSchema, err := mergeSchema(oldSchemaField.Properties, newSchemaField.Properties)
					if err != nil {
						return nil, err
					}
					resultSchema[ruleID].Properties = subResultSchema
				case "array":
					if oldSchemaField.Items.Type != newSchemaField.Items.Type {
						return nil, fmt.Errorf("array column field type %v between current[%v] and new[%v] are not equal", ruleID, oldSchemaField.Items.Type, newSchemaField.Items.Type)
					}
					if oldSchemaField.Items.Type == "struct" {
						subResultSchema, err := mergeSchema(oldSchemaField.Items.Properties, newSchemaField.Items.Properties)
						if err != nil {
							return nil, err
						}
						resultSchema[ruleID].Items.Properties = subResultSchema
					}
				}
			case oldSchemaField != nil && newSchemaField == nil:
				return nil, fmt.Errorf("array column field type %v between current[%v] and new[%v] are not equal", ruleID, oldSchemaField.Items.Type, "any")
			case oldSchemaField == nil && newSchemaField != nil:
				return nil, fmt.Errorf("array column field type %v between current[%v] and new[%v] are not equal", ruleID, "any", newSchemaField.Items.Type)
			case oldSchemaField == nil && newSchemaField == nil:
			}
			continue
		}
		resultSchema[ruleID] = newSchemaField
	}
	return resultSchema, nil
}

func (c *FastJsonConverter) Encode(d interface{}) ([]byte, error) {
	return json.Marshal(d)
}

func (c *FastJsonConverter) Decode(b []byte) (interface{}, error) {
	c.RLock()
	defer c.RUnlock()
	if len(c.wildcardMap) > 0 {
		return converter.Decode(b)
	}
	return c.decodeWithSchema(b, c.schema)
}

func (f *FastJsonConverter) decodeWithSchema(b []byte, schema map[string]*ast.JsonStreamField) (interface{}, error) {
	var p fastjson.Parser
	v, err := p.ParseBytes(b)
	if err != nil {
		return nil, err
	}
	switch v.Type() {
	case fastjson.TypeArray:
		array, err := v.Array()
		if err != nil {
			return nil, err
		}
		ms := make([]map[string]interface{}, len(array))
		for i, v := range array {
			obj, err := v.Object()
			if err != nil {
				return nil, err
			}
			subMap, err := f.decodeObject(obj, schema)
			if err != nil {
				return nil, err
			}
			ms[i] = subMap
		}
		return ms, nil
	case fastjson.TypeObject:
		obj, err := v.Object()
		if err != nil {
			return nil, err
		}
		m, err := f.decodeObject(obj, schema)
		if err != nil {
			return nil, err
		}
		return m, nil
	}
	return nil, fmt.Errorf("only map[string]interface{} and []map[string]interface{} is supported")
}

func (f *FastJsonConverter) decodeArray(array []*fastjson.Value, field *ast.JsonStreamField) ([]interface{}, error) {
	if !f.isSchemaLess && field == nil {
		return nil, nil
	}
	vs := make([]interface{}, len(array))
	for i, item := range array {
		switch item.Type() {
		case fastjson.TypeNull:
			vs[i] = nil
		case fastjson.TypeObject:
			if field != nil && field.Type != "struct" {
				return nil, fmt.Errorf("array has wrong type:%v, expect:%v", fastjson.TypeObject.String(), field.Type)
			}
			childObj, err := item.Object()
			if err != nil {
				return nil, err
			}
			var props map[string]*ast.JsonStreamField
			if field != nil {
				props = field.Properties
			}
			subMap, err := f.decodeObject(childObj, props)
			if err != nil {
				return nil, err
			}
			if subMap != nil {
				vs[i] = subMap
			}
		case fastjson.TypeArray:
			if field != nil && field.Type != "array" {
				return nil, fmt.Errorf("array has wrong type:%v, expect:%v", fastjson.TypeArray.String(), field.Type)
			}
			childArrays, err := item.Array()
			if err != nil {
				return nil, err
			}
			var items *ast.JsonStreamField
			if field != nil {
				items = field.Items
			}
			subList, err := f.decodeArray(childArrays, items)
			if err != nil {
				return nil, err
			}
			if subList != nil {
				vs[i] = subList
			}
		case fastjson.TypeString:
			v, err := f.extractStringValue("array", item, field)
			if err != nil {
				return nil, err
			}
			if v != nil {
				vs[i] = v
			}
		case fastjson.TypeNumber:
			v, err := f.extractNumberValue("array", item, field)
			if err != nil {
				return nil, err
			}
			if v != nil {
				vs[i] = v
			}
		case fastjson.TypeTrue, fastjson.TypeFalse:
			v, err := f.extractBooleanFromValue("array", item, field)
			if err != nil {
				return nil, err
			}
			if v != nil {
				vs[i] = v
			}
		}
	}
	return vs, nil
}

func (f *FastJsonConverter) decodeObject(obj *fastjson.Object, schema map[string]*ast.JsonStreamField) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	var err error
	obj.Visit(func(k []byte, v *fastjson.Value) {
		key := string(k)
		switch v.Type() {
		case fastjson.TypeNull:
			m[key] = nil
		case fastjson.TypeObject:
			if f.checkSchema(key, "struct", schema) {
				childObj, err2 := v.Object()
				if err2 != nil {
					err = err2
					return
				}
				var props map[string]*ast.JsonStreamField
				if schema != nil && schema[key] != nil {
					props = schema[key].Properties
				}
				childMap, err2 := f.decodeObject(childObj, props)
				if err2 != nil {
					err = err2
					return
				}
				if childMap != nil {
					m[key] = childMap
				}
			} else {
				err = fmt.Errorf("%v has wrong type:%v, expect:%v", key, v.Type().String(), getType(schema[key]))
				return
			}
		case fastjson.TypeArray:
			if f.checkSchema(key, "array", schema) {
				childArray, err2 := v.Array()
				if err2 != nil {
					err = err2
					return
				}
				var items *ast.JsonStreamField
				if schema != nil && schema[key] != nil {
					items = schema[key].Items
				}
				subList, err2 := f.decodeArray(childArray, items)
				if err2 != nil {
					err = err2
					return
				}
				if subList != nil {
					m[key] = subList
				}
			} else {
				err = fmt.Errorf("%v has wrong type:%v, expect:%v", key, v.Type().String(), getType(schema[key]))
				return
			}
		case fastjson.TypeString:
			var field *ast.JsonStreamField
			if schema != nil {
				field = schema[key]
			}
			v, err2 := f.extractStringValue(key, v, field)
			if err2 != nil {
				err = err2
				return
			}
			if v != nil {
				m[key] = v
			}
		case fastjson.TypeNumber:
			var field *ast.JsonStreamField
			if schema != nil {
				field = schema[key]
			}
			v, err2 := f.extractNumberValue(key, v, field)
			if err2 != nil {
				err = err2
				return
			}
			if v != nil {
				m[key] = v
			}
		case fastjson.TypeTrue, fastjson.TypeFalse:
			var field *ast.JsonStreamField
			if schema != nil {
				field = schema[key]
			}
			v, err2 := f.extractBooleanFromValue(key, v, field)
			if err2 != nil {
				err = err2
				return
			}
			if v != nil {
				m[key] = v
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (f *FastJsonConverter) checkSchema(key, typ string, schema map[string]*ast.JsonStreamField) bool {
	if f.isSchemaLess && (schema == nil || (schema[key] == nil)) {
		return true
	}
	if !f.isSchemaLess && schema[key] != nil && schema[key].Type == typ {
		return true
	}
	return false
}

func (f *FastJsonConverter) extractNumberValue(name string, v *fastjson.Value, field *ast.JsonStreamField) (interface{}, error) {
	if f.isSchemaLess && field == nil {
		f64, err := v.Float64()
		if err != nil {
			return nil, err
		}
		return f64, nil
	}
	if !f.isSchemaLess {
		if field == nil {
			return nil, nil
		}
		switch {
		case field.Type == "float", field.Type == "datetime":
			f64, err := v.Float64()
			if err != nil {
				return nil, err
			}
			return f64, nil
		case field.Type == "bigint":
			i64, err := v.Int64()
			if err != nil {
				return nil, err
			}
			return i64, nil
		case field.Type == "string":
			f64, err := v.Float64()
			if err != nil {
				return nil, err
			}
			return cast.ToStringAlways(f64), nil
		case field.Type == "boolean":
			bv, err := getBooleanFromValue(v)
			if err != nil {
				return nil, err
			}
			return bv, nil
		}
	}
	return nil, fmt.Errorf("%v has wrong type:%v, expect:%v", name, fastjson.TypeNumber.String(), getType(field))
}

func (f *FastJsonConverter) extractStringValue(name string, v *fastjson.Value, field *ast.JsonStreamField) (interface{}, error) {
	if f.isSchemaLess && field == nil {
		bs, err := v.StringBytes()
		if err != nil {
			return nil, err
		}
		return string(bs), nil
	}
	if !f.isSchemaLess {
		if field == nil {
			return nil, nil
		}
		switch {
		case field.Type == "string", field.Type == "datetime":
			bs, err := v.StringBytes()
			if err != nil {
				return nil, err
			}
			return string(bs), nil
		case field.Type == "bytea":
			s, err := v.StringBytes()
			if err != nil {
				return nil, err
			}
			return cast.ToByteA(string(s), cast.CONVERT_ALL)
		case field.Type == "boolean":
			return getBooleanFromValue(v)
		}
	}
	return nil, fmt.Errorf("%v has wrong type:%v, expect:%v", name, fastjson.TypeString.String(), getType(field))
}

func (f *FastJsonConverter) extractBooleanFromValue(name string, v *fastjson.Value, field *ast.JsonStreamField) (interface{}, error) {
	if f.isSchemaLess && field == nil {
		s, err := v.Bool()
		if err != nil {
			return nil, err
		}
		return s, nil
	}
	if !f.isSchemaLess {
		if field == nil {
			return nil, nil
		}
		if field.Type == "boolean" {
			s, err := v.Bool()
			if err != nil {
				return nil, err
			}
			return s, nil
		}
	}
	return nil, fmt.Errorf("%v has wrong type:%v, expect:%v", name, v.Type().String(), getType(field))
}

func getBooleanFromValue(value *fastjson.Value) (interface{}, error) {
	typ := value.Type()
	switch typ {
	case fastjson.TypeNumber:
		f64, err := value.Float64()
		if err != nil {
			return false, err
		}
		return cast.ToBool(f64, cast.CONVERT_ALL)
	case fastjson.TypeString:
		s, err := value.StringBytes()
		if err != nil {
			return false, err
		}
		return cast.ToBool(string(s), cast.CONVERT_ALL)
	case fastjson.TypeTrue, fastjson.TypeFalse:
		b, err := value.Bool()
		if err != nil {
			return false, err
		}
		return b, nil
	case fastjson.TypeNull:
		return nil, nil
	}
	return false, fmt.Errorf("wrong type:%v, expect:boolean", typ)
}

func getType(t *ast.JsonStreamField) string {
	if t == nil {
		return "null"
	} else {
		return t.Type
	}
}

func getType2(t *ast.JsonStreamField) string {
	if t == nil {
		return "null"
	} else {
		return t.Type
	}
}
