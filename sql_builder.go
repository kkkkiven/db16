package db

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/big"
	"reflect"
	"strconv"
	"strings"
)

const (
	_ = iota
	TypeInsert
	TypeDelete
	TypeUpdate
	TypeSelect
	TypeInsertUpdate
)

var (
	WrapSymbol = "`"
	DBType     = "mysql"
)

// SQL语句构造结构
type SB struct {
	db                                       *Database
	t                                        int
	field, table, where, group, order, limit string
	values                                   SBValues
	values2                                  SBValues
	ignore                                   bool
	fullsql                                  bool
	debug                                    bool
	unsafe                                   bool //是否进行安全检查, 专门针对无限定的UPDATE和DELETE进行二次验证
	args                                     []interface{}
}

// Exec返回结果
type SBResult struct {
	Success  bool   //语句是否执行成功
	Code     int    //错误代码
	Msg      string //错误提示信息
	LastID   int64  //最后产生的ID
	Affected int64  //受影响的行数
	Sql      string //最后执行的SQL
}

// 值对象
type SBValues map[string]interface{}

// 增量值
type IncVal struct {
	Val       int64
	BaseField string // 为空表示对当前字段累加
}

// 向值对象中加入值
func (v SBValues) Add(key string, val interface{}) {
	v[key] = val
}

// 删除值对象中的某个值
func (v SBValues) Del(key string) {
	delete(v, key)
}

// 判断指定键是否存在
func (v SBValues) IsExist(key string) bool {
	if _, exist := v[key]; exist {
		return true
	}
	return false
}

// 获取键的整形值
func (v SBValues) Get(key string) interface{} {
	if val, exist := v[key]; exist {
		return val
	}
	return nil
}

// 获取键的字符串值
func (v SBValues) GetString(key string) string {
	if val, exist := v[key]; exist {
		if trueVal, ok := val.(string); ok {
			return trueVal
		}
	}
	return ""
}

// 获取键的整形值
func (v SBValues) GetInt(key string) int {
	if val, exist := v[key]; exist {
		if trueVal, ok := val.(int); ok {
			return trueVal
		}
	}
	return 0
}

// 获取键的无符号整形值
func (v SBValues) GetUint(key string) uint {
	if val, exist := v[key]; exist {
		if trueVal, ok := val.(uint); ok {
			return trueVal
		}
	}
	return 0
}

// 获取键的64位整形值
func (v SBValues) GetInt64(key string) int64 {
	if val, exist := v[key]; exist {
		if trueVal, ok := val.(int64); ok {
			return trueVal
		}
	}
	return 0
}

// 返回绑定完参数的完整的SQL语句
func FullSql(str string, args ...interface{}) (string, error) {
	if !strings.Contains(str, "?") {
		return str, nil
	}
	sons := strings.Split(str, "?")

	var ret string
	var argIndex int
	var maxArgIndex = len(args)

	for _, son := range sons {
		ret += son

		if argIndex < maxArgIndex {
			switch v := args[argIndex].(type) {
			case int:
				ret += strconv.Itoa(v)
			case int8:
				ret += strconv.Itoa(int(v))
			case int16:
				ret += strconv.Itoa(int(v))
			case int32:
				ret += I64toA(int64(v))
			case int64:
				ret += I64toA(v)
			case uint:
				ret += UitoA(v)
			case uint8:
				ret += UitoA(uint(v))
			case uint16:
				ret += UitoA(uint(v))
			case uint32:
				ret += Ui32toA(v)
			case uint64:
				ret += Ui64toA(v)
			case float32:
				ret += F32toA(v)
			case float64:
				ret += F64toA(v)
			case *big.Int:
				ret += v.String()
			case bool:
				if v {
					ret += "true"
				} else {
					ret += "false"
				}
			case string:
				ret += "'" + strings.Replace(strings.Replace(v, "'", "", -1), `\`, `\\`, -1) + "'"
			case nil:
				ret += "NULL"
			default:
				return "", errors.New(fmt.Sprintf("invalid sql argument type: %v => %v (sql: %s)", reflect.TypeOf(v).String(), v, str))
			}

			argIndex++
		}
	}

	return ret, nil
}

// 构建SQL语句
// param: returnFullSql 是否返回完整的sql语句(即:绑定参数之后的语句)
func (q *SB) ToSql(returnFullSql ...bool) (str string, err error) {
	q.args = make([]interface{}, 0)

	switch q.t {
	case TypeInsert:
		if q.table == "" {
			err = errors.New("table cannot be empty")
			return
		}
		if len(q.values) == 0 {
			err = errors.New("values cannot be empty")
			return
		}
		if q.ignore {
			str = "INSERT IGNORE INTO " + q.table
		} else {
			str = "INSERT INTO " + q.table
		}
		var fields, placeholder string
		for k, v := range q.values {
			fields += "," + WrapSymbol + k + WrapSymbol
			placeholder += ",?"
			if iv, ok := v.(IncVal); ok {
				q.args = append(q.args, iv.Val)
			} else {
				q.args = append(q.args, v)
			}
		}
		str += " (" + Substr(fields, 1) + ") VALUES (" + Substr(placeholder, 1) + ")"
	case TypeDelete:
		if q.table != "" {
			if q.where == "" && !q.unsafe {
				err = errors.New("deleting all data is not safe")
				return
			}
			str = "DELETE " + q.table
			if q.table != "" {
				str += " FROM " + q.table
			}
			if q.where != "" {
				str += " WHERE " + q.where
			}
		}
	case TypeUpdate:
		if q.table != "" {
			if q.where == "" && !q.unsafe {
				err = errors.New("updating all data is not safe")
				return
			}
			str = "UPDATE " + q.table
			str += " SET " + Substr(q.buildUpdateParams(q.values), 1)
			if q.where != "" {
				str += " WHERE " + q.where
			}
		}
	case TypeInsertUpdate:
		if q.table != "" {
			str = "INSERT INTO " + q.table
			var fields, placeholder string
			for k, v := range q.values {
				fields += "," + WrapSymbol + k + WrapSymbol
				placeholder += ",?"
				q.args = append(q.args, v)
			}
			str += " (" + Substr(fields, 1) + ") VALUES (" + Substr(placeholder, 1) + ") ON DUPLICATE KEY UPDATE "
			placeholder = q.buildUpdateParams(q.values2)
			str += Substr(placeholder, 1)
		}
	case TypeSelect:
		str = "SELECT " + q.field
		if q.table != "" {
			str += " FROM " + q.table
		}
		if q.where != "" {
			str += " WHERE " + q.where
		}
		if q.group != "" {
			str += " GROUP BY " + q.group
		}
		if q.order != "" {
			str += " ORDER BY " + q.order
		}
		if q.limit != "" && (q.db.Type == "" || q.db.Type == "mysql") {
			str += " LIMIT " + q.limit
		}
	}

	if len(returnFullSql) == 1 && returnFullSql[0] {
		str, err = FullSql(str, q.args...)
	}

	return
}

// 构造Update更新参数
func (q *SB) buildUpdateParams(vals SBValues) string {
	var placeholder string
	for k, v := range vals {
		if iv, ok := v.(IncVal); ok {
			placeholder += "," + WrapSymbol + k + WrapSymbol + "=" + Ternary(iv.BaseField == "", k, iv.BaseField).(string)
			if iv.Val >= 0 {
				placeholder += "+" + I64toA(iv.Val)
			} else {
				placeholder += I64toA(iv.Val)
			}
		} else {
			placeholder += "," + WrapSymbol + k + WrapSymbol + "=?"
			q.args = append(q.args, v)
		}
	}
	return placeholder
}

// 设置数据库对象
func (q *SB) DB(db *Database) *SB {
	q.db = db
	return q
}

// 设置FROM字句
func (q *SB) From(str string) *SB {
	q.table = str
	return q
}

// 设置表名
func (q *SB) Table(str string) *SB {
	return q.From(str)
}

// 设置WHERE字句
func (q *SB) Where(str string) *SB {
	q.where = str
	return q
}

// 设置GROUP字句
func (q *SB) Group(str string) *SB {
	q.group = str
	return q
}

// 设置GROUP字句
func (q *SB) Order(str string) *SB {
	q.order = str
	return q
}

// 设置LIMIT字句
func (q *SB) Limit(count int, offset ...int) *SB {
	if len(offset) > 0 {
		q.limit = Itoa(offset[0]) + "," + Itoa(count)
	} else {
		q.limit = "0," + Itoa(count)
	}
	return q
}

// 设置安全检查开关
func (q *SB) Unsafe(unsefe ...bool) *SB {
	if len(unsefe) == 1 && !unsefe[0] {
		q.unsafe = false
	} else {
		q.unsafe = true
	}
	return q
}

// 是否Debug
func (q *SB) Debug(debug ...bool) *SB {
	if len(debug) == 1 && !debug[0] {
		q.debug = false
	} else {
		q.debug = true
	}
	return q
}

// 设置值
func (q *SB) Value(m SBValues) *SB {
	q.values = m
	return q
}

// 设置值2
func (q *SB) Value2(m SBValues) *SB {
	q.values2 = m
	return q
}

// 添加值
func (q *SB) AddValue(key string, val interface{}) *SB {
	q.values.Add(key, val)
	return q
}

// 添加值2
func (q *SB) AddValue2(key string, val interface{}) *SB {
	q.values2.Add(key, val)
	return q
}

// 获取一个值对象
func NewValues() SBValues {
	return SBValues{}
}

// 构建INSERT语句
func Insert(ignore ...bool) *SB {
	var i bool
	if len(ignore) == 1 && ignore[0] {
		i = true
	}
	return &SB{t: TypeInsert, db: Obj, ignore: i, values: SBValues{}, args: make([]interface{}, 0)}
}

// 构建DELETE语句
func Delete() *SB {
	return &SB{t: TypeDelete, db: Obj}
}

// 构建UPDATE语句
func Update() *SB {
	return &SB{t: TypeUpdate, db: Obj, values: SBValues{}, args: make([]interface{}, 0)}
}

// 构建InsertUpdate语句, 仅针对MySQL有效, 内部使用ON DUPLICATE KEY UPDATE方式实现
func InsertUpdate() *SB {
	return &SB{t: TypeInsertUpdate, db: Obj, values: SBValues{}, values2: SBValues{}, args: make([]interface{}, 0)}
}

// 构建SELECT语句
func Select(str ...string) *SB {
	fields := "*"
	if len(str) == 1 {
		fields = str[0]
	}
	return &SB{t: TypeSelect, db: Obj, field: fields}
}

// 获取构造SQL后的参数
func (q *SB) GetArgs() []interface{} {
	return q.args
}

//
func (q *SB) FullSql(yes ...bool) *SB {
	if len(yes) == 1 {
		q.fullsql = yes[0]
	} else {
		q.fullsql = true
	}
	return q
}

// 执行INSERT、DELETE、UPDATE语句
func (q *SB) Exec(args ...interface{}) *SBResult {
	var err error
	sbRet := &SBResult{}
	sbRet.Sql, err = q.ToSql()
	if err != nil {
		sbRet.Msg = err.Error()
	} else {
		if q.debug {
			log.Println("\n\tSQL prepare statement:\n\t", sbRet.Sql, "\n\tMap args:\n\t", q.args, "\n\tParams:\n\t", args)
		}

		var ret sql.Result
		var err error
		if q.fullsql {
			var sqlStr string
			sqlStr, err = FullSql(sbRet.Sql, append(q.args, args...)...)
			if err == nil {
				ret, err = q.db.Exec(sqlStr)
			}
		} else {
			ret, err = q.db.Exec(sbRet.Sql, append(q.args, args...)...)
		}
		if err != nil {
			sbRet.Msg = err.Error()
		} else {
			sbRet.Success = true
			switch q.t {
			case TypeInsert:
				if DBType == "mysql" {
					last, err := ret.LastInsertId()
					if err == nil {
						sbRet.LastID = last
					}
				}
			case TypeDelete:
				fallthrough
			case TypeUpdate:
				fallthrough
			case TypeInsertUpdate:
				aff, err := ret.RowsAffected()
				if err == nil {
					sbRet.Affected = aff
				}
			}
		}
	}
	return sbRet
}

// 查询记录集
func (q *SB) Query(args ...interface{}) (Results, error) {
	s, e := q.ToSql()
	if e != nil {
		return nil, e
	}
	if q.debug {
		log.Println("\n\tSQL prepare statement:\n\t", s, "\n\tParams:\n\t", args)
	}
	return q.db.Select(s, args...)
}

// 查询单行数据
func (q *SB) QueryOne(args ...interface{}) (OneRow, error) {
	q.Limit(1, 0)
	s, e := q.ToSql()
	if e != nil {
		return nil, e
	}
	if q.debug {
		log.Println("\n\tSQL prepare statement:\n\t", s, "\n\tParams:\n\t", args)
	}
	return q.db.SelectOne(s, args...)
}

// 查询记录集
func (q *SB) QueryAllRow(args ...interface{}) (*sql.Rows, error) {
	s, e := q.ToSql()
	if e != nil {
		return nil, e
	}
	if q.debug {
		log.Println("\n\tSQL prepare statement:\n\t", s, "\n\tParams:\n\t", args)
	}
	return q.db.Query(s, args...)
}

// 查询单行数据
func (q *SB) QueryRow(args ...interface{}) *sql.Row {
	s, e := q.ToSql()
	if e != nil {
		return nil
	}
	if q.debug {
		log.Println("\n\tSQL prepare statement:\n\t", s, "\n\tParams:\n\t", args)
	}
	return q.db.QueryRow(s, args...)
}
