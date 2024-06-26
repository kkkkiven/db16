package db

import (
	"database/sql"
	_ "github.com/denisenkom/go-mssqldb"
)

// ProcExec 执行存储过程, 返回受影响的行数
func (this *Database) ExecProc(procname string, params ...interface{}) (int64, error) {
	result, err := this.Exec("EXEC " + procname + " " + this.GetProcPlaceholder(len(params)), params...)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	lastinsertid, err := result.LastInsertId()
	if err != nil {
		return affected, nil
	}
	return lastinsertid, nil
}

// GetExecProcErr 执行存储过程, 返回是否在执行过程中出现错误
func (this *Database) GetExecProcErr(procname string, params ...interface{}) error {
	_, err := this.ExecProc(procname, params...)
	if err != nil {
		return err
	}
	return nil
}

// ProcQuery 通过存储过程查询记录
func (this *Database) ProcQuery(procname string, params ...interface{}) (rows *sql.Rows, err error) {
	rows, err = this.Query("EXEC " + procname + " " + this.GetProcPlaceholder(len(params)), params...)
	return
}

// ProcQueryRow 通过存储过程查询单条记录
func (this *Database) ProcQueryRow(procname string, params ...interface{}) *sql.Row {
	return this.QueryRow("EXEC " + procname + " " + this.GetProcPlaceholder(len(params)), params...)
}

// ProcStatus 调用存储过程并获取最终的执行状态码和提示信息
func (this *Database) ProcStatus(procname string, params ...interface{}) (int, string) {
	var status int
	var msg string
	err := this.QueryRow("EXEC " + procname + " " + this.GetProcPlaceholder(len(params)), params...).Scan(&status, &msg)
	if err != nil {
		return -99, err.Error()
	}
	return status, msg
}

// ProcSelect 通过存储过程查询结果集
func (this *Database) ProcSelect(procname string, params ...interface{}) (Results, error) {
	return this.Select("EXEC " + procname + " " + this.GetProcPlaceholder(len(params)), params...)
}

// ProcSelectOne 通过存储查询一行不定字段的结果
func (this *Database) ProcSelectOne(procname string, params ...interface{}) (OneRow, error) {
	return this.SelectOne("EXEC " + procname + " " + this.GetProcPlaceholder(len(params)), params...)
}

// GetProcPlaceholder 按照指定数量生成调用存储过程时所用的参数占位符
func (this *Database) GetProcPlaceholder(count int) (placeholder string) {
	placeholder = ""
	for i := 0; i < count; i++ {
		if i > 0 {
			placeholder += ","
		}
		placeholder += "?"
	}
	return
}