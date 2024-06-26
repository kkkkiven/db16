## Usage

```go
package main

import (
	"database/sql"
	"encoding/hex"
	"fmt"

	"git.jiaxianghudong.com/go/db"
	_ "github.com/go-sql-driver/mysql"
)

// dbConfig 数据库配置结构
type dbConfig struct {
	Host          string `yaml:"host"`           //数据库地址
	Port          int    `yaml:"port"`           //数据库端口
	Dbname        string `yaml:"dbname"`         //数据库库名
	User          string `yaml:"user"`           //数据库用户名
	Pass          string `yaml:"pass" spew:"-"`  //数据库密码
	Charset       string `yaml:"charset"`        //数据库字符集
	MaxIdle       int    `yaml:"max_idle"`       //最大闲置连接数
	MaxConnection int    `yaml:"max_conncetion"` //数据库最大连接数
}

// connMySQL 连接MySQL数据库
func connMySQL(conf *dbConfig) error {
	var err error
	db.Obj = &db.Database{}
	db.Obj.DB, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s", conf.User, conf.Pass, conf.Host, conf.Port, conf.Dbname, conf.Charset))
	if err != nil {
		return err
	}

	if conf.MaxIdle > 0 {
		db.Obj.DB.SetMaxIdleConns(conf.MaxIdle)
		db.Obj.DB.SetConnMaxLifetime(-1)
	}
	if conf.MaxConnection > 0 {
		db.Obj.DB.SetMaxOpenConns(conf.MaxConnection)
	}

	err = db.Obj.DB.Ping()
	if err != nil {
		return err
	}
	return nil
}

func init() {
	err := connMySQL(&dbConfig{
		Host:          "127.0.0.1",
		Port:          3306,
		Dbname:        "test",
		User:          "root",
		Pass:          "weile2018",
		Charset:       "utf8",
		MaxIdle:       10,
		MaxConnection: 10,
	})
	if err != nil {
		panic(err)
	}
}

func main() {
	_, err := db.Obj.Exec(`
CREATE TABLE IF NOT EXISTS users (
  id int unsigned NOT NULL AUTO_INCREMENT,
  name varchar(36) NOT NULL,
  old int unsigned NOT NULL,
  pwd binary(16) NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
`)
	if err != nil {
		panic(err)
	}

	// 插入记录
	passHex, err := hex.DecodeString("68F18A7E6B7E9645F2E32CE1346BF9C5")
	if err != nil {
		panic(passHex)
	}
	ret := db.Insert().Table("users").Value(db.SBValues{"name": "小明", "old": 11, "pwd": passHex}).Exec()
	if !ret.Success {
		panic(err)
	}
	fmt.Printf("插入新记录 => ID为: %d", ret.LastID)

	_ = db.Insert().Table("users").Value(db.SBValues{"name": "小黑", "old": 12, "pwd": passHex}).Exec()
	_ = db.Insert().Table("users").Value(db.SBValues{"name": "小白", "old": 12, "pwd": passHex}).Exec()
	_ = db.Insert().Table("users").Value(db.SBValues{"name": "小二", "old": 14, "pwd": passHex}).Exec()
	_ = db.Insert().Table("users").Value(db.SBValues{"name": "小三", "old": 15, "pwd": passHex}).Exec()

	// 查表全部记录
	rows, err := db.Select().From("users").Query()
	for _, row := range rows {
		fmt.Println("多条记录 =>", row)
	}

	// 查表部分记录
	rows, err = db.Select().From("users").Limit(2, 1).Order("id DESC").Query()
	for _, row := range rows {
		fmt.Println("指定数量多条记录 =>", row)
	}

	// 查单行记录
	row, err := db.Select().From("users").Where("id=?").QueryOne(3)
	if err != nil {
		panic(err)
	}
	fmt.Printf("查单条记录 => ID: %d, Name: %s, Old: %d\n", row.GetInt("id"), row.Get("name"), row.GetInt("old"))

	// 获取单行二进制字段数据
	var pass []byte
	err = db.Select("pwd").From("users").Where("id=?").QueryRow(3).Scan(&pass)
	if err != nil {
		panic(err)
	}
	fmt.Printf("查单条二进制 => Pass: %X\n", pass)

	// 带Group字句
	rows, err = db.Select("old, SUM(1) AS total").From("users").Group("old").Query()
	if err != nil {
		panic(err)
	}
	for _, row := range rows {
		fmt.Printf("GROUP => old: %d, count: %d\n", row.GetInt("old"), row.GetInt("total"))
	}

	// 更新
	ret = db.Update().Table("users").AddValue("name", "小花").AddValue("old", 10).Where("id=?").Exec(3)
	if !ret.Success {
		panic(ret.Msg)
	}
	fmt.Printf("更新 => %d 条记录", ret.Affected)

	ret = db.Update().Table("users").Value(db.SBValues{"name": "小花222", "old": 11}).Where("id=?").Exec(3)
	if !ret.Success {
		panic(ret.Msg)
	}
	fmt.Printf("更新 => %d 条记录", ret.Affected)

	// 基于唯一索引, 存在则更新, 不存在则插入
	ret = db.InsertUpdate().Table("users").Value(db.SBValues{"id": 4, "name": "二狗", "old": 21}).Value2(db.SBValues{"old": 21}).Exec()
	if !ret.Success {
		panic(ret.Msg)
	}

	// 删除
	ret = db.Delete().Table("users").Where("id=?").Exec(5)
	if !ret.Success {
		panic(ret.Msg)
	}
	fmt.Printf("删除 => %d 条记录", ret.Affected)

	// 也可通过 db.Obj 直接传入完整对 SQL 语句查询
	// db.Obj.Exec(sql[, args...])
	// db.Obj.Insert(sql[, args...])
	// db.Obj.Update(sql[, args...])
	// db.Obj.Delete(sql[, args...])
	// db.Obj.Select()
	// db.Obj.SelectOne()
	// db.Obj.Query()
	// db.Obj.QueryRow()

	// 或通过 db.Obj.DB 访问 database/sql 调用标准库自带方法
}
```