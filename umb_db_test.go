package umb_db

import (
	"fmt"
	"testing"
	// "time"
	"io/ioutil"
	"os"
	"strings"
)

const default_filename string = "./config.txt"

/* формат config.txt
DBNAME=pg_dbname;host=localhost;port=5432
DBUSER=pg_username
DBPASSWD=pg_user_password
*/

func TestOne(t *testing.T) {
	conf := readConf("")
	mdb, err := BeginDB(map[string]string{"HOST": conf["HOST"], "DBNAME": conf["DBNAME"], "DBUSER": conf["DBUSER"], "DBPASSWD": conf["DBPASSWD"], "PORT": conf["PORT"]})
	defer mdb.CloseDB()

	if err != nil {
		t.Errorf("Error on BeginDB: %s\n", err)
	}

	sel := "select name from test where id=$1"
	got, err := mdb.Row0(sel, []interface{}{0})
	if got != "one" || err != nil {
		t.Errorf("Error on Row0: %s\n\tgot=%s\n", err, got)
	}

	sel = "select id,name from test where id=$1"
	got2, err := mdb.Row(sel, []interface{}{1})
	fmt.Printf("got2 %#v\n", got2)
	if got2[0].(int64) != 1 || got2[1].(string) != "two" || err != nil {
		t.Errorf("Error on Row: %s\n\tgot=%#v\n", err, got2)
	}

	sel = "select id,name from test where id<=$1 order by id"
	got3, err := mdb.Rows(sel, []interface{}{1})
	fmt.Printf("got3 %#v\n", got3)
	if got3[1][0].(int64) != 1 || got3[1][1].(string) != "two" || err != nil {
		t.Errorf("Error on Rows: %s\n\tgot=%#v\n", err, got3)
	}

	sel = "select id,name from test where id=$1"
	got4, err := mdb.Hash(sel, []interface{}{1})
	fmt.Printf("got4 %#v\n", got4)
	if got4["id"].(int64) != 1 || got4["name"].(string) != "two" || err != nil {
		t.Errorf("Error on Row: %s\n\tgot=%#v\n", err, got4)
	}

	sel = "select id,name from test where id<=$1 order by id"
	got5, err := mdb.Hashes(sel, []interface{}{1})
	fmt.Printf("got5 %#v\n", got5)
	if got5[1]["id"].(int64) != 1 || got5[1]["name"].(string) != "two" || err != nil {
		t.Errorf("Error on Rows: %s\n\tgot=%#v\n", err, got5)
	}
}

func TestTwo(t *testing.T) {
	conf := readConf("")
	mdb, err := BeginDB(map[string]string{"HOST": conf["HOST"], "DBNAME": conf["DBNAME"], "DBUSER": conf["DBUSER"], "DBPASSWD": conf["DBPASSWD"], "PORT": conf["PORT"]})
	if err != nil {
		t.Errorf("Error on BeginDB: %s\n", err)
	}
	defer mdb.CloseDB()

	mdb.BeginTx()
	if err != nil {
		t.Errorf("Error on BeginTx: %s\n", err)
	}
	// defer mdb.Rollback()

	sel := "select name from test where id=$1"
	got, err := mdb.Row0(sel, []interface{}{0})
	if got != "one" || err != nil {
		t.Errorf("Error on Row0: %s\n\tgot=%s\n", err, got)
	}

	sel = "select id,name from test where id=$1"
	got2, err := mdb.Row(sel, []interface{}{1})
	fmt.Printf("got2 %#v\n", got2)
	if got2[0].(int64) != 1 || got2[1].(string) != "two" || err != nil {
		t.Errorf("Error on Row: %s\n\tgot=%#v\n", err, got2)
	}

	sel = "select id,name from test where id<=$1 order by id"
	got3, err := mdb.Rows(sel, []interface{}{1})
	fmt.Printf("got3 %#v\n", got3)
	if got3[1][0].(int64) != 1 || got3[1][1].(string) != "two" || err != nil {
		t.Errorf("Error on Rows: %s\n\tgot=%#v\n", err, got3)
	}

	sel = "select id,name from test where id=$1"
	got4, err := mdb.Hash(sel, []interface{}{1})
	fmt.Printf("got4 %#v\n", got4)
	if got4["id"].(int64) != 1 || got4["name"].(string) != "two" || err != nil {
		t.Errorf("Error on Row: %s\n\tgot=%#v\n", err, got4)
	}

	sel = "select id,name from test where id<=$1 order by id"
	got5, err := mdb.Hashes(sel, []interface{}{1})
	fmt.Printf("got5 %#v\n", got5)
	if got5[1]["id"].(int64) != 1 || got5[1]["name"].(string) != "two" || err != nil {
		t.Errorf("Error on Rows: %s\n\tgot=%#v\n", err, got5)
	}
}


func TestDo(t *testing.T) {
	var err error
	conf := readConf("")
	mdb, err := BeginDB(map[string]string{"HOST": conf["HOST"], "DBNAME": conf["DBNAME"], "DBUSER": conf["DBUSER"], "DBPASSWD": conf["DBPASSWD"], "PORT": conf["PORT"]})
	defer mdb.CloseDB()

	// new_id, err := mdb.Row0("select nextval('s_test')",[]interface{}{})
	// if err != nil {
	// 	t.Errorf("Error on TestDo::nextval")
	// }
	// fmt.Printf("new_id=%d\n",new_id);

	err = mdb.BeginTx()
	if err != nil {
		t.Errorf("Error on TestDo::BeginTx. %s\n",err)
	}
	// defer mdb.Rollback()

	_,err = mdb.Do("insert into test (id,name) values($1,$2)",[]interface{}{5,"six"},false)
	if err != nil {
		// mdb.Rollback();
		t.Errorf("Error on TestDo::Do. %s\n",err)
	}
	// mdb.Commit();

	fmt.Printf("IsTxOpen=%b!\n",mdb.IsTxOpen());
	sel := "select name from test where id=$1"
	got, err := mdb.Row0(sel, []interface{}{5})
	fmt.Printf("1::Row0=%s\n",got);
	if got != "six" || err != nil {
		t.Errorf("Error on TestDo::Row0: %s\n\tgot=%s\n", err, got)
	}

	sel = "select name from test where id>=$1 and id<=5 order by id"
	got2, err2 := mdb.Hashes(sel, []interface{}{4})
	fmt.Printf("2::Row0=%s\n",got2[1]["name"]);
	if got2[1]["name"] != "six" || err2 != nil {
		t.Errorf("Error on TestDo::Row0: %s\n\tgot2=%s\n", err2, got2)
	}

	mdb.Rollback()
}

// читаем конфиг. По умолчанию default_filename
func readConf(filename string) map[string]string {
	Config := make(map[string]string)

	if filename == "" {
		filename = default_filename
	}

	data, err := ioutil.ReadFile(filename)
	Check_err(err, 1)

	var rows = strings.Split(string(data), "\n")
	for i := 0; i < len(rows); i++ {
		if strings.HasPrefix(rows[i], "#") != true && strings.Index(rows[i], "=") != -1 {
			addConf(Config, rows[i], "")
		}
	}
	return Config
}

func addConf(C map[string]string, row string, suffix string) {
	parts := strings.SplitN(row, "=", 2)
	if parts[0] != "" && parts[1] != "" {
		if strings.HasPrefix(parts[0], "DBNAME") {
			// особый случай DBNAME/DBNAME2
			subparts := strings.Split(parts[1], ";")
			C[strings.ToUpper(parts[0])+suffix] = subparts[0]
			s := ""
			if parts[0] == "DBNAME2" {
				s = "2"
			}
			for i := 1; i < len(subparts); i++ {
				addConf(C, subparts[i], s)
			}
		} else {
			C[strings.ToUpper(parts[0])+suffix] = parts[1]
		}
	}
}

// Функция для обработки ошибок. Если t=1, то panic, иначе просто выводим сообщение об ошибке
func Check_err(e error, t int64) {
	if e != nil {
		if t == 1 {
			panic(e)
		} else {
			fmt.Fprintf(os.Stderr, "Error: %s!", e)
		}
	}
}
