package umb_db

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	// "github.com/xwb1989/sqlparser"
	"errors"
	"os"
	"regexp"
	"strings"
	"time"
)

type MyDB struct {
	DB *sql.DB
	TX *sql.Tx
}

// создаем объект DB по конфугу
func BeginDB(C map[string]string) (MyDB, error) {
	db, err := Connect2db(C)
	return MyDB{db, nil}, err
}

// соединяемся с базой (для многопоточности стоит иметь много соединений, а не одно)
func Connect2db(C map[string]string) (*sql.DB, error) {
	db_connect_string := "host=" + C["HOST"] + " dbname=" + C["DBNAME"] + " user=" + C["DBUSER"] + " password=" + C["DBPASSWD"] + " port=" + C["PORT"]
	fmt.Println(db_connect_string)
	return sql.Open("postgres", db_connect_string)
}

//закрываем соединения с базой
func (m MyDB) CloseDB() error {
	if err := m.DB.Close(); err != nil {
		return err
	}
	return nil
}

func (m MyDB) BeginTX() (MyDB, error) {
	var err error

	m.TX, err = m.DB.Begin()
	if err != nil {
		return m, err
	}
	return m, nil
}

func (m MyDB) Commit() (MyDB, error) {
	if err := m.TX.Commit(); err != nil {
		return m, err
	}
	m.TX = nil
	return m, nil
}

func (m MyDB) Rollback() (MyDB, error) {
	if err := m.TX.Rollback(); err != nil {
		return m, err
	}
	m.TX = nil
	return m, nil
}

//получение одного значения
func (m MyDB) Row0(q string, pars []interface{}) (ret interface{}, err error) {
	pars = preparePars(pars)
	err = m.DB.QueryRow(q, pars...).Scan(&ret)
	return ret, err
}

//получение набора значений
func (m MyDB) Row(q string, pars []interface{}) (ret []interface{}, err error) {

	pars = preparePars(pars)

	rows, err := m.DB.Query(q, pars...)
	if err != nil {
		return nil, err
	}

	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	rows.Next()

	for i, _ := range columns {
		valuePtrs[i] = &values[i]
	}

	rows.Scan(valuePtrs...)

	for i, _ := range columns {
		var v interface{}
		val := values[i]
		b, ok := val.([]byte)
		if ok {
			v = string(b)
		} else {
			v = val
		}
		ret = append(ret, v)
	}
	return ret, nil
}

//получаем набор строк
func (m MyDB) Rows(q string, pars []interface{}) (ret [][]interface{}, err error) {

	pars = preparePars(pars)

	rows, err := m.DB.Query(q, pars...)
	if err != nil {
		return nil, err
	}

	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {
		var subret []interface{}

		for i, _ := range columns {
			valuePtrs[i] = &values[i]
		}

		rows.Scan(valuePtrs...)

		for i, _ := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)

			if ok {
				v = string(b)
			} else {
				v = val
			}
			// fmt.Println(col, v)
			subret = append(subret, v)
		}
		ret = append(ret, subret)
	}
	return ret, nil
}

//получаем словарь значений
func (m MyDB) Hash(q string, pars []interface{}) (map[string]interface{}, error) {
	ret := map[string]interface{}{}
	pars = preparePars(pars)

	rows, err := m.DB.Query(q, pars...)
	if err != nil {
		return nil, err
	}

	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	columnsName := make([]string, count)

	rows.Next()

	for i, cn := range columns {
		valuePtrs[i] = &values[i]
		columnsName[i] = cn
	}

	rows.Scan(valuePtrs...)

	for i, _ := range columns {
		var v interface{}
		val := values[i]
		b, ok := val.([]byte)
		if ok {
			v = string(b)
		} else {
			v = val
		}
		// ret = append (ret, v)
		ret[string(columnsName[i])] = v
	}
	return ret, nil
}

//получаем набор словарей значений
func (m MyDB) Hashes(q string, pars []interface{}) (ret []map[string]interface{}, err error) {
	pars = preparePars(pars)

	rows, err := m.DB.Query(q, pars...)
	if err != nil {
		return nil, err
	}

	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	columnsName := make([]string, count)

	for rows.Next() {
		subret := map[string]interface{}{}

		for i, cn := range columns {
			columnsName[i] = cn
			valuePtrs[i] = &values[i]
		}

		rows.Scan(valuePtrs...)

		for i, _ := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)

			if ok {
				v = string(b)
			} else {
				v = val
			}
			subret[string(columnsName[i])] = v
		}
		ret = append(ret, subret)
	}
	return ret, nil

}

// q - "шаблон запроса",
// pars - массив значения для запроса,
// needId - true - если не нужно получать id, false, если нужно
func (m MyDB) Do2(q string, pars []interface{}, needId bool) (int64, error) {
	var new_id int64

	if m.IsTxOpen() != true {
		return -1, errors.New("Tx is not open")
	}

	pars = preparePars(pars)

	if strings.HasPrefix(strings.ToUpper(q), "INSERT") {
		stmt1, err := m.TX.Prepare(q + " returning id")
		if err != nil {
			return -1, err
		}

		// выполняем первый запрос
		res := stmt1.QueryRow(pars...)

		//  получаем ID, если это нужно

		if needId == false {
			var lastPar = len(pars)

			// добавляем в запрос поле id и placeholder для него. Номер placeholder-а будет lastPar+1
			q = strings.Replace(q, fmt.Sprintf("$%d", lastPar), fmt.Sprintf("$%d,$%d", lastPar, lastPar+1), -1)
			q = strings.Replace(q, ") ", ",id) ", 1)

			if err := res.Scan(&new_id); err != nil {
				return -1, err
			}
			pars = append(pars, new_id)
		} else {
			new_id = 0
		}

		stmt2, err := m.TX.Prepare(q)
		if err != nil {
			return -1, err
		}
		if _, err := stmt2.Exec(pars...); err != nil {
			return -1, err
		}

	} else {
		new_id = 0
		// тут мы оказываемся в случае НЕ INSERT
		if strings.HasPrefix(strings.ToUpper(q), "DELETE") {
			// для DELETE надо переделать запрос
			tbl := ""
			var qq []string
			ww := strings.Fields(q)
			for i, w := range ww {
				if strings.ToUpper(w) == "FROM" {
					if strings.ToUpper(ww[i+1]) == "ONLY" || ww[i+1] == "*" {
						tbl = ww[i+2]
						qq = ww[i+3 : len(ww)]
					} else {
						tbl = ww[i+1]
						qq = ww[i+2 : len(ww)]
					}
					break
				}
			}
			q = fmt.Sprintf("update %s set del=1 %s", tbl, strings.Join(qq, " "))
		} // DELETE

		stmt1, err := m.TX.Prepare(q)
		if err != nil {
			return -1, err
		}
		stmt2, err := m.TX.Prepare(q)
		if err != nil {
			return -1, err
		}

		if pars != nil {
			if _, err := stmt1.Exec(pars...); err != nil {
				return -1, err
			}
			if _, err := stmt2.Exec(pars...); err != nil {
				return -1, err
			}
		} else {
			if _, err := stmt1.Exec(); err != nil {
				return -1, err
			}
			if _, err := stmt2.Exec(); err != nil {
				return -1, err
			}
		}

	}

	return new_id, nil
}

func preparePars(pars []interface{}) []interface{} {
	for i, v := range pars {
		switch v.(type) {
		case string:
			m, err := regexp.MatchString("^\\d{1,2}\\.\\d{1,2}\\.\\d\\d\\d\\d$", v.(string))
			if m && err == nil {
				pars[i] = German2ISO(v.(string))
			}
		case time.Time:
			pars[i] = pars[i].(time.Time).Format("2006-01-02")
		}
	}

	return pars
}

func (m MyDB) IsTxOpen() bool {
	if m.TX != nil {
		return true
	}
	return false
}

// перевод даты из German в ISO-8601
func German2ISO(v string) string {
	vv := strings.Split(strings.Replace(v, "'", "", -1), ".")
	v = fmt.Sprintf("%s-%s-%s", vv[2], vv[1], vv[0])
	return v
}

// перевод даты из  ISO-8601 в German
func ISO2German(v string) string {
	vv := strings.Split(strings.Replace(v, "'", "", -1), "-")
	v = fmt.Sprintf("%s.%s.%s", vv[2], vv[1], vv[0])
	return v
}

func check_err(e error, t int64, mark int64) {
	if e != nil {
		if t == 1 {
			panic(e)
		} else {
			fmt.Fprintf(os.Stderr, "Error: make=%d: %s\n", mark, e)
		}
	}
}
