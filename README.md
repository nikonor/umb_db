# umb_db

PACKAGE DOCUMENTATION

package umb_db
    import "github.com/nikonor/umb_db"



FUNCTIONS

>func Connect2db(C map[string]string) (*sql.DB, error)
    соединяемся с базой (для многопоточности стоит иметь много соединений, а
    не одно)

>func German2ISO(v string) string
    перевод даты из German в ISO-8601

>func ISO2German(v string) string
    перевод даты из ISO-8601 в German


TYPES

type MyDB struct {
    DB *sql.DB
    TX *sql.Tx
}


func BeginDB(C map[string]string) (MyDB, error)
    создаем объект DB по конфугу


func (m *MyDB) BeginTx() error

func (m *MyDB) CloseDB() error
    закрываем соединения с базой

func (m *MyDB) Commit() error

func (m *MyDB) Do(q string, pars []interface{}, needId bool) (int64, error)
    q - "шаблон запроса", pars - массив значения для запроса, needId - true
    - если не нужно получать id, false, если нужно

func (m MyDB) Hash(q string, pars []interface{}) (map[string]interface{}, error)
    получаем словарь значений

func (m MyDB) Hashes(q string, pars []interface{}) (ret []map[string]interface{}, err error)
    получаем набор словарей значений

func (m MyDB) IsTxOpen() bool

func (m *MyDB) Rollback() error

func (m MyDB) Row(q string, pars []interface{}) (ret []interface{}, err error)
    получение набора значений

func (m MyDB) Row0(q string, pars []interface{}) (ret interface{}, err error)
    получение одного значения

func (m MyDB) Rows(q string, pars []interface{}) (ret [][]interface{}, err error)
    получаем набор строк
