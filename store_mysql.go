package appdash

import (
	"log"
	"time"
	"github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"fmt"
	"github.com/go-xorm/core"
	"database/sql"
	"encoding/json"
)

func MsTimestampNow() int64 {
	return time.Now().UnixNano() / 1000000
}

type MySQLConfig struct {
	DBName               string `toml:"dbname"`
	Host                 string `toml:"host"`
	Port                 int    `toml:"port"`
	User                 string `toml:"user"`
	Password             string `toml:"password"`
	Sslmode              string `toml:"sslmode"`
	ShowLog              bool
	ShowExecTime         bool
	LogLevel             core.LogLevel
	DataSaveDir          string
	DataFileSaveLoopSize int
	MaxIdleConns         int    `toml:"max_idle_conns"`
	MaxOpenConns         int    `toml:"max_open_conns"`
	MaxLifeTime          int    `toml:"max_life_time"`
	Timeout              int    `toml:"timeout"`
	RTimeout             int    `toml:"rtimeout"`
	WTimeout             int    `toml:"wtimeout"`
}

func (c MySQLConfig) MySQLSource() string {
	params := make(map[string]string, 0)
	params["charset"] = "utf8mb4"
	cfg := mysql.Config{}
	cfg.Net = "tcp"
	cfg.Addr = c.Host
	cfg.User = c.User
	cfg.Passwd = c.Password
	cfg.DBName = c.DBName
	cfg.ParseTime = true
	cfg.Collation = "utf8mb4_unicode_ci"
	cfg.Params = params
	cfg.Loc, _ = time.LoadLocation("Asia/Chongqing")
	cfg.Timeout = time.Duration(c.Timeout) * time.Second
	cfg.MultiStatements = true
	cfg.ReadTimeout = time.Duration(c.RTimeout) * time.Second
	cfg.WriteTimeout = time.Duration(c.WTimeout) * time.Second
	return cfg.FormatDSN()
}

type TraceTable struct {
	Id        int64  `xorm:"id BIGINT(20) notnull autoincr pk" json:"id"`
	TraceKey  string `xorm:"trace_key varchar(32) not null index" json:"trace_id"`
	TraceName string `xorm:"trace_name varchar(30) not null" json:"trace_name"`
	CostTime  int64  `xorm:"cost_time int(11) not null default 0" json:"cost_time"`
	CreatedAt int64  `xorm:"created_at BIGINT(20) notnull default 0" json:"created_at"`
	UpdatedAt int64  `xorm:"updated_at BIGINT(20) notnull default 0" json:"updated_at"`
}

func (t *TraceTable) TableName() string {
	return "appdash_traces"
}

func (t *TraceTable) BeforeInsert() {
	t.CreatedAt = MsTimestampNow()
	t.UpdatedAt = t.CreatedAt
}

func (t *TraceTable) BeforeUpdate() {
	t.UpdatedAt = MsTimestampNow()
}

type EventType int

const (
	EventType_Http EventType = 1
	EventType_Span EventType = 2
	EventType_Log  EventType = 3
	EventType_Msg  EventType = 4
	EventType_Time EventType = 5
)

type SpanTable struct {
	Id            int64     `xorm:"id BIGINT(20) notnull autoincr pk" json:"id"`
	SpanKey       string    `xorm:"span_key varchar(32) not null index" json:"span_id"`
	SpanName      string    `xorm:"span_name varchar(30) not null" json:"span_name"`
	Content       string    `xorm:"content text" json:"content"`
	CostTime      int64     `xorm:"cost_time int(11) not null default 0" json:"cost_time"`
	ParentSpanKey string    `xorm:"parent_span_key varchar(32) not null index" json:"parent_span_key"`
	ParentSpanID  int64     `xorm:"parent_span_id BIGINT(20) not null default 0 index" json:"parent_span_id"`
	TraceKey      string    `xorm:"trace_key varchar(32) not null index" json:"trace_key"`
	Recv          time.Time `xorm:"-" json:"client_recv"`
	Send          time.Time `xorm:"-" json:"client_send"`
	ClientRecv    int64     `xorm:"client_recv BIGINT(20) notnull default 0" json:"client_recv"`
	ClientSend    int64     `xorm:"client_send BIGINT(20) notnull default 0" json:"client_send"`
	TraceId       int64     `xorm:"trace_id BIGINT(20) not null default 0 index" json:"trace_id"`
	CreatedAt     int64     `xorm:"created_at BIGINT(20) notnull default 0" json:"created_at"`
	UpdatedAt     int64     `xorm:"updated_at BIGINT(20) notnull default 0" json:"updated_at"`
}

func (s *SpanTable) TableName() string {
	return "appdash_spans"
}

func (s *SpanTable) BeforeInsert() {
	s.CreatedAt = MsTimestampNow()
	s.UpdatedAt = s.CreatedAt
}

func (s *SpanTable) BeforeUpdate() {
	s.UpdatedAt = MsTimestampNow()
}

type MySQLStore struct {
	DBXorm *xorm.Engine
	Log    bool
}

var (
	DBXorm *xorm.Engine
	err    error
)

func NewMySQLStore(cfg MySQLConfig) (*MySQLStore) {
	log.Print("start NewMySQLStore")
	DBXorm, err = xorm.NewEngine("mysql", cfg.MySQLSource())
	if err != nil {
		panic(fmt.Errorf("sql.Open failed: %v", err))
	}
	DBXorm.ShowSQL(cfg.ShowLog)
	DBXorm.SetLogLevel(cfg.LogLevel)
	DBXorm.ShowExecTime(cfg.ShowExecTime)
	DBXorm.SetTableMapper(core.GonicMapper{})
	DBXorm.SetMaxIdleConns(cfg.MaxIdleConns)
	DBXorm.SetMaxOpenConns(cfg.MaxOpenConns)
	DBXorm.SetConnMaxLifetime(time.Duration(cfg.MaxLifeTime) * time.Second)
	err = DBXorm.Ping()
	if err != nil {
		panic(fmt.Sprintf("DBXorm.Ping Err:%v", err))
	}
	err = DBXorm.Sync2(new(TraceTable), new(SpanTable))
	return &MySQLStore{DBXorm, true}
}

func CloseMySQLStore() {
	err = DBXorm.Ping()
	if err == nil {
		err = DBXorm.Close()
	}
}

func (s *MySQLStore) Collect(id SpanID, as ...Annotation) error {
	content := make(map[string]string, 0)
	//log.Printf("Collect id Trace:%v", id.Trace)
	//log.Printf("Collect id Span:%v", id.Span)
	//log.Printf("Collect id Parent:%v", id.Parent)

	t := SpanTable{}
	t.TraceKey = id.Trace.String()
	t.SpanKey = id.Span.String()
	t.ParentSpanKey = id.Parent.String()
	if len(as) > 0 {
		for _, v := range as {
			if v.Key == "Name" {
				t.SpanName = string(v.Value)
			}
			if v.Key == "ClientSend" {
				t.Send, _ = time.Parse("2006-01-02T15:04:05.999999999-07:00", string(v.Value))
			}
			if v.Key == "ClientRecv" {
				t.Recv, _ = time.Parse("2006-01-02T15:04:05.999999999-07:00", string(v.Value))
			}
			if v.Key == "Server.Recv" {
				t.Send, _ = time.Parse("2006-01-02T15:04:05.999999999-07:00", string(v.Value))
			}
			if v.Key == "Server.Send" {
				t.Recv, _ = time.Parse("2006-01-02T15:04:05.999999999-07:00", string(v.Value))
			}
			content[v.Key] = string(v.Value)
			//log.Printf("Collect as key:%v", v.Key)
			//log.Printf("Collect as value:%v", string(v.Value))
		}
	}
	//log.Print("------------------------------")
	//byteArr, _ := json.Marshal(content)
	byteArr, _ := json.Marshal(as)
	t.ClientSend = t.Send.UnixNano()
	t.ClientRecv = t.Recv.UnixNano()
	t.Content = string(byteArr)
	//log.Printf("span----->:%v\n", t)
	insertSpan(t)
	return nil
}

func insertSpan(t SpanTable) (r SpanTable, err error) {
	var (
		parent SpanTable
		trace  TraceTable
	)
	_, err = DBXorm.Where("span_key=?", t.SpanKey).Get(&r)
	if r.Id > 0 {
		return
	}
	if t.ParentSpanKey == "0000000000000000" {
		_, err = DBXorm.Where("span_key=?", t.SpanKey).Get(&parent)
	} else {
		_, err = DBXorm.Where("span_key=?", t.ParentSpanKey).Get(&parent)
	}
	_, err = DBXorm.Where("trace_key=?", t.TraceKey).Get(&trace)
	if trace.Id == 0 {
		trace.TraceName = t.SpanName
		trace.TraceKey = t.TraceKey
		trace.CostTime = t.Recv.UnixNano() - t.Send.UnixNano()
		_, err = DBXorm.Insert(&trace)
	} else {
		if t.ParentSpanKey == "0000000000000000" {
			trace.TraceName = t.SpanName
		}
		trace.CostTime = trace.CostTime + (t.Recv.UnixNano() - t.Send.UnixNano())
		_, err = DBXorm.Where("trace_key=?", t.TraceKey).Update(&trace)
	}
	t.TraceId = trace.Id

	if parent.Id == 0 {
		t.CostTime = t.Recv.UnixNano() - t.Send.UnixNano()
		_, err = DBXorm.Insert(&t)
		r = t
	} else if parent.Id > 0 {
		t.ParentSpanID = parent.Id
		_, err = DBXorm.Insert(&t)
		r = t
	}
	sqlStr := `UPDATE %s AS t,(SELECT id FROM %s AS t2 WHERE t2.span_key=? AND t2.parent_span_key='0000000000000000') AS t2 SET t.parent_span_id=t2.id WHERE t.parent_span_key=?`
	_, err = DBXorm.Exec(fmt.Sprintf(sqlStr, t.TableName(), t.TableName()), t.SpanKey, t.SpanKey)
	return
}

func (s *MySQLStore) Traces(opts TracesOpts) ([]*Trace, error) {
	return recursiveHandleSpan("0000000000000000")
}

func recursiveHandleSpan(rootKey string) (parent []*Trace, err error) {
	var spans = make([]*SpanTable, 0, 0)
	err = DBXorm.Where("parent_span_key=?", rootKey).OrderBy("client_send DESC").Limit(20, 0).Find(&spans)
	if err != nil && err != sql.ErrNoRows {
		return
	} else if err == sql.ErrNoRows {
		err = nil
		return
	}
	if len(spans) > 0 {
		for _, v := range spans {
			trace := new(Trace)
			trace.Sub = make([]*Trace, 0, 0)
			spanId := SpanID{}
			spanId.Trace, _ = ParseID(v.TraceKey)
			spanId.Span, _ = ParseID(v.SpanKey)
			spanId.Parent, _ = ParseID(v.ParentSpanKey)
			span := Span{ID: spanId}
			err = json.Unmarshal([]byte(v.Content), &span.Annotations)
			trace.Span = span
			child, _ := recursiveHandleSpan(v.SpanKey)
			if len(child) > 0 {
				trace.Sub = child
			}
			parent = append(parent, trace)
		}
	}
	return
}

func (s *MySQLStore) Trace(id ID) (parent *Trace, err error) {
	//log.Printf("Trace id:%v\n", id)
	var span SpanTable
	var subs = make([]*Trace, 0, 0)
	parent = new(Trace)
	_, err = DBXorm.Where("trace_key=? AND parent_span_key=?", id.String(), "0000000000000000").Get(&span)
	if err != nil && err != sql.ErrNoRows {
		return
	} else if err == sql.ErrNoRows {
		err = nil
		return
	}
	spanId := SpanID{}
	spanId.Trace, _ = ParseID(span.TraceKey)
	spanId.Span, _ = ParseID(span.SpanKey)
	spanId.Parent, _ = ParseID(span.ParentSpanKey)
	parent.Span = Span{ID: spanId}
	err = json.Unmarshal([]byte(span.Content), &parent.Span.Annotations)
	subs, _ = recursiveHandleSpan(span.SpanKey)
	if len(subs) > 0 {
		parent.Sub = subs
	}
	return
}
