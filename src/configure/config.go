package configure

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func checkErr(err error) {
	if err != nil {
		logrus.WithError(err).Fatal("config")
	}
}

func New() *Config {
	config := viper.New()

	// Default config
	b, _ := json.Marshal(Config{
		ConfigFile: "config.yaml",
	})
	tmp := viper.New()
	defaultConfig := bytes.NewReader(b)
	tmp.SetConfigType("json")
	checkErr(tmp.ReadConfig(defaultConfig))
	checkErr(config.MergeConfigMap(viper.AllSettings()))

	pflag.String("config", "config.yaml", "Config file location")
	pflag.Bool("noheader", false, "Disable the startup header")
	pflag.Parse()
	checkErr(config.BindPFlags(pflag.CommandLine))

	// File
	config.SetConfigFile(config.GetString("config"))
	config.AddConfigPath(".")
	err := config.ReadInConfig()
	if err != nil {
		logrus.Warning(err)
		logrus.Info("Using default config")
	} else {
		checkErr(config.MergeInConfig())
	}

	BindEnvs(config, Config{})

	// Environment
	config.AutomaticEnv()
	config.SetEnvPrefix("EDGE")
	config.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	config.AllowEmptyEnv(true)

	// Print final config
	c := &Config{}
	checkErr(config.Unmarshal(&c))

	initLogging(c.Level)

	return c
}

func BindEnvs(config *viper.Viper, iface interface{}, parts ...string) {
	ifv := reflect.ValueOf(iface)
	ift := reflect.TypeOf(iface)
	for i := 0; i < ift.NumField(); i++ {
		v := ifv.Field(i)
		t := ift.Field(i)
		tv, ok := t.Tag.Lookup("mapstructure")
		if !ok {
			continue
		}
		switch v.Kind() {
		case reflect.Struct:
			BindEnvs(config, v.Interface(), append(parts, tv)...)
		default:
			_ = config.BindEnv(strings.Join(append(parts, tv), "."))
		}
	}
}

type Config struct {
	Level      string `mapstructure:"level" json:"level"`
	ConfigFile string `mapstructure:"config" json:"config"`
	NoHeader   bool   `mapstructure:"noheader" json:"noheader"`

	Edge struct {
		Bind string `mapstructure:"bind" json:"bind"`
	} `mapstructure:"edge" json:"edge"`

	Pod struct {
		Name string `mapstructure:"name" json:"name"`
		IP   string `mapstructure:"ip" json:"ip"`
	} `mapstructure:"pod" json:"pod"`

	Mongo struct {
		URI      string `mapstructure:"uri" json:"uri"`
		Database string `mapstructure:"database" json:"database"`
		Direct   bool   `mapstructure:"direct" json:"direct"`
	} `mapstructure:"mongo" json:"mongo"`

	Auth struct {
		JwtToken string `mapstructure:"jwt_token" json:"jwt_token"`
	} `mapstructure:"auth" json:"auth"`

	Redis struct {
		Username   string   `mapstructure:"username" json:"username"`
		Password   string   `mapstructure:"password" json:"password"`
		MasterName string   `mapstructure:"master_name" json:"master_name"`
		Addresses  []string `mapstructure:"addresses" json:"addresses"`
		Database   int      `mapstructure:"database" json:"database"`
		Sentinel   bool     `mapstructure:"sentinel" json:"sentinel"`
	} `mapstructure:"redis" json:"redis"`

	Monitoring struct {
		Enabled bool       `mapstructure:"enabled" json:"enabled"`
		Bind    string     `mapstructure:"bind" json:"bind"`
		Labels  []KeyValue `mapstructure:"labels" json:"labels"`
	} `mapstructure:"monitoring" json:"monitoring"`

	Health struct {
		Enabled bool   `mapstructure:"enabled" json:"enabled"`
		Bind    string `mapstructure:"bind" json:"bind"`
	} `mapstructure:"health" json:"health"`
}

type KeyValue struct {
	Key   string `mapstructure:"key" json:"key"`
	Value string `mapstructure:"value" json:"value"`
}
