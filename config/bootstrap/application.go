package bootstrap

import (
	"fmt"
	"github.com/spf13/viper"
	"strings"
	"log"
)

var (
	DEV_ENV = "dev"
	PROD_ENV = "prod"
	REPLACER *strings.Replacer = strings.NewReplacer(".", "_")
	APP_CONFIG_PREFIX = `APP`
	APP_CONFIG_NAME = `application`
	ENV_CONFIG_PREFIX = `DB`
	ENV_CONFIG_NAME = `env`
	CONFIG_PATH = `./`
	CONFIG_FILE_TYPE = `yaml`
)


var App *Application

type Config viper.Viper

type Application struct {
	ENV       string `json:"env"`
	EnvConfig Config `json:"env_config"`
	AppConfig Config `json:"application_config"`
}

func init() {
	log.Println("load env")
	App = &Application{}
	App.loadEnvConfig()
	App.loadENV()
	App.loadAppConfig()
}

// loadEnvConfig: read application config and build viper object
func (app *Application) loadEnvConfig() {
	var (
		envConfig *viper.Viper
		err       error
	)
	envConfig = viper.New()
	envConfig.SetEnvKeyReplacer(REPLACER)
	envConfig.AutomaticEnv()
	envConfig.SetConfigName(ENV_CONFIG_NAME)
	envConfig.AddConfigPath(CONFIG_PATH)
	envConfig.SetConfigType(CONFIG_FILE_TYPE)
	if err = envConfig.ReadInConfig(); err != nil {
		panic(err)
	}
	app.EnvConfig = Config(*envConfig)
}

// loadAppConfig: read application config and build viper object
func (app *Application) loadAppConfig() {
	var (
		appConfig *viper.Viper
		err       error
	)
	appConfig = viper.New()
	appConfig.SetEnvKeyReplacer(REPLACER)
	appConfig.AutomaticEnv()
	appConfig.SetConfigName(APP_CONFIG_NAME)
	appConfig.AddConfigPath(CONFIG_PATH)
	appConfig.SetConfigType(CONFIG_FILE_TYPE)
	if err = appConfig.ReadInConfig(); err != nil {
		panic(err)
	}
	app.AppConfig = Config(*appConfig)
}

func (app *Application) loadENV() {
	var APPENV string
	var envConfig viper.Viper
	envConfig = viper.Viper(app.EnvConfig)
	APPENV = envConfig.GetString("env")
	switch APPENV {
	case DEV_ENV:
		app.ENV = DEV_ENV
		break
	case PROD_ENV:
		app.ENV = PROD_ENV
		break
	default:
		app.ENV = DEV_ENV
		break
	}
}

// String: read string value from viper.Viper
func (config *Config) String(key string) string {
	var viperConfig viper.Viper
	viperConfig = viper.Viper(*config)
	return viperConfig.GetString(fmt.Sprintf("%s.%s", App.ENV, key))
}

// Int: read int value from viper.Viper
func (config *Config) Int(key string) int {
	var viperConfig viper.Viper
	viperConfig = viper.Viper(*config)
	return viperConfig.GetInt(fmt.Sprintf("%s.%s", App.ENV, key))
}

// Boolean: read boolean value from viper.Viper
func (config *Config) Boolean(key string) bool {
	var viperConfig viper.Viper
	viperConfig = viper.Viper(*config)
	return viperConfig.GetBool(fmt.Sprintf("%s.%s", App.ENV, key))
}

func (config *Config) Map(key string) map[string]string {
	var viperConfig viper.Viper
	viperConfig = viper.Viper(*config)
	return viperConfig.GetStringMapString(fmt.Sprintf("%s.%s", App.ENV, key))
}