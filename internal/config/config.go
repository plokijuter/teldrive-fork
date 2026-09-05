package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/tgdrive/teldrive/internal/duration"
)

type ServerCmdConfig struct {
	Server       ServerConfig       `config:"server"`
	Log          LoggingConfig      `config:"log"`
	JWT          JWTConfig          `config:"jwt"`
	DB           DBConfig           `config:"db"`
	TG           TGConfig           `config:"tg"`
	CronJobs     CronJobConfig      `config:"cronjobs"`
	Cache        CacheConfig        `config:"cache"`
	ScanDetector ScanDetectorConfig `config:"scan-detector"`
}

type ScanDetectorConfig struct {
	Enabled             bool `config:"enabled" description:"Enable Jellyfin scan detection and throttling" default:"true"`
	ThrottleDelayMs     int  `config:"throttle-delay-ms" description:"Base throttle delay in milliseconds" default:"200"`
	ScanThresholdPerSec int  `config:"scan-threshold-per-sec" description:"Request threshold per second to detect scan" default:"10"`
}

type ServerConfig struct {
	Port             int           `config:"port" description:"HTTP port for the server to listen on" default:"8080"`
	GracefulShutdown time.Duration `config:"graceful-shutdown" description:"Grace period for server shutdown" default:"10s"`
	EnablePprof      bool          `config:"enable-pprof" description:"Enable pprof debugging endpoints"`
	ReadTimeout      time.Duration `config:"read-timeout" description:"Maximum duration for reading entire request" default:"1h"`
	WriteTimeout     time.Duration `config:"write-timeout" description:"Maximum duration for writing response" default:"1h"`
}

type CacheConfig struct {
	MaxSize   int    `config:"max-size" description:"Maximum cache size in bytes" default:"10485760"`
	RedisAddr string `config:"redis-addr" description:"Redis server address"`
	RedisPass string `config:"redis-pass" description:"Redis server password"`
}

type LoggingConfig struct {
	Level string `config:"level" description:"Logging level (debug, info, warn, error)" default:"info"`
	File  string `config:"file" description:"Log file path, if empty logs to stdout"`
}

type JWTConfig struct {
	Secret       string        `config:"secret" description:"JWT signing secret key" required:"true"`
	SessionTime  time.Duration `config:"session-time" description:"JWT token validity duration" default:"30d"`
	AllowedUsers []string      `config:"allowed-users" description:"List of allowed usernames"`
}

type DBPool struct {
	Enable             bool          `config:"enable" description:"Enable connection pooling" default:"true"`
	MaxOpenConnections int           `config:"max-open-connections" description:"Maximum number of open connections" default:"25"`
	MaxIdleConnections int           `config:"max-idle-connections" description:"Maximum number of idle connections" default:"25"`
	MaxLifetime        time.Duration `config:"max-lifetime" description:"Maximum connection lifetime" default:"10m"`
}
type DBConfig struct {
	DataSource  string `config:"data-source" description:"Database connection string" required:"true"`
	PrepareStmt bool   `config:"prepare-stmt" description:"Use prepared statements" default:"true"`
	LogLevel    string `config:"log-level" description:"Database logging level" default:"error"`
	Pool        DBPool `config:"pool"`
}

type CronJobConfig struct {
	Enable               bool          `config:"enable" description:"Enable scheduled background jobs" default:"true"`
	LockerInstance       string        `config:"locker-instance" description:"Distributed unique cron locker name" default:"cron-locker"`
	CleanFilesInterval   time.Duration `config:"clean-files-interval" description:"Interval for cleaning expired files" default:"1h"`
	CleanUploadsInterval time.Duration `config:"clean-uploads-interval" description:"Interval for cleaning incomplete uploads" default:"12h"`
	FolderSizeInterval   time.Duration `config:"folder-size-interval" description:"Interval for updating folder sizes" default:"2h"`
}

type TGStream struct {
	MultiThreads int           `config:"multi-threads" description:"Number of download threads"`
	Buffers      int           `config:"buffers" description:"Number of stream buffers" default:"8"`
	ChunkTimeout time.Duration `config:"chunk-timeout" description:"Chunk download timeout" default:"20s"`
	// LocationCache active le cache des locations Telegram. Il etait
	// inoperant jusqu'au 2026-08-22 (pointeur passe par valeur a msgpack),
	// ce qui coutait 2 RPC supplementaires par morceau de 1 Mio. Ce
	// drapeau permet de MESURER le gain en le desactivant, sans
	// reconstruire ni redeployer. Le laisser a true en production.
	// PoolSize ouvre N connexions MTProto pour la LECTURE, comme le fait
	// deja l'envoi (upload.go : pool.NewPool(client, TG.PoolSize)).
	// Telegram limite le debit par connexion : la lecture n'en ouvrant
	// qu'une seule, c'est l'explication structurelle du facteur ~12
	// mesure le 2026-08-22 entre lecture (3,3 Mio/s) et envoi (40 Mio/s).
	// 0 = comportement historique (une seule connexion partagee).
	PoolSize int `config:"pool-size" description:"Connexions MTProto pour la lecture (0 = une seule, historique)" default:"0"`

	// ClientCache garde des clients MTProto VIVANTS entre les requetes HTTP.
	// Sans lui, teldrive construit un client neuf et appelle client.Run() a
	// CHAQUE requete -- donc une poignee de main MTProto complete a chaque
	// fois. Mesure du 2026-08-24 : ~1,3 s par aller-retour a froid, et
	// ffmpeg en enchaine 3-4 pour demarrer une lecture, d'ou 4-6 s par saut.
	// A false : comportement d'origine, strictement inchange.
	ClientCache bool `config:"client-cache" description:"Garder les clients MTProto vivants entre les requetes (0 = comportement historique)" default:"false"`

	LocationCache bool `config:"location-cache" description:"Cache Telegram file locations (false = ancien comportement, pour mesure A/B)" default:"true"`
}

type TGUpload struct {
	EncryptionKey string        `config:"encryption-key" description:"Encryption key for uploads" required:"true"`
	Threads       int           `config:"threads" description:"Number of upload threads" default:"8"`
	MaxRetries    int           `config:"max-retries" description:"Maximum upload retry attempts" default:"10"`
	Retention     time.Duration `config:"retention" description:"Upload retention period" default:"7d"`
	ChunkDelay    time.Duration `config:"chunk-delay" description:"Delay between chunk uploads to avoid flood" default:"0s"`
}
type TGConfig struct {
	RateLimit bool `config:"rate-limit" description:"Enable rate limiting for API calls" default:"true"`
	RateBurst int  `config:"rate-burst" description:"Maximum burst size for rate limiting" default:"5"`
	// ATTENTION : ce nombre n'est PAS une frequence, c'est un INTERVALLE
	// EN MILLISECONDES. tgc.go fait rate.Every(time.Millisecond * Rate),
	// donc 100 => une requete toutes les 100 ms => 10 RPC/s, et non
	// 100 par minute comme la description le laissait croire.
	// Plus la valeur est PETITE, plus on autorise de requetes.
	// Mesure du 2026-08-22 : ce plafond de 10 RPC/s, combine au cout de
	// 3 RPC par Mio, donnait exactement les 3,3 Mio/s constates en lecture.
	Rate              int           `config:"rate" description:"Intervalle minimal entre requetes, en MILLISECONDES (100 = 10 req/s ; plus petit = plus rapide)" default:"100"`
	Ntp               bool          `config:"ntp" description:"Use NTP for time synchronization"`
	DisableStreamBots bool          `config:"disable-stream-bots" description:"Disable streaming bots"`
	MtprotoLogFile    string        `config:"mtproto-log-file" description:"File path to log MTProto requests/responses (empty=disabled)"`
	Proxy             string        `config:"proxy" description:"HTTP/SOCKS5 proxy URL"`
	ProxyEnabled      bool          `config:"proxy-enabled" description:"Enable proxy usage" default:"false"`
	ProxyPool         []string      `config:"proxy-pool" description:"Pool of proxy URLs for per-bot proxy rotation"`
	ReconnectTimeout  time.Duration `config:"reconnect-timeout" description:"Client reconnection timeout" default:"5m"`
	PoolSize          int           `config:"pool-size" description:"Session pool size" default:"8"`
	EnableLogging     bool          `config:"enable-logging" description:"Enable Telegram client logging"`
	AppId             int           `config:"app-id" description:"Telegram app ID" default:"2496"`
	AppHash           string        `config:"app-hash" description:"Telegram app hash" default:"8da85b0d5bfe62527e5b244c209159c3"`
	DeviceModel       string        `config:"device-model" description:"Device model" default:"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/116.0"`
	SystemVersion     string        `config:"system-version" description:"System version" default:"Win32"`
	AppVersion        string        `config:"app-version" description:"App version" default:"6.1.4 K"`
	LangCode          string        `config:"lang-code" description:"Language code" default:"en"`
	SystemLangCode    string        `config:"system-lang-code" description:"System language code" default:"en-US"`
	LangPack          string        `config:"lang-pack" description:"Language pack" default:"webk"`
	SessionInstance   string        `config:"session-instance" description:"Bot Sessions Instance Name" default:"teldrive"`
	AutoChannelCreate bool          `config:"auto-channel-create" description:"Auto Create Channel" default:"true"`
	ChannelLimit      int64         `config:"channel-limit" description:"Channel message limit before auto channel creation" default:"500000"`
	Uploads           TGUpload      `config:"uploads"`
	Stream            TGStream      `config:"stream"`
}

type ConfigLoader struct {
	v              *viper.Viper
	requiredFields []string
}

func NewConfigLoader() *ConfigLoader {
	return &ConfigLoader{
		v: viper.New(),
	}
}

func StringToDurationHook() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data any) (any, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		if t != reflect.TypeOf(time.Duration(0)) {
			return data, nil
		}

		str, ok := data.(string)
		if !ok {
			return data, nil
		}
		return duration.ParseDuration(str)
	}
}

func (cl *ConfigLoader) Load(cmd *cobra.Command, cfg any) error {

	cl.v.SetConfigType("toml")

	cfgFile := cmd.Flags().Lookup("config").Value.String()
	if cfgFile != "" {
		cl.v.SetConfigFile(cfgFile)
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("error getting home directory: %v", err)
		}
		cl.v.AddConfigPath(filepath.Join(home, ".teldrive"))
		cl.v.AddConfigPath(".")
		cl.v.SetConfigName("config")
	}

	if err := cl.v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return fmt.Errorf("error reading config file: %v", err)
		}
	}

	return cl.load(cfg)
}

func (cl *ConfigLoader) Validate() error {
	missingFields := []string{}
	for _, key := range cl.requiredFields {
		if !cl.v.IsSet(key) {
			missingFields = append(missingFields, strings.ReplaceAll(key, ".", "-"))
		}
	}
	if len(missingFields) > 0 {
		return fmt.Errorf("required configuration values not set: %s", strings.Join(missingFields, ", "))
	}
	return nil
}

func (cl *ConfigLoader) RegisterPlags(flags *pflag.FlagSet, prefix string, v any, skipFlags bool) error {
	flags.StringP("config", "c", "", "Config file path (default $HOME/.teldrive/config.toml)")
	return cl.walkStruct(v, prefix, func(key string, field reflect.StructField, value reflect.Value) error {
		return cl.setDefault(flags, key, field, skipFlags)
	})
}

func (cl *ConfigLoader) setDefault(flags *pflag.FlagSet, key string, field reflect.StructField, skipFlags bool) error {
	description := field.Tag.Get("description")
	defaultVal := field.Tag.Get("default")

	if defaultVal != "" {
		description += fmt.Sprintf(" (default %s)", defaultVal)
	}
	if required := field.Tag.Get("required"); required == "true" {
		cl.requiredFields = append(cl.requiredFields, key)
	}

	flagKey := strings.ReplaceAll(key, ".", "-")

	if defaultVal != "" {
		cl.v.SetDefault(key, defaultVal)
	}

	if skipFlags {
		return nil
	}

	switch field.Type.Kind() {
	case reflect.String:
		flags.String(flagKey, "", description)
	case reflect.Int:
		flags.Int(flagKey, 0, description)
	case reflect.Int64:
		flags.Int64(flagKey, 0, description)
	case reflect.Bool:
		flags.Bool(flagKey, false, description)
	case reflect.Slice:
		switch field.Type.Elem().Kind() {
		case reflect.String:
			flags.StringSlice(flagKey, nil, description)
		case reflect.Int:
			flags.IntSlice(flagKey, nil, description)

		}
	default:
		if field.Type == reflect.TypeOf(time.Duration(0)) {
			flags.Duration(flagKey, time.Duration(0), description)

		}
	}
	if err := cl.v.BindPFlag(key, flags.Lookup(flagKey)); err != nil {
		return fmt.Errorf("error binding flag %s: %w", key, err)
	}

	return nil
}

func (cl *ConfigLoader) walkStruct(v any, prefix string, fn func(key string, field reflect.StructField, value reflect.Value) error) error {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	typ := val.Type()

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		value := val.Field(i)
		configTag := field.Tag.Get("config")
		if configTag == "" {
			continue
		}
		key := configTag
		if prefix != "" {
			key = prefix + "." + configTag
		}
		if field.Type.Kind() == reflect.Struct {
			var nestedValue any
			if value.CanAddr() {
				nestedValue = value.Addr().Interface()
			} else {
				nestedValue = value.Interface()
			}
			if err := cl.walkStruct(nestedValue, key, fn); err != nil {
				return err
			}
			continue
		}

		if err := fn(key, field, value); err != nil {
			return err
		}
	}

	return nil
}

func decodeTag(tag string) viper.DecoderConfigOption {
	return func(c *mapstructure.DecoderConfig) {
		c.TagName = tag
	}
}

func (cl *ConfigLoader) load(cfg any) error {
	return cl.v.Unmarshal(&cfg, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		StringToDurationHook(),
	)), decodeTag("config"))

}
