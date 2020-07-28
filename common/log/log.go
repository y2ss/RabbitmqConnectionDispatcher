package log

import (
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"RabbitmqConnectionDispatcher/config/bootstrap"
	"strconv"
	"time"
)

var Logger *zap.SugaredLogger
func init() {
	logConfig := bootstrap.App.AppConfig.Map("log")
	//maxSize, err := strconv.Atoi(logConfig["max_size"]);
	//if err != nil { panic(err) }
	maxAge, err := strconv.Atoi(logConfig["max_age"])
	if err != nil { panic(err) }

	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
		MessageKey: "msg",
		LevelKey: "level",
		EncodeLevel: zapcore.CapitalLevelEncoder,
		TimeKey: "ts",
		EncodeTime: func(time time.Time, encoder zapcore.PrimitiveArrayEncoder) {
			encoder.AppendString(time.Format("2006-01-02 15:04:05"))
		},
		CallerKey: "file",
		EncodeCaller: zapcore.ShortCallerEncoder,
		EncodeDuration: func(duration time.Duration, encoder zapcore.PrimitiveArrayEncoder) {
			encoder.AppendInt64(int64(duration) / 1000000)
		},
	})

	//按日志时间rotate
	infoHook, err := rotatelogs.New(
		"info_%Y%m%d_" + logConfig["filename"],
		rotatelogs.WithMaxAge(time.Duration(maxAge) * 24 * time.Hour),
		rotatelogs.WithRotationTime(24 * time.Hour),
		)
	if err != nil {
		panic(err)
	}

	errorHook, err := rotatelogs.New(
		"error_%Y%m%d_"+ logConfig["filename"],
		rotatelogs.WithMaxAge(time.Duration(maxAge) * 24 * time.Hour),
		rotatelogs.WithRotationTime(24 * time.Hour),
	)
	if err != nil {
		panic(err)
	}

	debugHook, err := rotatelogs.New(
		"debug_%Y%m%d_"+ logConfig["filename"],
		rotatelogs.WithMaxAge(time.Duration(maxAge) * 24 * time.Hour),
		rotatelogs.WithRotationTime(24 * time.Hour),
	)
	if err != nil {
		panic(err)
	}
	//按日志大小rotate
	//infoHook := zapcore.AddSync(&lumberjack.Logger{
	//	Filename: fmt.Sprintf("info_%s", logConfig["filename"]),
	//	MaxSize: maxSize,
	//	LocalTime: true,
	//	Compress: true,
	//	MaxAge: maxAge,//days
	//})
	//
	//errorHook := zapcore.AddSync(&lumberjack.Logger{
	//	Filename: fmt.Sprintf("error_%s", logConfig["filename"]),
	//	MaxSize: maxSize,//1g
	//	LocalTime: true,
	//	Compress: true,
	//	MaxAge: maxAge,//days
	//})

	debugLevel := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl == zapcore.DebugLevel
	})

	infoLevel := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl < zapcore.WarnLevel && lvl != zapcore.DebugLevel
	})

	errorLevel := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.WarnLevel
	})

	core := zapcore.NewTee(
		zapcore.NewCore(encoder, zapcore.AddSync(debugHook), debugLevel),
		zapcore.NewCore(encoder, zapcore.AddSync(infoHook), infoLevel),
		zapcore.NewCore(encoder, zapcore.AddSync(errorHook), errorLevel))

	log := zap.New(core, zap.AddCaller())
	Logger = log.Sugar()
}
