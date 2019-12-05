package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/judwhite/go-svc/svc"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqd"
)

// 实现了 Server 接口，并通过 Run 进行执行
type program struct {
	once sync.Once
	nsqd *nsqd.NSQD
}

func main() {
	log.Println("----------begin---------------------")
	// nsqd 开始进行运行
	prg := &program{}
	// 使用 svc.Run 传入 program，并通过依次调用 Service 的四个接口来进行服务的启动
	// Init、Start、NotifySignal、Stop。
	// NotifySignal 关联的就是后面传入的两个信号
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%s", err)
	}
}

func (p *program) Init(env svc.Environment) error {
	log.Println("begin---------------------")
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

// nsqd 启动入口
func (p *program) Start() error {
	opts := nsqd.NewOptions()

	// 进行命令行的解析
	flagSet := nsqdFlagSet(opts)
	flagSet.Parse(os.Args[1:])

	rand.Seed(time.Now().UTC().UnixNano())
	// 打印版本号并退出
	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqd"))
		os.Exit(0)
	}

	// 加载 config 文件
	var cfg config
	configFile := flagSet.Lookup("config").Value.String()
	if configFile != "" {
		// 解压 config 文件到 cfg 结构体中
		_, err := toml.DecodeFile(configFile, &cfg)
		if err != nil {
			logFatal("failed to load config file %s - %s", configFile, err)
		}
	}
	// 验证配置文件中的数据
	cfg.Validate()
	// 合并命令行和配置文件中的参数
	options.Resolve(opts, flagSet, cfg)
	// 根据配置创建一个 nsq 对象
	nsqd, err := nsqd.New(opts)
	if err != nil {
		logFatal("failed to instantiate nsqd - %s", err)
	}
	// 创建成功，放到 p 中
	p.nsqd = nsqd
	// 加载元数据，存储了 topic 和 channel 数据
	err = p.nsqd.LoadMetadata()
	if err != nil {
		logFatal("failed to load metadata - %s", err)
	}
	// 加载完立即进行数据的写入？？？
	err = p.nsqd.PersistMetadata()
	if err != nil {
		logFatal("failed to persist metadata - %s", err)
	}
	// 启动一个协程来运行 nsq 的 main 函数
	go func() {
		err := p.nsqd.Main()
		if err != nil {
			// 运行失败退出
			p.Stop()
			os.Exit(1)
		}
	}()

	return nil
}

func (p *program) Stop() error {
	// 仅执行一次 Exit 操作
	p.once.Do(func() {
		p.nsqd.Exit()
	})
	return nil
}

// 打印 log 参数并结束程序
func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqd] ", f, args...)
}
