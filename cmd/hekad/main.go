/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2012-2015
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Rob Miller (rmiller@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

/*

Main entry point for the `hekad` daemon. Loads the specified config and calls
`pipeline.Run` to launch the PluginRunners and all additional goroutines.

*/
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "heka/examples"
	"heka/message"
	"heka/pipeline"
	_ "heka/plugins"
	_ "heka/plugins/amqp"
	_ "heka/plugins/dasher"
	_ "heka/plugins/elasticsearch"
	_ "heka/plugins/file"
	_ "heka/plugins/graphite"
	_ "heka/plugins/http"
	_ "heka/plugins/irc"
	_ "heka/plugins/kafka"
	_ "heka/plugins/logstreamer"
	_ "heka/plugins/nagios"
	_ "heka/plugins/payload"
	_ "heka/plugins/process"
	_ "heka/plugins/smtp"
	_ "heka/plugins/statsd"
	_ "heka/plugins/tcp"
	_ "heka/plugins/udp"
)

const (
	VERSION = "0.11.0"
)

func setGlobalConfigs(config *HekadConfig) (*pipeline.GlobalConfigStruct, string, string) {
	maxprocs := config.Maxprocs
	poolSize := config.PoolSize
	chanSize := config.ChanSize
	cpuProfName := config.CpuProfName
	memProfName := config.MemProfName
	maxMsgLoops := config.MaxMsgLoops
	maxMsgProcessInject := config.MaxMsgProcessInject
	maxMsgProcessDuration := config.MaxMsgProcessDuration
	maxMsgTimerInject := config.MaxMsgTimerInject
	maxPackIdle, _ := time.ParseDuration(config.MaxPackIdle)

	runtime.GOMAXPROCS(maxprocs)

	globals := pipeline.DefaultGlobals()
	globals.PoolSize = poolSize
	globals.PluginChanSize = chanSize
	globals.MaxMsgLoops = maxMsgLoops
	if globals.MaxMsgLoops == 0 {
		globals.MaxMsgLoops = 1
	}
	globals.MaxMsgProcessInject = maxMsgProcessInject
	globals.MaxMsgProcessDuration = maxMsgProcessDuration
	globals.MaxMsgTimerInject = maxMsgTimerInject
	globals.MaxPackIdle = maxPackIdle
	globals.BaseDir = config.BaseDir
	globals.ShareDir = config.ShareDir
	globals.SampleDenominator = config.SampleDenominator
	globals.Hostname = config.Hostname
	globals.FullBufferMaxRetries = uint(config.FullBufferMaxRetries)

	return globals, cpuProfName, memProfName
}

func main() {
	exitCode := 0
	// `os.Exit` will skip any registered deferred functions, so to support
	// exit codes we put it in the first registerred deferred (i.e. the last to
	// run), we can set the exitCode and then call `return` to exit with an
	// error code.
	defer func() {
		os.Exit(exitCode)
	}()

	configPath := flag.String("config", filepath.FromSlash("/etc/hekad.toml"),
		"Config file or directory. If directory is specified then all files "+
			"in the directory will be loaded.")
	version := flag.Bool("version", false, "Output version and exit")
	flag.Parse()

	config := &HekadConfig{}
	var err error
	var cpuProfName string
	var memProfName string

	if *version {
		fmt.Println(VERSION)
		return
	}
	// 加载hekad 配置 默认从etc读取，读不到退出
	config, err = LoadHekadConfig(*configPath)
	if err != nil {
		pipeline.LogError.Println("Error reading config: ", err)
		exitCode = 1
		return
	}
	pipeline.LogInfo.SetFlags(config.LogFlags)
	pipeline.LogError.SetFlags(config.LogFlags)
	if config.SampleDenominator <= 0 {
		pipeline.LogError.Println("'sample_denominator' value must be greater than 0.")
		exitCode = 1
		return
	}

	if _, err = time.ParseDuration(config.MaxPackIdle); err != nil {
		pipeline.LogError.Printf("Can't parse `max_pack_idle` time duration: %s\n",
			config.MaxPackIdle)
		exitCode = 1
		return
	}

	globals, cpuProfName, memProfName := setGlobalConfigs(config)

	if err = os.MkdirAll(globals.BaseDir, 0755); err != nil {
		pipeline.LogError.Printf("Error creating 'base_dir' %s: %s", config.BaseDir, err)
		exitCode = 1
		return
	}

	if config.MaxMessageSize > 1024 {
		message.SetMaxMessageSize(config.MaxMessageSize)
	} else if config.MaxMessageSize > 0 {
		pipeline.LogError.Println("Error: 'max_message_size' setting must be greater than 1024.")
		exitCode = 1
		return
	}
	// todo 改为多个小函数
	if config.PidFile != "" {
		contents, err := ioutil.ReadFile(config.PidFile)
		if err == nil {
			pid, err := strconv.Atoi(strings.TrimSpace(string(contents)))
			if err != nil {
				pipeline.LogError.Printf("Error reading proccess id from pidfile '%s': %s",
					config.PidFile, err)
				exitCode = 1
				return
			}
			//防止启动多个进程
			process, err := os.FindProcess(pid)

			// on Windows, err != nil if the process cannot be found
			if runtime.GOOS == "windows" {
				if err == nil {
					pipeline.LogError.Printf("Process %d is already running.", pid)
					exitCode = 1
					return
				}
			} else if process != nil {
				// err is always nil on POSIX, so we have to send the process
				// a signal to check whether it exists
				if err = process.Signal(syscall.Signal(0)); err == nil {
					pipeline.LogError.Printf("Process %d is already running.", pid)
					exitCode = 1
					return
				}
			}
		}
		if err = ioutil.WriteFile(config.PidFile, []byte(strconv.Itoa(os.Getpid())),
			0644); err != nil {

			pipeline.LogError.Printf("Unable to write pidfile '%s': %s", config.PidFile, err)
			exitCode = 1
		}
		pipeline.LogInfo.Printf("Wrote pid to pidfile '%s'", config.PidFile)
		defer func() {
			if err = os.Remove(config.PidFile); err != nil {
				pipeline.LogError.Printf("Unable to remove pidfile '%s': %s", config.PidFile, err)
			}
		}()
	}

	if cpuProfName != "" {
		profFile, err := os.Create(cpuProfName)
		if err != nil {
			pipeline.LogError.Println(err)
			exitCode = 1
			return
		}
		// 开启cpu监控
		pprof.StartCPUProfile(profFile)
		defer func() {
			pprof.StopCPUProfile()
			profFile.Close()
		}()
	}

	if memProfName != "" {
		defer func() {
			profFile, err := os.Create(memProfName)
			if err != nil {
				pipeline.LogError.Fatalln(err)
			}
			pprof.WriteHeapProfile(profFile)
			profFile.Close()
		}()
	}

	// 读取其它节点配置开始管道运行，并初始化插件，失败则退出
	// Set up and load the pipeline configuration and start the daemon.
	pipeconf := pipeline.NewPipelineConfig(globals)
	if err = loadFullConfig(pipeconf, configPath); err != nil {
		pipeline.LogError.Println("Error reading config: ", err)
		exitCode = 1
		return
	}
	exitCode = pipeline.Run(pipeconf)
}

func loadFullConfig(pipeconf *pipeline.PipelineConfig, configPath *string) (err error) {
	p, err := os.Open(*configPath)
	if err != nil {
		return fmt.Errorf("error opening file: %s", err.Error())
	}
	fi, err := p.Stat()
	if err != nil {
		return fmt.Errorf("can't stat file: %s", err.Error())
	}
	//判断传入的配置，是路径还是文件，路径则加载所有toml文件
	if fi.IsDir() {
		files, _ := ioutil.ReadDir(*configPath)
		for _, f := range files {
			fName := f.Name()
			if !strings.HasSuffix(fName, ".toml") {
				// Skip non *.toml files in a config dir.
				continue
			}
			err = pipeconf.PreloadFromConfigFile(filepath.Join(*configPath, fName))
			if err != nil {
				break
			}
		}
	} else {
		err = pipeconf.PreloadFromConfigFile(*configPath)
	}
	if err == nil {
		err = pipeconf.LoadConfig()
	}
	return err
}
