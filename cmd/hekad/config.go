/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2012-2014
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Victor Ng (vng@mozilla.com)
#   Rob Miller (rmiller@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

// Hekad configuration.

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"heka/pipeline"
)

type HekadConfig struct {
	Maxprocs              int    `toml:"maxprocs"`                //启用多核使用；默认值为 1 个核心。更多的内核通常会增加消息吞吐量
	PoolSize              int    `toml:"poolsize"`                // 指定可以存在的最大消息池大小。默认值为 100。
	ChanSize              int    `toml:"plugin_chansize"`         //为各种 Heka 插件指定输入通道的缓冲区大小。默认为 30。
	CpuProfName           string `toml:"cpuprof"`                 //打开hekad的CPU分析；输出记录到output_file。
	MemProfName           string `toml:"memprof"`                 //启用内存分析；输出记录到output_file。
	MaxMsgLoops           uint   `toml:"max_message_loops"`       //消息可以重新注入系统的最大次数。这用于防止从过滤器到过滤器的无限消息循环；默认值为 4
	MaxMsgProcessInject   uint   `toml:"max_process_inject"`      //沙盒过滤器的 ProcessMessage 函数在一次调用中可以注入的最大消息数；默认值为 1。
	MaxMsgProcessDuration uint64 `toml:"max_process_duration"`    //沙盒过滤器的 ProcessMessage 函数在被终止之前可以在单个调用中消耗的最大纳秒数；默认值为 100000。
	MaxMsgTimerInject     uint   `toml:"max_timer_inject"`        //沙盒过滤器的 TimerEvent 函数在一次调用中可以注入的最大消息数；默认值为 10。
	MaxPackIdle           string `toml:"max_pack_idle"`           //泄露之前的最大等待时间比如 2s 2m等
	BaseDir               string `toml:"base_dir"`                //持久化和运行需要的目录 需要可写权限
	ShareDir              string `toml:"share_dir"`               // 配置目录 只需要可读权限
	SampleDenominator     int    `toml:"sample_denominator"`      //采样率 默认1000，即1000条采样1条，统计时间
	PidFile               string `toml:"pid_file"`                // 防止重复运行，启动前会检测，退出时会删除
	Hostname              string `toml:"Hostname"`                // 主机名，默认os.Hostname()
	MaxMessageSize        uint32 `toml:"max_message_size"`        // 发送的消息最大大小，默认 64k
	LogFlags              int    `toml:"log_flags"`               // log格式
	FullBufferMaxRetries  uint32 `toml:"full_buffer_max_retries"` // 缓冲区过大时，为减轻背压清空缓冲区，hekad等待缓存区小于90%的最大间隔数
}

// 配置文件和环境变量处理
func LoadHekadConfig(configPath string) (config *HekadConfig, err error) {
	hostname, err := os.Hostname()
	if err != nil {
		return
	}

	// hekad 节点默认配置
	config = &HekadConfig{Maxprocs: 1,
		PoolSize:              100,
		ChanSize:              30,
		CpuProfName:           "",
		MemProfName:           "",
		MaxMsgLoops:           4,
		MaxMsgProcessInject:   1,
		MaxMsgProcessDuration: 100000,
		MaxMsgTimerInject:     10,
		MaxPackIdle:           "2m",
		BaseDir:               filepath.FromSlash("."), // /var/cache/hekad
		ShareDir:              filepath.FromSlash("."), // /usr/share/heka
		SampleDenominator:     1000,
		PidFile:               "",
		Hostname:              hostname,
		LogFlags:              log.LstdFlags,
		FullBufferMaxRetries:  10,
	}

	var configFile map[string]toml.Primitive
	p, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("Error opening config file: %s", err)
	}
	fi, err := p.Stat()
	if err != nil {
		return nil, fmt.Errorf("Error fetching config file info: %s", err)
	}

	if fi.IsDir() {
		files, _ := ioutil.ReadDir(configPath)
		for _, f := range files {
			fName := f.Name() // 遍历所有toml文件依次加载
			if !strings.HasSuffix(fName, ".toml") {
				// Skip non *.toml files in a config dir.
				continue
			}
			fPath := filepath.Join(configPath, fName)
			contents, err := pipeline.ReplaceEnvsFile(fPath)
			if err != nil {
				return nil, err
			}
			if _, err = toml.Decode(contents, &configFile); err != nil {
				return nil, fmt.Errorf("Error decoding config file: %s", err)
			}
		}
	} else {
		// 把配置文件中通过%ENV[]设置的替换为环境变量里的真实值
		contents, err := pipeline.ReplaceEnvsFile(configPath)
		if err != nil {
			return nil, err
		}
		if _, err = toml.Decode(contents, &configFile); err != nil {
			return nil, fmt.Errorf("Error decoding config file: %s", err)
		}
	}

	//empty_ignore := map[string]interface{}{}
	parsed_config, ok := configFile[pipeline.HEKA_DAEMON]
	if ok {
		//if err = toml.PrimitiveDecodeStrict(parsed_config, config, empty_ignore); err != nil {
		if err = toml.PrimitiveDecode(parsed_config, config); err != nil {
			err = fmt.Errorf("Can't unmarshal config: %s", err)
		}
	}

	return
}
