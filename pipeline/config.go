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
#   Mike Trinkala (trink@mozilla.com)
#   Justin Judd (justin@justinjudd.org)
#
# ***** END LICENSE BLOCK *****/

package pipeline

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"regexp"
	"sync"
	"time"
	"unsafe"

	"github.com/BurntSushi/toml"
	"github.com/pborman/uuid"
)

const (
	HEKA_DAEMON     = "hekad"
	invalidEnvChars = "\n\r\t "
)

var (
	invalidEnvPrefix     = []byte("%ENV[")
	AvailablePlugins     = make(map[string]func() interface{})
	ErrMissingCloseDelim = errors.New("Missing closing delimiter")
	ErrInvalidChars      = errors.New("Invalid characters in environmental variable")
	LogInfo              = log.New(os.Stdout, "", log.LstdFlags)
	LogError             = log.New(os.Stderr, "", log.LstdFlags)
)

func GetUnexportedField(field reflect.Value) interface{} {
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
}

func SetUnexportedField(field reflect.Value, value interface{}) {
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).
		Elem().
		Set(reflect.ValueOf(value))
}

// Adds a plugin to the set of usable Heka plugins that can be referenced from
// a Heka config file.
func RegisterPlugin(name string, factory func() interface{}) {
	AvailablePlugins[name] = factory
}

// Generic plugin configuration type that will be used for plugins that don't
// provide the `HasConfigStruct` interface.
type PluginConfig map[string]toml.Primitive

func (c *PluginConfig) GetData() map[string]interface{} {
	data := make(map[string]interface{})
	for s, primitive := range *c {

		rs := reflect.ValueOf(primitive)
		rs2 := reflect.New(rs.Type()).Elem()
		rs2.Set(rs)
		rf := rs2.Field(0)

		data[s] = GetUnexportedField(rf)
	}
	return data
}

// API made available to all plugins providing Heka-wide utility functions.
type PluginHelper interface {

	// Returns an `OutputRunner` for an output plugin registered using the
	// specified name, or ok == false if no output by that name is registered.
	Output(name string) (oRunner OutputRunner, ok bool)

	// Returns the running `FilterRunner` for a filter plugin registered using
	// the specified name, or ok == false if no filter by that name is
	// registered.
	Filter(name string) (fRunner FilterRunner, ok bool)

	// Instantiates and returns an `Encoder` plugin of the specified name, or
	// ok == false if no encoder by that name is registered.
	Encoder(base_name, full_name string) (encoder Encoder, ok bool)

	// Returns the currently running Heka instance's unique PipelineConfig
	// object.
	PipelineConfig() *PipelineConfig

	// Instantiates, starts, and returns a DecoderRunner wrapped around a newly
	// created Decoder of the specified name.
	DecoderRunner(base_name, full_name string) (dRunner DecoderRunner, ok bool)

	// Stops and unregisters the provided DecoderRunner.
	StopDecoderRunner(dRunner DecoderRunner) (ok bool)

	// Expects a loop count value from an existing message (or zero if there's
	// no relevant existing message), returns an initialized `PipelinePack`
	// pointer that can be populated w/ message data and inserted into the
	// Heka pipeline. Returns `nil` if the loop count value provided is
	// greater than the maximum allowed by the Heka instance.
	PipelinePack(msgLoopCount uint) (*PipelinePack, error)

	// Returns an input plugin of the given name that provides the
	// StatAccumulator interface, or an error value if such a plugin
	// can't be found.
	StatAccumulator(name string) (statAccum StatAccumulator, err error)

	// Returns the configured Hostname for the Heka process. This can come
	// either from the runtime or from the Heka config.
	Hostname() string
}

// Indicates a plug-in has a specific-to-itself config struct that should be
// passed in to its Init method.
type HasConfigStruct interface {
	// Returns a default-value-populated configuration structure into which
	// the plugin's TOML configuration will be deserialized.
	ConfigStruct() interface{}
}

// Master config object encapsulating the entire heka/pipeline configuration.
type PipelineConfig struct {
	// Heka global values.
	Globals *GlobalConfigStruct
	// PluginMakers for every registered plugin, by category.
	makers map[string]map[string]PluginMaker
	// Direct access to makers["Decoder"] since it's needed by MultiDecoder
	// outside of the pipeline package.
	DecoderMakers map[string]PluginMaker
	// Mutex protecting the makers map.
	makersLock sync.RWMutex
	// All running InputRunners, by name.
	InputRunners map[string]InputRunner
	// All running FilterRunners, by name.
	FilterRunners map[string]FilterRunner
	// All running OutputRunners, by name.
	OutputRunners map[string]OutputRunner
	// Heka message router instance.
	router *messageRouter
	// PipelinePack supply for Input plugins.
	inputRecycleChan chan *PipelinePack
	// PipelinePack supply for Filter plugins (separate pool prevents
	// deadlocks).
	injectRecycleChan chan *PipelinePack
	// Stores log messages generated by plugin config errors.
	LogMsgs []string
	// Lock protecting access to the set of running filters so dynamic filters
	// can be safely added and removed while Heka is running.
	filtersLock sync.RWMutex
	// Is freed when all FilterRunners have stopped.
	filtersWg sync.WaitGroup
	// Is freed when all DecoderRunners have stopped.
	decodersWg sync.WaitGroup
	// Slice providing access to all running DecoderRunners.
	allDecoders []DecoderRunner
	// Mutex protecting allDecoders.
	allDecodersLock sync.RWMutex

	// Slice providing access to all Decoders called synchronously by InputRunner
	allSyncDecoders []ReportingDecoder
	// Mutex protecting allSyncDecoders
	allSyncDecodersLock sync.RWMutex

	// Slice providing access to all Splitters
	allSplitters []SplitterRunner
	// Mutex protecting AllSplitters
	allSplittersLock sync.RWMutex

	// Slice providing access to all instantiated Encoders.
	allEncoders map[string]Encoder
	// Mutex protecting allEncoders.
	allEncodersLock sync.RWMutex
	// Name of host on which Heka is running.
	hostname string
	// Heka process id.
	pid int32
	// Lock protecting access to the set of running inputs so they
	// can be safely added while Heka is running.
	inputsLock sync.RWMutex
	// Is freed when all Input runners have stopped.
	inputsWg sync.WaitGroup
	// Lock protecting access to running outputs so they can be removed
	// safely.
	outputsLock sync.RWMutex
	// Internal reporting channel.
	reportRecycleChan chan *PipelinePack

	// The next few values are used only during the initial configuration
	// loading process.

	// Track default plugin registration.
	defaultConfigs map[string]bool
	// Loaded PluginMakers sorted by category.
	makersByCategory map[string][]PluginMaker
	// Number of config loading errors.
	errcnt uint
}

// Creates and initializes a PipelineConfig object. `nil` value for `globals`
// argument means we should use the default global config values.
func NewPipelineConfig(globals *GlobalConfigStruct) (config *PipelineConfig) {
	config = new(PipelineConfig)
	if globals == nil {
		globals = DefaultGlobals()
	}
	config.Globals = globals
	config.makers = make(map[string]map[string]PluginMaker)
	config.makers["Input"] = make(map[string]PluginMaker)
	config.makers["Decoder"] = make(map[string]PluginMaker)
	config.makers["Filter"] = make(map[string]PluginMaker)
	config.makers["Encoder"] = make(map[string]PluginMaker)
	config.makers["Output"] = make(map[string]PluginMaker)
	config.makers["Splitter"] = make(map[string]PluginMaker)
	config.DecoderMakers = config.makers["Decoder"]

	config.InputRunners = make(map[string]InputRunner)
	config.FilterRunners = make(map[string]FilterRunner)
	config.OutputRunners = make(map[string]OutputRunner)

	config.allEncoders = make(map[string]Encoder)
	config.router = NewMessageRouter(globals.PluginChanSize, globals.abortChan)
	config.inputRecycleChan = make(chan *PipelinePack, globals.PoolSize)
	config.injectRecycleChan = make(chan *PipelinePack, globals.PoolSize)
	config.LogMsgs = make([]string, 0, 4)
	config.allDecoders = make([]DecoderRunner, 0, 10)
	config.allSyncDecoders = make([]ReportingDecoder, 0, 10)
	config.allSplitters = make([]SplitterRunner, 0, 10)
	config.hostname = globals.Hostname
	config.pid = int32(os.Getpid())
	config.reportRecycleChan = make(chan *PipelinePack, 1)

	return config
}

// Callers should pass in the msgLoopCount value from any relevant Message
// objects they are holding. Returns a PipelinePack for injection into Heka
// pipeline, or nil if the msgLoopCount is above the configured maximum.
func (self *PipelineConfig) PipelinePack(msgLoopCount uint) (*PipelinePack, error) {
	if msgLoopCount++; msgLoopCount > self.Globals.MaxMsgLoops {
		return nil, fmt.Errorf("exceeded MaxMsgLoops = %d", self.Globals.MaxMsgLoops)
	}
	var pack *PipelinePack
	select {
	case pack = <-self.injectRecycleChan:
	case <-self.Globals.abortChan:
		return nil, AbortError
	}
	pack.Message.SetTimestamp(time.Now().UnixNano())
	pack.Message.SetUuid(uuid.NewRandom())
	pack.Message.SetHostname(self.hostname)
	pack.Message.SetPid(self.pid)
	pack.RefCount = 1
	pack.MsgLoopCount = msgLoopCount
	return pack, nil
}

// Returns the router.
func (self *PipelineConfig) Router() MessageRouter {
	return self.router
}

// Returns the inputRecycleChannel.
func (self *PipelineConfig) InputRecycleChan() chan *PipelinePack {
	return self.inputRecycleChan
}

// Returns the injectRecycleChannel.
func (self *PipelineConfig) InjectRecycleChan() chan *PipelinePack {
	return self.injectRecycleChan
}

// Returns the hostname.
func (self *PipelineConfig) Hostname() string {
	return self.hostname
}

// Returns OutputRunner registered under the specified name, or nil (and ok ==
// false) if no such name is registered.
func (self *PipelineConfig) Output(name string) (oRunner OutputRunner, ok bool) {
	oRunner, ok = self.OutputRunners[name]
	return
}

// Returns the underlying config object via the Helper interface.
func (self *PipelineConfig) PipelineConfig() *PipelineConfig {
	return self
}

// Instantiates and returns a Decoder of the specified name. Note that any
// time this method is used to fetch an unwrapped Decoder instance, it is up
// to the caller to check for and possibly satisfy the WantsDecoderRunner and
// WantsDecoderRunnerShutdown interfaces.
func (self *PipelineConfig) Decoder(name string) (decoder Decoder, ok bool) {
	var maker PluginMaker
	self.makersLock.RLock()
	defer self.makersLock.RUnlock()
	if maker, ok = self.DecoderMakers[name]; !ok {
		return
	}

	plugin, _, err := maker.Make()
	if err != nil {
		return nil, false
	}
	decoder = plugin.(Decoder)
	return
}

// Instantiates, starts, and returns a DecoderRunner wrapped around a newly
// created Decoder of the specified name.
func (self *PipelineConfig) DecoderRunner(baseName, fullName string) (
	dRunner DecoderRunner, ok bool) {

	self.makersLock.RLock()
	var maker PluginMaker
	if maker, ok = self.DecoderMakers[baseName]; !ok {
		self.makersLock.RUnlock()
		return
	}

	runner, err := maker.MakeRunner(fullName)
	self.makersLock.RUnlock()
	if err != nil {
		return nil, false
	}

	dRunner = runner.(DecoderRunner)
	self.allDecodersLock.Lock()
	self.allDecoders = append(self.allDecoders, dRunner)
	self.allDecodersLock.Unlock()
	self.decodersWg.Add(1)
	dRunner.Start(self, &self.decodersWg)
	return
}

// Stops and unregisters the provided DecoderRunner.
func (self *PipelineConfig) StopDecoderRunner(dRunner DecoderRunner) (ok bool) {
	self.allDecodersLock.Lock()
	defer self.allDecodersLock.Unlock()
	for i, r := range self.allDecoders {
		if r == dRunner {
			close(dRunner.InChan())
			self.allDecoders = append(self.allDecoders[:i], self.allDecoders[i+1:]...)
			ok = true
			break
		}
	}
	return
}

// Instantiates and returns an Encoder of the specified name.
func (self *PipelineConfig) Encoder(baseName, fullName string) (Encoder, bool) {
	self.makersLock.RLock()
	maker, ok := self.makers["Encoder"][baseName]
	if !ok {
		self.makersLock.RUnlock()
		return nil, false
	}

	plugin, _, err := maker.Make()
	self.makersLock.RUnlock()
	if err != nil {
		msg := fmt.Sprintf("Error creating encoder '%s': %s", fullName, err.Error())
		self.log(msg)
		return nil, false
	}
	encoder := plugin.(Encoder)
	if wantsName, ok := encoder.(WantsName); ok {
		wantsName.SetName(fullName)
	}
	self.allEncodersLock.Lock()
	self.allEncoders[fullName] = encoder
	self.allEncodersLock.Unlock()
	return encoder, true
}

// Returns a FilterRunner with the given name, or nil and ok == false if no
// such name is registered.
func (self *PipelineConfig) Filter(name string) (fRunner FilterRunner, ok bool) {
	self.filtersLock.RLock()
	defer self.filtersLock.RUnlock()
	fRunner, ok = self.FilterRunners[name]
	return
}

// Returns the specified StatAccumulator input plugin, or an error if it can't
// be found.
func (self *PipelineConfig) StatAccumulator(name string) (statAccum StatAccumulator,
	err error) {

	self.inputsLock.RLock()
	defer self.inputsLock.RUnlock()
	iRunner, ok := self.InputRunners[name]
	if !ok {
		err = fmt.Errorf("No Input named '%s", name)
		return
	}
	input := iRunner.Input()
	if statAccum, ok = input.(StatAccumulator); !ok {
		err = fmt.Errorf("Input '%s' is not a StatAccumulator", name)
	}
	return
}

// Starts the provided FilterRunner and adds it to the set of running Filters.
func (self *PipelineConfig) AddFilterRunner(fRunner FilterRunner) error {
	self.filtersLock.Lock()
	defer self.filtersLock.Unlock()
	self.FilterRunners[fRunner.Name()] = fRunner
	self.filtersWg.Add(1)
	if err := fRunner.Start(self, &self.filtersWg); err != nil {
		self.filtersWg.Done()
		return fmt.Errorf("AddFilterRunner '%s' failed to start: %s",
			fRunner.Name(), err)
	} else {
		self.router.AddFilterMatcher() <- fRunner.MatchRunner()
	}
	return nil
}

// Removes the specified FilterRunner from the configuration and the
// MessageRouter which signals the filter to shutdown by closing the input
// channel. Returns true if the filter was removed.
func (self *PipelineConfig) RemoveFilterRunner(name string) bool {
	if self.Globals.IsShuttingDown() {
		return false
	}

	self.filtersLock.Lock()
	defer self.filtersLock.Unlock()
	if fRunner, ok := self.FilterRunners[name]; ok {
		self.router.RemoveFilterMatcher() <- fRunner.MatchRunner()
		delete(self.FilterRunners, name)
		return true
	}
	return false
}

// AddInputRunner Starts the provided InputRunner and adds it to the set of
// running Inputs.
func (self *PipelineConfig) AddInputRunner(iRunner InputRunner) error {
	self.inputsLock.Lock()
	defer self.inputsLock.Unlock()
	self.InputRunners[iRunner.Name()] = iRunner
	self.inputsWg.Add(1)
	if err := iRunner.Start(self, &self.inputsWg); err != nil {
		self.inputsWg.Done()
		return fmt.Errorf("AddInputRunner '%s' failed to start: %s", iRunner.Name(), err)
	}
	return nil
}

// RemoveInputRunner unregisters the provided InputRunner, and stops it.
func (self *PipelineConfig) RemoveInputRunner(iRunner InputRunner) {
	name := iRunner.Name()
	self.makersLock.Lock()
	inputMakers := self.makers["Input"]
	if _, ok := inputMakers[name]; ok {
		delete(inputMakers, name)
	}
	self.makersLock.Unlock()

	self.inputsLock.Lock()
	delete(self.InputRunners, name)
	self.inputsLock.Unlock()

	iRunner.Input().Stop()
}

// RemoveOutputRunner unregisters the provided OutputRunner from heka, and
// removes it's message matcher from the heka router.
func (self *PipelineConfig) RemoveOutputRunner(oRunner OutputRunner) {
	name := oRunner.Name()
	self.makersLock.Lock()
	outputMakers := self.makers["Output"]
	if _, ok := outputMakers[name]; ok {
		self.router.RemoveOutputMatcher() <- oRunner.MatchRunner()
		delete(outputMakers, name)
	}
	self.makersLock.Unlock()

	self.outputsLock.Lock()
	delete(self.OutputRunners, name)
	self.outputsLock.Unlock()
}

type ConfigFile PluginConfig

var unknownOptionRegex = regexp.MustCompile("^Configuration contains key \\[(?P<key>\\S+)\\]")

// getAttr uses reflection to extract an attribute value from an arbitrary
// struct type that may or may not actually have the attribute, returning a
// provided default if the provided object is not a struct or if the attribute
// doesn't exist.
func getAttr(ob interface{}, attr string, default_ interface{}) (ret interface{}) {
	ret = default_
	obVal := reflect.ValueOf(ob)
	obVal = reflect.Indirect(obVal) // Dereference if it's a pointer.
	if obVal.Kind().String() != "struct" {
		// `FieldByName` will panic if we're not a struct.
		return ret
	}
	attrVal := obVal.FieldByName(attr)
	if !attrVal.IsValid() {
		return ret
	}
	return attrVal.Interface()
}

// getDefaultBool expects the name of a boolean setting and will extract and
// return the struct's default value for the setting, as a boolean pointer.
func getDefaultBool(ob interface{}, name string) (*bool, error) {
	defaultValue := getAttr(ob, name, false)
	switch defaultValue := defaultValue.(type) {
	case bool:
		return &defaultValue, nil
	case *bool:
		if defaultValue == nil {
			b := false
			defaultValue = &b
		}
		return defaultValue, nil
	}
	// If you hit this then a non-boolean config setting is conflicting
	// with one of Heka's defined boolean settings.
	return nil, fmt.Errorf("%s config setting must be boolean", name)
}

// Used internally to log and record plugin config loading errors.
func (self *PipelineConfig) log(msg string) {
	self.LogMsgs = append(self.LogMsgs, msg)
	LogError.Println(msg)
}

// PluginTypeRegex 插件类型 有5种，都是在名字或者type上可以看出来的
var PluginTypeRegex = regexp.MustCompile("(Decoder|Encoder|Filter|Input|Output|Splitter)$")

func getPluginCategory(pluginType string) string {
	pluginCats := PluginTypeRegex.FindStringSubmatch(pluginType)
	if len(pluginCats) < 2 {
		return ""
	}
	return pluginCats[1]
}

/*
// 0.8 版本的插件配置
type PluginGlobals struct {
	Typ        string `toml:"type"`
	Ticker     uint   `toml:"ticker_interval"`
	Matcher    string `toml:"message_matcher"` // Filter and Output only.
	Signer     string `toml:"message_signer"`  // Filter and Output only.
	Retries    RetryOptions
	Encoder    string // Output only.
	UseFraming *bool  `toml:"use_framing"` // Output only.
	CanExit    *bool  `toml:"can_exit"`
}
*/

// CommonConfig 插件通用配置 插件要起作用，需要 RegisterPlugin，注册方式 https://hekad.readthedocs.io/en/v0.10.0/developing/plugin.html
type CommonConfig struct {
	Typ string `toml:"type"` //插件类型，参见上面的 PluginTypeRegex 如果 type为空， 则 这个节的名字就是 type
}

// 通用输入插件
type CommonInputConfig struct {
	Ticker             uint `toml:"ticker_interval"`
	Decoder            string
	Splitter           string
	SyncDecode         *bool `toml:"synchronous_decode"`
	SendDecodeFailures *bool `toml:"send_decode_failures"`
	LogDecodeFailures  *bool `toml:"log_decode_failures"`
	CanExit            *bool `toml:"can_exit"`
	Retries            RetryOptions
}

type CommonFOConfig struct {
	Ticker       uint   `toml:"ticker_interval"`
	Matcher      string `toml:"message_matcher"`
	Signer       string `toml:"message_signer"`
	CanExit      *bool  `toml:"can_exit"`
	Retries      RetryOptions
	Encoder      string             // Output only.
	UseFraming   *bool              `toml:"use_framing"` // Output only.
	UseBuffering *bool              `toml:"use_buffering"`
	Buffering    *QueueBufferConfig `toml:"buffering"`
}

type CommonSplitterConfig struct {
	KeepTruncated   *bool `toml:"keep_truncated"`
	UseMsgBytes     *bool `toml:"use_message_bytes"`
	BufferSize      uint  `toml:"min_buffer_size"`
	IncompleteFinal *bool `toml:"deliver_incomplete_final"`
}

// Default configurations. 默认配置和加载的插件
func makeDefaultConfigs() map[string]bool {
	return map[string]bool{
		"ProtobufDecoder":         false,
		"ProtobufEncoder":         false,
		"TokenSplitter":           false,
		"PatternGroupingSplitter": false,
		"HekaFramingSplitter":     false,
		"NullSplitter":            false,
	}
}

func (self *PipelineConfig) RegisterDefault(name string) error {
	var config ConfigFile
	confStr := fmt.Sprintf("[%s]", name)
	toml.Decode(confStr, &config)
	LogInfo.Printf("Pre-loading: %s\n", confStr)
	maker, err := NewPluginMaker(name, self, config[name])
	if err != nil {
		// This really shouldn't happen.
		return err
	}
	LogInfo.Printf("Loading: [%s]\n", maker.Name())
	if _, err = maker.PrepConfig(); err != nil {
		return err
	}
	category := maker.Category()
	self.makersLock.Lock()
	self.makers[category][name] = maker
	self.makersLock.Unlock()
	// If we ever add a default input, filter, or output we'd need to call
	// maker.MakeRunner() here and store the runner on the PipelineConfig.
	return nil
}

// PreloadFromConfigFile loads all plugin configuration from a TOML
// configuration file, generates a PluginMaker for each loaded section, and
// stores the created PluginMakers in the makersByCategory map. The
// PipelineConfig should be already initialized via the Init function before
// this method is called. PreloadFromConfigFile is not reentrant, so it should
// only be called serially, not from multiple concurrent goroutines.
// 加载插件配置文件
func (self *PipelineConfig) PreloadFromConfigFile(filename string) error {
	var (
		configFile ConfigFile
		err        error
	)
	// 更新配置文件中，自定义变量（环境变量）
	contents, err := ReplaceEnvsFile(filename)
	if err != nil {
		return err
	}
	// TOML 解析成 configFile
	if _, err = toml.Decode(contents, &configFile); err != nil {
		return fmt.Errorf("Error decoding config file: %s", err)
	}

	if self.makersByCategory == nil {
		self.makersByCategory = make(map[string][]PluginMaker)
	}

	if self.defaultConfigs == nil {
		self.defaultConfigs = makeDefaultConfigs()
	}
	// 加载插件配置文件， 这里面做了插件注册的检查
	// Load all the plugin makers and file them by category.
	for name, conf := range configFile {
		if name == HEKA_DAEMON {
			continue
		}
		if _, ok := self.defaultConfigs[name]; ok {
			self.defaultConfigs[name] = true
		}
		LogInfo.Printf("Pre-loading: [%s]\n", name)

		maker, err := NewPluginMaker(name, self, conf) // todo 构造插件
		if err != nil {
			self.log(err.Error())
			self.errcnt++
			continue
		}
		// 获取插件的类型，不同类型特殊处理
		if maker.Type() == "MultiDecoder" {
			// Special case MultiDecoders so we can make sure they get
			// registered *after* all possible subdecoders.
			self.makersByCategory["MultiDecoder"] = append(
				self.makersByCategory["MultiDecoder"], maker)
		} else {
			category := maker.Category()
			self.makersByCategory[category] = append(
				self.makersByCategory[category], maker)
		}
	}
	return nil
}

// LoadConfig any not yet preloaded default plugins, then it finishes loading
// and initializing all of the plugin config that has been prepped from calls
// to PreloadFromConfigFile. This method should be called only once, after
// PreloadFromConfigFile has been called as many times as needed.
// 插件依赖和排序
func (self *PipelineConfig) LoadConfig() error {
	// Make sure our default plugins are registered.
	for name, registered := range self.defaultConfigs {
		if registered {
			continue
		}
		if err := self.RegisterDefault(name); err != nil {
			self.log(err.Error())
			self.errcnt++
		}
	}

	makersByCategory := self.makersByCategory
	if len(makersByCategory) == 0 {
		return errors.New("Empty configuration, exiting.")
	}

	var err error

	multiDecoders := make([]multiDecoderNode, len(makersByCategory["MultiDecoder"]))
	multiMakers := make(map[string]PluginMaker)
	for i, maker := range makersByCategory["MultiDecoder"] {
		multiMakers[maker.Name()] = maker
		tomlSection := maker.(*pluginMaker).tomlSection
		multiDecoders[i] = newMultiDecoderNode(maker.Name(), subsFromSection(tomlSection))
	}
	multiDecoders, err = orderDependencies(multiDecoders)
	if err != nil {
		return err
	}
	for i, d := range multiDecoders {
		makersByCategory["MultiDecoder"][i] = multiMakers[d.name]
	}

	// Append MultiDecoders to the end of the Decoders list.
	makersByCategory["Decoder"] = append(makersByCategory["Decoder"],
		makersByCategory["MultiDecoder"]...)

	// Force decoders and encoders to be loaded before the other plugin
	// types are initialized so we know they'll be there for inputs and
	// outputs to use during initialization.
	order := []string{"Decoder", "Encoder", "Splitter", "Input", "Filter", "Output"}
	for _, category := range order {
		for _, maker := range makersByCategory[category] {
			LogInfo.Printf("Loading: [%s]\n", maker.Name())
			if _, err = maker.PrepConfig(); err != nil {
				self.log(err.Error())
				self.errcnt++
			}
			self.makers[category][maker.Name()] = maker
			if category == "Encoder" {
				continue
			}
			runner, err := maker.MakeRunner("") // todo xx 这里才是运行插件 找对应的插件运行
			if err != nil {
				// Might be a duplicate error.
				seen := false
				for _, prevErr := range self.LogMsgs {
					if err.Error() == prevErr {
						seen = true
						break
					}
				}
				if !seen {
					msg := fmt.Sprintf("Error making runner for %s: %s", maker.Name(),
						err.Error())
					self.log(msg)
					self.errcnt++
				}
				continue
			}
			switch category {
			case "Input":
				self.InputRunners[maker.Name()] = runner.(InputRunner)
			case "Filter":
				self.FilterRunners[maker.Name()] = runner.(FilterRunner)
			case "Output":
				self.OutputRunners[maker.Name()] = runner.(OutputRunner)
			}
		}
	}

	if self.errcnt != 0 {
		return fmt.Errorf("%d errors loading plugins", self.errcnt)
	}

	return nil
}

func subsFromSection(section toml.Primitive) []string {
	var secMap = make(map[string]interface{})
	toml.PrimitiveDecode(section, &secMap)
	var subs []string
	if _, ok := secMap["subs"]; ok {
		subsUntyped, _ := secMap["subs"].([]interface{})
		subs = make([]string, len(subsUntyped))
		for i, subUntyped := range subsUntyped {
			subs[i], _ = subUntyped.(string)
		}
	}
	return subs
}

func ReplaceEnvsFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	r, err := EnvSub(file)
	if err != nil {
		return "", err
	}
	contents, err := ioutil.ReadAll(r)
	if err != nil {
		return "", err
	}
	return string(contents), nil
}

func EnvSub(r io.Reader) (io.Reader, error) {
	bufIn := bufio.NewReader(r)
	bufOut := new(bytes.Buffer)
	for {
		chunk, err := bufIn.ReadBytes(byte('%'))
		if err != nil {
			if err == io.EOF {
				// We're done.
				bufOut.Write(chunk)
				break
			}
			return nil, err
		}
		bufOut.Write(chunk[:len(chunk)-1])

		tmp := make([]byte, 4)
		tmp, err = bufIn.Peek(4)
		if err != nil {
			if err == io.EOF {
				// End of file, write the last few bytes out and exit.
				bufOut.WriteRune('%')
				bufOut.Write(tmp)
				break
			}
			return nil, err
		}

		if string(tmp) == "ENV[" {
			// Found opening delimiter, advance the read cursor and look for
			// closing delimiter.
			tmp, err = bufIn.ReadBytes(byte('['))
			if err != nil {
				// This shouldn't happen, since the Peek succeeded.
				return nil, err
			}
			chunk, err = bufIn.ReadBytes(byte(']'))
			if err != nil {
				if err == io.EOF {
					// No closing delimiter, return an error
					return nil, ErrMissingCloseDelim
				}
				return nil, err
			}
			// `chunk` is now holding var name + closing delimiter.
			// var name contains invalid characters, return an error
			if bytes.IndexAny(chunk, invalidEnvChars) != -1 ||
				bytes.Index(chunk, invalidEnvPrefix) != -1 {
				return nil, ErrInvalidChars
			}
			varName := string(chunk[:len(chunk)-1])
			varVal := os.Getenv(varName)
			bufOut.WriteString(varVal)
		} else {
			// Just a random '%', not an opening delimiter, write it out and
			// keep going.
			bufOut.WriteRune('%')
		}
	}
	return bufOut, nil
}
